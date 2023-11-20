/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.avro.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.internal.util.collection.ReadOptimizedLruCache;
import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public final class AvroSerializerHooks {
    private AvroSerializerHooks() { }

    public static <T> byte[] serialize(DatumWriter<T> datumWriter, T data) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            datumWriter.write(data, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    public static <T> T deserialize(DatumReader<T> datumReader, byte[] data) {
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    public static class GenericContainerHook implements SerializerHook<GenericContainer> {
        private static final int CACHE_SIZE = 500;
        private static final int CACHE_THRESHOLD = CACHE_SIZE + Math.min(CACHE_SIZE / 10, 50);

        @Override
        public Class<GenericContainer> getSerializationType() {
            return GenericContainer.class;
        }

        @Override
        public Serializer createSerializer(SerializationService serializationService) {
            ((AbstractSerializationService) serializationService)
                    .registerFromSuperType(SpecificRecordBase.class, Externalizable.class);

            return new StreamSerializer<GenericContainer>() {
                private final ReadOptimizedLruCache<String, Schema> jsonToSchema =
                        new ReadOptimizedLruCache<>(CACHE_SIZE, CACHE_THRESHOLD);
                private final ReadOptimizedLruCache<Schema, String> schemaToJson =
                        new ReadOptimizedLruCache<>(CACHE_SIZE, CACHE_THRESHOLD);

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.AVRO_GENERIC_CONTAINER;
                }

                @Override
                public void write(@Nonnull ObjectDataOutput out, @Nonnull GenericContainer datum) throws IOException {
                    // SchemaNormalization.toParsingForm() drops details that are irrelevant to reader,
                    // such as field default values. However, GenericData.Record.equals() compares record
                    // schemas without ignoring the dropped details, which is effectively the same as
                    // schema1.toString().equals(schema2.toString()).
                    String schemaJson = schemaToJson.computeIfAbsent(datum.getSchema(), Schema::toString);
                    out.writeString(schemaJson);
                    out.writeByteArray(datum instanceof LazyImmutableContainer
                            ? ((LazyImmutableContainer<?>) datum).serialized
                            : serialize(new GenericDatumWriter<>(datum.getSchema()), datum));
                }

                @Nonnull @Override
                public GenericContainer read(@Nonnull ObjectDataInput in) throws IOException {
                    String schemaJson = in.readString();
                    byte[] serializedDatum = in.readByteArray();
                        Schema schema = jsonToSchema.computeIfAbsent(schemaJson,
                                json -> new Schema.Parser().parse(json));
                    switch (schema.getType()) {
                        case RECORD:
                            return new LazyImmutableRecord(serializedDatum, schema);
                        case ARRAY:
                            return new LazyImmutableArray<>(serializedDatum, schema);
                        case ENUM:  // GenericEnumSymbol
                        case FIXED: // GenericFixed
                            return deserialize(new GenericDatumReader<>(schema), serializedDatum);
                        default:
                            throw new UnsupportedOperationException(
                                    "Schema type " + schema.getType() + " is unsupported");
                    }
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }

    public static final class Utf8Hook implements SerializerHook<Utf8> {
        @Override
        public Class<Utf8> getSerializationType() {
            return Utf8.class;
        }

        @Override
        public Serializer createSerializer() {
            return new ByteArraySerializer<Utf8>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.AVRO_UTF8;
                }

                @Override
                public byte[] write(Utf8 string) {
                    return string.getBytes();
                }

                @Override
                public Utf8 read(byte[] buffer) {
                    return new Utf8(buffer);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }

    @ThreadSafe
    private abstract static class LazyImmutableContainer<T extends GenericContainer> implements GenericContainer {
        private final byte[] serialized;
        private final Schema schema;
        private volatile T deserialized;

        protected LazyImmutableContainer(byte[] serialized, Schema schema) {
            this.serialized = serialized;
            this.schema = schema;
        }

        protected T deserialized() {
            if (deserialized == null) {
                deserialized = deserialize(new GenericDatumReader<>(schema), serialized);
            }
            return deserialized;
        }

        @Override
        public Schema getSchema() {
            return schema;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof GenericContainer
                    && ((GenericContainer) obj).getSchema().getType() == schema.getType())) {
                return false;
            }
            return deserialized().equals(obj);
        }

        @Override
        public int hashCode() {
            return deserialized().hashCode();
        }

        @Override
        public String toString() {
            return deserialized != null ? deserialized.toString() : super.toString();
        }
    }

    //region Lazy Immutable Containers
    private static final class LazyImmutableRecord extends LazyImmutableContainer<GenericRecord>
            implements GenericRecord {

        private LazyImmutableRecord(byte[] serializedRecord, Schema schema) {
            super(serializedRecord, schema);
        }

        @Override
        public Object get(int i) {
            return deserialized().get(i);
        }

        @Override
        public Object get(String key) {
            return deserialized().get(key);
        }

        @Override
        public void put(String key, Object v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void put(int i, Object v) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class LazyImmutableArray<T> extends LazyImmutableContainer<GenericArray<T>>
            implements GenericArray<T> {
        private volatile List<T> immutable;

        private LazyImmutableArray(byte[] serializedArray, Schema schema) {
            super(serializedArray, schema);
        }

        private List<T> immutable() {
            if (immutable == null) {
                immutable = Collections.unmodifiableList(deserialized());
            }
            return immutable;
        }

        @Override
        public T peek() {
            return deserialized().peek();
        }

        @Override
        public int size() {
            return deserialized().size();
        }

        @Override
        public boolean isEmpty() {
            return deserialized().isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return deserialized().contains(o);
        }

        @Override
        @SuppressWarnings("SlowListContainsAll")
        public boolean containsAll(@Nonnull Collection<?> c) {
            return deserialized().containsAll(c);
        }

        @Override
        public T get(int index) {
            return deserialized().get(index);
        }

        @Override
        public int indexOf(Object o) {
            return deserialized().indexOf(o);
        }

        @Override
        public int lastIndexOf(Object o) {
            return deserialized().lastIndexOf(o);
        }

        @Nonnull @Override
        public Object[] toArray() {
            return deserialized().toArray();
        }

        @Nonnull @Override
        public <T1> T1[] toArray(@Nonnull T1[] a) {
            return deserialized().toArray(a);
        }

        @Nonnull @Override
        public Iterator<T> iterator() {
            return immutable().iterator();
        }

        @Nonnull @Override
        public ListIterator<T> listIterator() {
            return immutable().listIterator();
        }

        @Nonnull @Override
        public ListIterator<T> listIterator(int index) {
            return immutable().listIterator(index);
        }

        @Nonnull @Override
        public List<T> subList(int fromIndex, int toIndex) {
            return immutable().subList(fromIndex, toIndex);
        }

        @Override
        public boolean add(T t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reverse() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(@Nonnull Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(int index, @Nonnull Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(@Nonnull Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(@Nonnull Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public T set(int index, T element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add(int index, T element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public T remove(int index) {
            throw new UnsupportedOperationException();
        }
    }
    //endregion
}
