/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl.compact.record;

import com.hazelcast.internal.serialization.impl.compact.CompactStreamSerializer;
import com.hazelcast.internal.serialization.impl.compact.CompactUtil;
import com.hazelcast.internal.serialization.impl.compact.DefaultCompactReader;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.zeroconfig.ValueReaderWriter;
import com.hazelcast.internal.serialization.impl.compact.zeroconfig.ValueReaderWriters;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("checkstyle:executablestatementcount")
public class JavaRecordSerializer implements CompactSerializer<Object> {

    private final CompactStreamSerializer compactStreamSerializer;
    private final Map<Class<?>, JavaRecordReader> readersCache = new ConcurrentHashMap<>();
    private final Map<Class<?>, ComponentReaderWriter[]> readerWritersCache = new ConcurrentHashMap<>();

    public JavaRecordSerializer(CompactStreamSerializer compactStreamSerializer) {
        this.compactStreamSerializer = compactStreamSerializer;
    }

    public boolean isRecord(Class<?> clazz) {
        return clazz.isRecord();
    }

    @Nonnull
    @Override
    public Object read(@Nonnull CompactReader reader) {
        DefaultCompactReader compactReader = (DefaultCompactReader) reader;
        Class<?> associatedClass = requireNonNull(compactReader.getAssociatedClass(),
                "AssociatedClass is required for JavaRecordSerializer");

        JavaRecordReader recordReader = readersCache.get(associatedClass);
        if (recordReader == null) {
            populateReadersWriters(associatedClass);
            recordReader = readersCache.get(associatedClass);
        }

        return recordReader.readRecord(compactReader, compactReader.getSchema());
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull Object object) {
        Class<?> clazz = object.getClass();
        ComponentReaderWriter[] readerWriters = readerWritersCache.get(clazz);
        if (readerWriters == null) {
            populateReadersWriters(clazz);
            readerWriters = readerWritersCache.get(clazz);
        }

        try {
            for (ComponentReaderWriter readerWriter : readerWriters) {
                readerWriter.writeComponent(writer, object);
            }
        } catch (Exception e) {
            throw new HazelcastSerializationException("Failed to write the Java record", e);
        }
    }

    @Nonnull
    @Override
    public String getTypeName() {
        throw new IllegalStateException("getTypeName should not be called for the record serializer");
    }

    @Nonnull
    @Override
    public Class<Object> getCompactClass() {
        throw new IllegalStateException("getCompactClass should not be called for the record serializer");
    }

    private void populateReadersWriters(Class<?> clazz) {
        // The top level class might not be Compact serializable
        CompactUtil.verifyClassIsCompactSerializable(clazz);

        try {
            RecordComponent[] recordComponents = clazz.getRecordComponents();
            Class<?>[] componentTypes = new Class<?>[recordComponents.length];

            ComponentReaderWriter[] componentReaderWriters = new ComponentReaderWriter[recordComponents.length];

            for (int i = 0; i < recordComponents.length; i++) {
                RecordComponent recordComponent = recordComponents[i];
                Class<?> type = recordComponent.getType();
                Type genericType = recordComponent.getGenericType();
                String name = recordComponent.getName();

                componentTypes[i] = type;
                Method componentGetter = clazz.getDeclaredMethod(name);
                componentGetter.setAccessible(true);
                componentReaderWriters[i] = new ComponentReaderWriterAdapter<>(
                        ValueReaderWriters.readerWriterFor(compactStreamSerializer, clazz, type, genericType, name),
                        componentGetter
                );
            }

            Constructor<?> constructor = clazz.getDeclaredConstructor(componentTypes);
            constructor.setAccessible(true);

            JavaRecordReader recordReader = new JavaRecordReader(constructor, componentReaderWriters);
            readersCache.put(clazz, recordReader);
            readerWritersCache.put(clazz, componentReaderWriters);
        } catch (Exception e) {
            throw new HazelcastSerializationException("Failed to construct the readers/writers for the Java record", e);
        }
    }

    private record ComponentReaderWriterAdapter<T>(
            ValueReaderWriter<T> readerWriter,
            Method componentGetter) implements ComponentReaderWriter {

        @Override
        public Object readComponent(CompactReader compactReader, Schema schema) {
            return readerWriter.read(compactReader, schema);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void writeComponent(CompactWriter compactWriter, Object recordObject) throws Exception {
            readerWriter.write(compactWriter, (T) componentGetter.invoke(recordObject));
        }
    }
}
