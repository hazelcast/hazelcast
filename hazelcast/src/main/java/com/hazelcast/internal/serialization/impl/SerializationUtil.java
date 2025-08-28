/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.VersionedPortable;
import com.hazelcast.partition.PartitioningStrategy;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public final class SerializationUtil {

    static final PartitioningStrategy<?> EMPTY_PARTITIONING_STRATEGY = new EmptyPartitioningStrategy();

    private SerializationUtil() {
    }

    /**
     * Checks that the {@code object} implements {@link Serializable} and is
     * correctly serializable by actually trying to serialize it. This will
     * reveal some non-serializable field early.
     *
     * @param object     object to check
     * @param objectName object description for the exception
     * @throws IllegalArgumentException if {@code object} is not serializable
     */
    public static void checkSerializable(Object object, String objectName) {
        if (object == null) {
            return;
        }
        if (!(object instanceof Serializable)) {
            throw new IllegalArgumentException('"' + objectName + "\" must implement Serializable");
        }
        try (ObjectOutputStream os = new ObjectOutputStream(OutputStream.nullOutputStream())) {
            os.writeObject(object);
        } catch (NotSerializableException | InvalidClassException e) {
            throw new IllegalArgumentException("\"" + objectName + "\" must be serializable", e);
        } catch (IOException e) {
            // never really thrown, as the underlying stream never throws it
            throw new HazelcastException(e);
        }
    }

    public static boolean isNullData(Data data) {
        // in some edge cases (generally malformed) CONSTANT_TYPE_NULL can be used with non-zero length.
        // it should be treated as null, to avoid multiple representations of null
        return data.getType() == SerializationConstants.CONSTANT_TYPE_NULL;
    }

    static RuntimeException handleException(Throwable e) {
        if (e instanceof OutOfMemoryError error) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(error);
            throw error;
        }
        if (e instanceof Error error) {
            throw error;
        }
        if (e instanceof HazelcastSerializationException exception) {
            return exception;
        }
        if (e instanceof HazelcastInstanceNotActiveException exception) {
            return exception;
        }
        if (e instanceof HazelcastClientNotActiveException exception) {
            return exception;
        }
        return new HazelcastSerializationException(e);
    }

    static RuntimeException handleSerializeException(Object rootObject, Throwable e) {
        if (e instanceof OutOfMemoryError error) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(error);
            throw error;
        }
        if (e instanceof Error error) {
            throw error;
        }
        if (e instanceof HazelcastInstanceNotActiveException exception) {
            return exception;
        }
        if (e instanceof HazelcastClientNotActiveException exception) {
            return exception;
        }
        String clazz = rootObject == null ? "null" : rootObject.getClass().getName();
        return new HazelcastSerializationException("Failed to serialize '" + clazz + '\'', e);
    }

    public static SerializerAdapter createSerializerAdapter(Serializer serializer) {
        final SerializerAdapter s;
        if (serializer instanceof StreamSerializer streamSerializer) {
            s = new StreamSerializerAdapter(streamSerializer);
        } else if (serializer instanceof ByteArraySerializer arraySerializer) {
            s = new ByteArraySerializerAdapter(arraySerializer);
        } else {
            throw new IllegalArgumentException("Serializer " + serializer.getClass().getName()
                    + " must be an instance of either StreamSerializer or ByteArraySerializer");
        }
        return s;
    }

    static void getInterfaces(Class<?> clazz, Set<Class<?>> interfaces) {
        final var classes = clazz.getInterfaces();
        if (classes.length > 0) {
            Collections.addAll(interfaces, classes);
            for (Class<?> cl : classes) {
                getInterfaces(cl, interfaces);
            }
        }
    }

    static int indexForDefaultType(final int typeId) {
        return -typeId;
    }

    @SuppressWarnings("removal")
    public static int getPortableVersion(Portable portable, int defaultVersion) {
        int version = defaultVersion;
        if (portable instanceof VersionedPortable versionedPortable) {
            version = versionedPortable.getClassVersion();
            if (version < 0) {
                throw new IllegalArgumentException("Version cannot be negative!");
            }
        }
        return version;
    }

    public static ObjectDataOutputStream createObjectDataOutputStream(OutputStream out, InternalSerializationService ss) {
        return new ObjectDataOutputStream(out, ss);
    }

    public static ObjectDataInputStream createObjectDataInputStream(InputStream in, InternalSerializationService ss) {
        return new ObjectDataInputStream(in, ss);
    }

    public static InputStream convertToInputStream(DataInput in, int offset) {
        if (!(in instanceof ByteArrayObjectDataInput byteArrayInput)) {
            throw new HazelcastSerializationException("Cannot convert " + in.getClass().getName() + " to input stream");
        }
        return new ByteArrayInputStream(byteArrayInput.data, offset, byteArrayInput.size - offset);
    }

    @SerializableByConvention
    @SuppressWarnings("rawtypes")
    private static class EmptyPartitioningStrategy implements PartitioningStrategy {
        public Object getPartitionKey(Object key) {
            return null;
        }
    }

    /**
     * Writes a map to given {@code ObjectDataOutput}.
     *
     * @param map the map to serialize, can be {@code null}
     * @param out the output to write the map to
     */
    public static <K, V> void writeNullableMap(Map<K, V> map, ObjectDataOutput out) throws IOException {
        // write true when the map is NOT null
        out.writeBoolean(map != null);
        if (map == null) {
            return;
        }

        writeMap(map, out);
    }

    public static <K, V> void writeMap(@Nonnull Map<K, V> map, ObjectDataOutput out) throws IOException {
        int size = map.size();
        out.writeInt(size);

        int k = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
            k++;
        }
        assert size == k : "Map has been updated during serialization! Initial size: " + size + ", written size: " + k;
    }

    public static <V> void writeMapStringKey(@Nonnull Map<String, V> map, ObjectDataOutput out) throws IOException {
        int size = map.size();
        out.writeInt(size);

        int k = 0;
        for (Map.Entry<String, V> entry : map.entrySet()) {
            out.writeString(entry.getKey());
            out.writeObject(entry.getValue());
            k++;
        }
        assert size == k : "Map has been updated during serialization! Initial size: " + size + ", written size: " + k;
    }

    public static <V> void writeMapStringKey(@Nonnull Map<String, V> map, ObjectDataOutput out,
                                             BiConsumerEx<ObjectDataOutput, V> valueWriter) throws IOException {
        int size = map.size();
        out.writeInt(size);

        int k = 0;
        for (Map.Entry<String, V> entry : map.entrySet()) {
            out.writeString(entry.getKey());
            valueWriter.accept(out, entry.getValue());
            k++;
        }
        assert size == k : "Map has been updated during serialization! Initial size: " + size + ", written size: " + k;
    }

    /**
     * Reads a map written by {@link #writeNullableMap(Map, ObjectDataOutput)}. The map itself
     * may be {@code null}. No guarantee is provided about the type of Map returned or its suitability
     * to be used in a thread-safe manner.
     *
     * @param in  the {@code ObjectDataInput} input to read from
     * @param <K> type of key class
     * @param <V> type of value class
     * @return a {@code Map} containing the keys &amp; values read from the input or {@code null}
     * if the original serialized map was {@code null}
     * @throws IOException when an error occurs while reading from the input
     */
    public static <K, V> Map<K, V> readNullableMap(ObjectDataInput in) throws IOException {
        boolean isNull = !in.readBoolean();
        if (isNull) {
            return null;
        }
        return readMap(in);
    }

    @Nonnull
    public static <K, V> Map<K, V> readMap(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        Map<K, V> map = createHashMap(size);
        for (int i = 0; i < size; i++) {
            K key = in.readObject();
            V value = in.readObject();
            map.put(key, value);
        }
        return map;
    }

    @Nonnull
    public static <V> Map<String, V> readMapStringKey(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        Map<String, V> map = createHashMap(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            V value = in.readObject();
            map.put(key, value);
        }
        return map;
    }

    @Nonnull
    public static <V> Map<String, V> readMapStringKey(ObjectDataInput in,
                                                      FunctionEx<ObjectDataInput, V> valueReader) throws IOException {
        int size = in.readInt();
        Map<String, V> map = createHashMap(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            V value = valueReader.apply(in);
            map.put(key, value);
        }
        return map;
    }

    /**
     * Writes a collection to an {@link ObjectDataOutput}. The collection's size is written
     * to the data output, then each object in the collection is serialized.
     * The collection is allowed to be null.
     *
     * @param items collection of items to be serialized
     * @param out   data output to write to
     * @param <T>   type of items
     * @throws IOException when an error occurs while writing to the output
     */
    public static <T> void writeNullableCollection(Collection<T> items, ObjectDataOutput out) throws IOException {
        // write true when the collection is NOT null
        out.writeBoolean(items != null);
        if (items == null) {
            return;
        }
        writeCollection(items, out);
    }

    /**
     * Writes a list to an {@link ObjectDataOutput}. The list's size is written
     * to the data output, then each object in the list is serialized.
     * The list is allowed to be null.
     *
     * @param items list of items to be serialized
     * @param out   data output to write to
     * @param <T>   type of items
     * @throws IOException when an error occurs while writing to the output
     */
    public static <T> void writeNullableList(List<T> items, ObjectDataOutput out) throws IOException {
        writeNullableCollection(items, out);
    }

    /**
     * Writes a collection to an {@link ObjectDataOutput}. The collection's size is written
     * to the data output, then each object in the collection is serialized.
     *
     * @param items collection of items to be serialized
     * @param out   data output to write to
     * @param <T>   type of items
     * @throws NullPointerException if {@code items} or {@code out} is {@code null}
     * @throws IOException          when an error occurs while writing to the output
     */
    public static <T> void writeCollection(Collection<T> items, ObjectDataOutput out) throws IOException {
        int size = items.size();
        out.writeInt(size);

        int k = 0;
        for (T item : items) {
            out.writeObject(item);
            k++;
        }
        assert size == k : "Collection has been updated during serialization! Initial size: " + size + ", written size: " + k;
    }

    /**
     * Writes a list to an {@link ObjectDataOutput}. The list's size is written
     * to the data output, then each object in the list is serialized.
     *
     * @param items list of items to be serialized
     * @param out   data output to write to
     * @param <T>   type of items
     * @throws NullPointerException if {@code items} or {@code out} is {@code null}
     * @throws IOException          when an error occurs while writing to the output
     */
    public static <T> void writeList(List<T> items, ObjectDataOutput out) throws IOException {
        writeCollection(items, out);
    }

    /**
     * Writes a nullable {@link PartitionIdSet} to the given data output.
     */
    public static void writeNullablePartitionIdSet(PartitionIdSet partitionIds, ObjectDataOutput out) throws IOException {
        if (partitionIds == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(partitionIds.getPartitionCount());
        out.writeInt(partitionIds.size());
        PrimitiveIterator.OfInt intIterator = partitionIds.intIterator();
        while (intIterator.hasNext()) {
            out.writeInt(intIterator.nextInt());
        }
    }

    /**
     * Reads a collection from the given {@link ObjectDataInput}. It is expected that
     * the next int read from the data input is the collection's size, then that
     * many objects are read from the data input and returned as a collection.
     *
     * @param in  data input to read from
     * @param <T> type of items
     * @return collection of items read from data input or null
     * @throws IOException when an error occurs while reading from the input
     */
    public static <T> Collection<T> readNullableCollection(ObjectDataInput in) throws IOException {
        return readNullableList(in);
    }

    /**
     * Reads a list from the given {@link ObjectDataInput}. It is expected that
     * the next int read from the data input is the list's size, then that
     * many objects are read from the data input and returned as a list.
     *
     * @param in  data input to read from
     * @param <T> type of items
     * @return list of items read from data input or null
     * @throws IOException when an error occurs while reading from the input
     */
    public static <T> List<T> readNullableList(ObjectDataInput in) throws IOException {
        boolean isNull = !in.readBoolean();
        if (isNull) {
            return null;
        }
        return readList(in);
    }

    /**
     * Reads a collection from the given {@link ObjectDataInput}. It is expected that
     * the next int read from the data input is the collection's size, then that
     * many objects are read from the data input and returned as a collection.
     *
     * @param in  data input to read from
     * @param <T> type of items
     * @return collection of items read from data input
     * @throws IOException when an error occurs while reading from the input
     */
    public static <T> Collection<T> readCollection(ObjectDataInput in) throws IOException {
        return readList(in);
    }

    /**
     * Reads a list from the given {@link ObjectDataInput}. It is expected that
     * the next int read from the data input is the list's size, then that
     * many objects are read from the data input and returned as a list.
     *
     * @param in  data input to read from
     * @param <T> type of items
     * @return list of items read from data input
     * @throws IOException when an error occurs while reading from the input
     */
    public static <T> List<T> readList(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        if (size == 0) {
            return Collections.emptyList();
        }
        List<T> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            T item = in.readObject();
            list.add(item);
        }
        return list;
    }

    public static PartitionIdSet readNullablePartitionIdSet(ObjectDataInput in) throws IOException {
        int partitionCount = in.readInt();
        if (partitionCount == -1) {
            return null;
        }
        PartitionIdSet result = new PartitionIdSet(partitionCount);
        int setSize = in.readInt();
        for (int i = 0; i < setSize; i++) {
            result.add(in.readInt());
        }
        return result;
    }

    public static boolean isClassStaticAndSerializable(Object object) {
        Class<?> clazz = object.getClass();
        boolean isStatic = !clazz.isSynthetic() && !clazz.isAnonymousClass() && !clazz.isLocalClass();
        if (!isStatic) {
            return false;
        }

        try {
            checkSerializable(object, "object");
        } catch (Throwable t) {
            return false;
        }

        return true;
    }

    public static void writeNullableBoolean(ObjectDataOutput out, Boolean b) throws IOException {
        if (b == null) {
            out.writeByte(Byte.MAX_VALUE);
        } else {
            out.writeByte(b ? 1 : 0);
        }
    }

    public static Boolean readNullableBoolean(ObjectDataInput in) throws IOException {
        byte b = in.readByte();
        return switch (b) {
            case Byte.MAX_VALUE -> null;
            case 0 -> Boolean.FALSE;
            case 1 -> Boolean.TRUE;
            default -> throw new IllegalStateException("Unexpected value " + b + " while reading nullable boolean.");
        };
    }
}
