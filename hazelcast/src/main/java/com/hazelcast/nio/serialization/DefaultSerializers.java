/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.impl.GroupProperties;
import com.hazelcast.nio.FastDataInputStream;
import com.hazelcast.nio.FastDataOutputStream;
import com.hazelcast.nio.HazelcastSerializationException;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.hazelcast.nio.ClassLoaderUtil.loadClass;
import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.nio.IOUtil.*;
import static com.hazelcast.nio.serialization.SerializationConstants.*;

/**
 * @mdogan 6/18/12
 */
public final class DefaultSerializers {

    private static final List<SingletonTypeSerializer> DEFAULT_SERIALIZERS;

    static {
        // order is important !
        final List<SingletonTypeSerializer> coll = new LinkedList<SingletonTypeSerializer>();
        coll.add(new StringSerializer());
        coll.add(new LongSerializer());
        coll.add(new IntegerSerializer());
        coll.add(new ByteArraySerializer());
        coll.add(new BooleanSerializer());
        coll.add(new Externalizer());
        coll.add(new DateSerializer());
        coll.add(new BigIntegerSerializer());
        coll.add(new ClassSerializer());
        coll.add(new ObjectSerializer());
        DEFAULT_SERIALIZERS = Collections.unmodifiableList(coll);
    }

    public static TypeSerializer serializerFor(Class type) {
        for (SingletonTypeSerializer serializer : DEFAULT_SERIALIZERS) {
            if (serializer.isSuitable(type)) {
                return serializer;
            }
        }
        return null;
    }

    public static TypeSerializer serializerFor(int typeId) {
        for (TypeSerializer serializer : DEFAULT_SERIALIZERS) {
            if (serializer.getTypeId() == typeId) {
                return serializer;
            }
        }
        return null;
    }

    public static final class IntegerSerializer extends SingletonTypeSerializer<Integer> {

        public final boolean isSuitable(final Class clazz) {
            return clazz == Integer.class || clazz == int.class;
        }

        public final int getTypeId() {
            return SERIALIZER_TYPE_INTEGER;
        }

        public final Integer read(final FastDataInputStream dataIn) throws Exception {
            return dataIn.readInt();
        }

        public final void write(final FastDataOutputStream dataOut, final Integer obj) throws Exception {
            dataOut.writeInt(obj.intValue());
        }
    }

    public static final class LongSerializer extends SingletonTypeSerializer<Long> {

        public final boolean isSuitable(final Class clazz) {
            return clazz == Long.class || clazz == long.class;
        }

        public final int getTypeId() {
            return SERIALIZER_TYPE_LONG;
        }

        public final Long read(final FastDataInputStream dataIn) throws Exception {
            return dataIn.readLong();
        }

        public final void write(final FastDataOutputStream dataOut, final Long obj) throws Exception {
            dataOut.writeLong(obj.longValue());
        }
    }

    public static final class StringSerializer extends SingletonTypeSerializer<String> {

        public final boolean isSuitable(final Class clazz) {
            return clazz == String.class;
        }

        public final int getTypeId() {
            return SERIALIZER_TYPE_STRING;
        }

        public final String read(final FastDataInputStream dataIn) throws Exception {
            return dataIn.readUTF();
        }

        public final void write(final FastDataOutputStream dataOut, final String obj) throws Exception {
            dataOut.writeUTF(obj);
        }
    }

    public static final class ByteArraySerializer extends SingletonTypeSerializer<byte[]> {

        public final boolean isSuitable(final Class clazz) {
            return clazz == byte[].class;
        }

        public final int getTypeId() {
            return SERIALIZER_TYPE_BYTE_ARRAY;
        }

        public final byte[] read(final FastDataInputStream dataIn) throws Exception {
            return readByteArray(dataIn);
        }

        public final void write(final FastDataOutputStream dataOut, final byte[] obj) throws Exception {
            writeByteArray(dataOut, obj);
        }
    }

    public static final class BooleanSerializer extends SingletonTypeSerializer<Boolean> {

        public boolean isSuitable(final Class clazz) {
            return clazz == Boolean.class || clazz == boolean.class;
        }

        public int getTypeId() {
            return SERIALIZER_TYPE_BOOLEAN;
        }

        public void write(FastDataOutputStream dataOut, Boolean obj) throws Exception {
            dataOut.write((obj ? 1 : 0));
        }

        public Boolean read(FastDataInputStream dataIn) throws Exception {
            return dataIn.readByte() != 0;
        }
    }

    public static final class BigIntegerSerializer extends SingletonTypeSerializer<BigInteger> {

        public final boolean isSuitable(final Class clazz) {
            return BigInteger.class.isAssignableFrom(clazz);
        }

        public final int getTypeId() {
            return SERIALIZER_TYPE_BIG_INTEGER;
        }

        public final BigInteger read(final FastDataInputStream dataIn) throws Exception {
            final byte[] bytes = new byte[dataIn.readInt()];
            dataIn.readFully(bytes);
            return new BigInteger(bytes);
        }

        public final void write(final FastDataOutputStream dataOut, final BigInteger obj) throws Exception {
            final byte[] bytes = obj.toByteArray();
            dataOut.writeInt(bytes.length);
            dataOut.write(bytes);
        }
    }

    public static final class DateSerializer extends SingletonTypeSerializer<Date> {

        public final boolean isSuitable(final Class clazz) {
            return Date.class.isAssignableFrom(clazz);
        }

        public final int getTypeId() {
            return SERIALIZER_TYPE_DATE;
        }

        public final Date read(final FastDataInputStream dataIn) throws Exception {
            return new Date(dataIn.readLong());
        }

        public final void write(final FastDataOutputStream dataOut, final Date obj) throws Exception {
            dataOut.writeLong(obj.getTime());
        }
    }

    public static final class ClassSerializer extends SingletonTypeSerializer<Class> {

        public final boolean isSuitable(final Class clazz) {
            return clazz == Class.class;
        }

        public final int getTypeId() {
            return SERIALIZER_TYPE_CLASS;
        }

        public final Class read(final FastDataInputStream dataIn) throws Exception {
            return loadClass(dataIn.readUTF());
        }

        public final void write(final FastDataOutputStream dataOut, final Class obj) throws Exception {
            dataOut.writeUTF(obj.getName());
        }
    }

    public static final class Externalizer extends SingletonTypeSerializer<Externalizable> {

        public final boolean isSuitable(final Class clazz) {
            return Externalizable.class.isAssignableFrom(clazz);
        }

        public final int getTypeId() {
            return SERIALIZER_TYPE_EXTERNALIZABLE;
        }

        public final Externalizable read(final FastDataInputStream dataIn) throws Exception {
            final String className = dataIn.readUTF();
            try {
                final Externalizable ds = (Externalizable) newInstance(loadClass(className));
                ds.readExternal(newObjectInputStream(dataIn));
                return ds;
            } catch (final Exception e) {
                throw new HazelcastSerializationException("Problem while reading Externalizable class : "
                                                          + className + ", exception: " + e);
            }
        }

        public final void write(final FastDataOutputStream dataOut, final Externalizable obj) throws Exception {
            dataOut.writeUTF(obj.getClass().getName());
            final ObjectOutputStream out = new ObjectOutputStream(dataOut);
            obj.writeExternal(out);
            out.flush();
        }
    }

    public static final class ObjectSerializer extends SingletonTypeSerializer<Object> {

        private static final boolean shared = GroupProperties.SERIALIZER_SHARED.getBoolean();
        private static final boolean gzipEnabled = GroupProperties.SERIALIZER_GZIP_ENABLED.getBoolean();

        public final boolean isSuitable(final Class clazz) {
            return Serializable.class.isAssignableFrom(clazz);
        }

        public final int getTypeId() {
            return SERIALIZER_TYPE_OBJECT;
        }

        public final Object read(final FastDataInputStream dataIn) throws Exception {
            final ObjectInputStream in;
            if (gzipEnabled) {
                in = newObjectInputStream(new BufferedInputStream(new GZIPInputStream(dataIn)));
            } else {
                in = newObjectInputStream(dataIn);
            }

            final Object result;
            try {
                if (shared) {
                    result = in.readObject();
                } else {
                    result = in.readUnshared();
                }
            } finally {
                closeResource(in);
            }
            return result;
        }

        public final void write(final FastDataOutputStream dataOut, final Object obj) throws Exception {
            final ObjectOutputStream out;
            if (gzipEnabled) {
                out = new ObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(dataOut)));
            } else {
                out = new ObjectOutputStream(dataOut);
            }
            try {
                if (shared) {
                    out.writeObject(obj);
                } else {
                    out.writeUnshared(obj);
                }
                out.flush();
            } finally {
                closeResource(out);
            }
        }
    }

    private abstract static class SingletonTypeSerializer<T> implements TypeSerializer<T> {

        abstract boolean isSuitable(Class clazz);

        public void destroy() {
        }
    }

    private DefaultSerializers() {}

}
