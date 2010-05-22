/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio;

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.impl.ThreadContext;

import java.io.*;
import java.math.BigInteger;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class Serializer {

    private static final byte SERIALIZER_TYPE_DATA = 0;

    private static final byte SERIALIZER_TYPE_OBJECT = 1;

    private static final byte SERIALIZER_TYPE_BYTE_ARRAY = 2;

    private static final byte SERIALIZER_TYPE_INTEGER = 3;

    private static final byte SERIALIZER_TYPE_LONG = 4;

    private static final byte SERIALIZER_TYPE_CLASS = 5;

    private static final byte SERIALIZER_TYPE_STRING = 6;

    private static final byte SERIALIZER_TYPE_DATE = 7;

    private static final byte SERIALIZER_TYPE_BIG_INTEGER = 8;

    private static TypeSerializer[] typeSerializer = new TypeSerializer[9];

    private static int OUTPUT_STREAM_BUFFER_SIZE = 100 * 1024;

    private static final boolean gzipEnabled = Boolean.getBoolean("hazelcast.serializer.gzip.enabled");

    static {
        registerTypeSerializer(new ObjectSerializer());
        registerTypeSerializer(new LongSerializer());
        registerTypeSerializer(new IntegerSerializer());
        registerTypeSerializer(new StringSerializer());
        registerTypeSerializer(new ClassSerializer());
        registerTypeSerializer(new ByteArraySerializer());
        registerTypeSerializer(new DataSerializer());
        registerTypeSerializer(new DateSerializer());
        registerTypeSerializer(new BigIntegerSerializer());
    }

    final FastByteArrayOutputStream bbos;

    final FastByteArrayInputStream bbis;

    public Serializer() {
        bbos = new FastByteArrayOutputStream(OUTPUT_STREAM_BUFFER_SIZE);
        bbis = new FastByteArrayInputStream(new byte[10]);
    }

    public static Class<?> classForName(String className) throws ClassNotFoundException {
        return classForName(null, className);
    }

    public static Class<?> classForName(ClassLoader classLoader, String className) throws ClassNotFoundException {
        if (className == null) {
            throw new IllegalArgumentException("ClassName cannot be null!");
        }
        if (className.startsWith("com.hazelcast")) {
            return Class.forName(className, true, Serializer.class.getClassLoader());
        }
        ClassLoader theClassLoader = classLoader;
        if (theClassLoader == null) {
            theClassLoader = Thread.currentThread().getContextClassLoader();
        }
        if (theClassLoader != null) {
            return Class.forName(className, true, theClassLoader);
        } else {
            return Class.forName(className);
        }
    }

    public Data writeObject(Object obj) throws Exception {
        if (obj instanceof Data) {
            return (Data) obj;
        }
        bbos.reset();
        byte typeId = SERIALIZER_TYPE_OBJECT;
        if (obj instanceof DataSerializable) {
            typeId = SERIALIZER_TYPE_DATA;
        } else if (obj instanceof byte[]) {
            typeId = SERIALIZER_TYPE_BYTE_ARRAY;
        } else if (obj instanceof Long) {
            typeId = SERIALIZER_TYPE_LONG;
        } else if (obj instanceof Integer) {
            typeId = SERIALIZER_TYPE_INTEGER;
        } else if (obj instanceof String) {
            typeId = SERIALIZER_TYPE_STRING;
        } else if (obj instanceof Date) {
            typeId = SERIALIZER_TYPE_DATE;
        } else if (obj instanceof Class) {
            typeId = SERIALIZER_TYPE_CLASS;
        } else if (obj instanceof BigInteger) {
            typeId = SERIALIZER_TYPE_BIG_INTEGER;
        }
        bbos.writeByte(typeId);
        typeSerializer[typeId].write(bbos, obj);
        Data data = new Data(bbos.toByteArray());
        if (bbos.size() > OUTPUT_STREAM_BUFFER_SIZE) {
            bbos.set(new byte[OUTPUT_STREAM_BUFFER_SIZE]);
        }
        return data;
    }

    public Object readObject(Data data) {
        if (data == null || data.size() == 0)
            return null;
        try {
            bbis.set(data.buffer, data.size());
            byte typeId = bbis.readByte();
            Object obj = typeSerializer[typeId].read(bbis);
            if (obj instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) obj).setHazelcastInstance(ThreadContext.get().getCurrentFactory());
            }
            return obj;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static void registerTypeSerializer(TypeSerializer ts) {
        typeSerializer[ts.getTypeId()] = ts;
    }

    static class LongSerializer implements TypeSerializer<Long> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_LONG;
        }

        public Long read(FastByteArrayInputStream bbis) throws Exception {
            return bbis.readLong();
        }

        public void write(FastByteArrayOutputStream bbos, Long obj) throws Exception {
            bbos.writeLong(obj.longValue());
        }
    }

    static class DateSerializer implements TypeSerializer<Date> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_DATE;
        }

        public Date read(FastByteArrayInputStream bbis) throws Exception {
            return new Date(bbis.readLong());
        }

        public void write(FastByteArrayOutputStream bbos, Date obj) throws Exception {
            bbos.writeLong(obj.getTime());
        }
    }

    static class BigIntegerSerializer implements TypeSerializer<BigInteger> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_BIG_INTEGER;
        }

        public BigInteger read(FastByteArrayInputStream bbis) throws Exception {
            byte[] bytes = new byte[bbis.readInt()];
            bbis.read(bytes);
            return new BigInteger(bytes);
        }

        public void write(FastByteArrayOutputStream bbos, BigInteger obj) throws Exception {
            byte[] bytes = obj.toByteArray();
            bbos.writeInt(bytes.length);
            bbos.write(bytes);
        }
    }

    static class IntegerSerializer implements TypeSerializer<Integer> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_INTEGER;
        }

        public Integer read(FastByteArrayInputStream bbis) throws Exception {
            return bbis.readInt();
        }

        public void write(FastByteArrayOutputStream bbos, Integer obj) throws Exception {
            bbos.writeInt(obj.intValue());
        }
    }

    static class ClassSerializer implements TypeSerializer<Class> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_CLASS;
        }

        public Class read(FastByteArrayInputStream bbis) throws Exception {
            return classForName(bbis.readUTF());
        }

        public void write(FastByteArrayOutputStream bbos, Class obj) throws Exception {
            bbos.writeUTF(obj.getName());
        }
    }

    static class StringSerializer implements TypeSerializer<String> {

        public byte getTypeId() {
            return SERIALIZER_TYPE_STRING;
        }

        public String read(FastByteArrayInputStream bbis) throws Exception {
            return bbis.readUTF();
        }

        public void write(FastByteArrayOutputStream bbos, String obj) throws Exception {
            bbos.writeUTF(obj);
        }
    }

    static class ByteArraySerializer implements TypeSerializer<byte[]> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_BYTE_ARRAY;
        }

        public byte[] read(FastByteArrayInputStream bbis) throws Exception {
            int size = bbis.readInt();
            byte[] bytes = new byte[size];
            bbis.read(bytes);
            return bytes;
        }

        public void write(FastByteArrayOutputStream bbos, byte[] obj) throws Exception {
            bbos.writeInt(obj.length);
            bbos.write(obj);
        }
    }

    static class DataSerializer implements TypeSerializer<DataSerializable> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_DATA;
        }

        public DataSerializable read(FastByteArrayInputStream bbis) throws Exception {
            String className = bbis.readUTF();
            try {
                DataSerializable ds = (DataSerializable) classForName(className).newInstance();
                ds.readData(bbis);
                return ds;
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("Problem reading DataSerializable class : " + className + ", exception: " + e);
            }
        }

        public void write(FastByteArrayOutputStream bbos, DataSerializable obj) throws Exception {
            bbos.writeUTF(obj.getClass().getName());
            obj.writeData(bbos);
        }
    }

    public static ObjectInputStream newObjectInputStream(InputStream in) throws IOException {
        return new ObjectInputStream(in) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
                return classForName(desc.getName());
            }
        };
    }

    static class ObjectSerializer implements TypeSerializer<Object> {
        static final boolean shared = GroupProperties.SERIALIZER_SHARED.getBoolean();

        public byte getTypeId() {
            return SERIALIZER_TYPE_OBJECT;
        }

        public Object read(FastByteArrayInputStream bbis) throws Exception {
            if (gzipEnabled) {
                return readGZip(bbis);
            } else {
                return readNormal(bbis);
            }
        }

        public void write(FastByteArrayOutputStream bbos, Object obj) throws Exception {
            if (gzipEnabled) {
                writeGZip(bbos, obj);
            } else {
                writeNormal(bbos, obj);
            }
        }

        private Object readGZip(FastByteArrayInputStream bbis) throws Exception {
            GZIPInputStream zis = new GZIPInputStream(bbis);
            ObjectInputStream in = newObjectInputStream(zis);
            if (shared) {
                return in.readObject();
            } else {
                return in.readUnshared();
            }
        }

        private Object readNormal(FastByteArrayInputStream bbis) throws Exception {
            ObjectInputStream in = newObjectInputStream(bbis);
            if (shared) {
                return in.readObject();
            } else {
                return in.readUnshared();
            }
        }

        private void writeGZip(FastByteArrayOutputStream bbos, Object obj) throws Exception {
            GZIPOutputStream zos = new GZIPOutputStream(bbos);
            ObjectOutputStream os = new ObjectOutputStream(zos);
            if (shared) {
                os.writeObject(obj);
            } else {
                os.writeUnshared(obj);
            }
            os.flush();
            os.close();
        }

        private void writeNormal(FastByteArrayOutputStream bbos, Object obj) throws Exception {
            ObjectOutputStream os = new ObjectOutputStream(bbos);
            if (shared) {
                os.writeObject(obj);
            } else {
                os.writeUnshared(obj);
            }
            os.flush();
            os.close();
        }
    }

    interface TypeSerializer<T> {
        byte getTypeId();

        void write(FastByteArrayOutputStream bbos, T obj) throws Exception;

        T read(FastByteArrayInputStream bbis) throws Exception;
    }
}
