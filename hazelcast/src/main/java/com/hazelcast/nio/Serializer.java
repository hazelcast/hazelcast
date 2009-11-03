/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.config.ConfigProperty;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.impl.ThreadContext;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public final class Serializer {

    private static final byte SERIALIZER_TYPE_DATA = 0;

    private static final byte SERIALIZER_TYPE_OBJECT = 1;

    private static final byte SERIALIZER_TYPE_BYTE_ARRAY = 2;

    private static final byte SERIALIZER_TYPE_INTEGER = 3;

    private static final byte SERIALIZER_TYPE_LONG = 4;

    private static final byte SERIALIZER_TYPE_CLASS = 5;

    private static final byte SERIALIZER_TYPE_STRING = 6;

    private static TypeSerializer[] typeSerizalizers = new TypeSerializer[7];

    static {
        registerTypeSerializer(new ObjectSerializer());
        registerTypeSerializer(new LongSerializer());
        registerTypeSerializer(new IntegerSerializer());
        registerTypeSerializer(new StringSerializer());
        registerTypeSerializer(new ClassSerializer());
        registerTypeSerializer(new ByteArraySerializer());
        registerTypeSerializer(new DataSerializer());
    }

    final FastByteArrayOutputStream bbos;

    final FastByteArrayInputStream bbis;

    public Serializer() {
        bbos = new FastByteArrayOutputStream(100 * 1024);
        bbis = new FastByteArrayInputStream(new byte[10]);
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
        } else if (obj instanceof Class) {
            typeId = SERIALIZER_TYPE_CLASS;
        }
        bbos.writeByte(typeId);
        typeSerizalizers[typeId].write(bbos, obj);
        bbos.flush();
        Data data = new Data(bbos.getBytes(), bbos.size());
        data.postRead();
        return data;
    }

    public Object readObject(Data data) {
        if (data == null || data.size() == 0)
            return null;
        try {
            bbis.set(data.buffer.array(), data.size());
            byte typeId = bbis.readByte();
            Object obj = typeSerizalizers[typeId].read(bbis);
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
        typeSerizalizers[ts.getTypeId()] = ts;
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
            return Class.forName(bbis.readUTF());
        }

        public void write(FastByteArrayOutputStream bbos, Class obj) throws Exception {
            bbos.writeUTF(obj.getName());
        }
    }

    static class StringSerializer implements TypeSerializer<String> {
        private static final int STRING_CHUNK_SIZE = 16 * 1024;

        public byte getTypeId() {
            return SERIALIZER_TYPE_STRING;
        }

        public String read(FastByteArrayInputStream bbis) throws Exception {
            StringBuilder result = new StringBuilder();
            while (bbis.available() > 0) {
                result.append(bbis.readShortUTF());
            }
            return result.toString();
        }

        public void write(FastByteArrayOutputStream bbos, String obj) throws Exception {
            int length = obj.length();
            int chunkSize = length / STRING_CHUNK_SIZE + 1;
            for (int i = 0; i < chunkSize; i++) {
                int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
                int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
                bbos.writeShortUTF(obj.substring(beginIndex, endIndex));
            }
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
            String className = bbis.readShortUTF();
            try {
                DataSerializable ds = (DataSerializable) Class.forName(className).newInstance();
                ds.readData(bbis);
                return ds;
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("Problem reading DataSerializable class : " + className + ", exception: " + e);
            }
        }

        public void write(FastByteArrayOutputStream bbos, DataSerializable obj) throws Exception {
            bbos.writeShortUTF(obj.getClass().getName());
            obj.writeData(bbos);
        }
    }

    static class ObjectSerializer implements TypeSerializer<Object> {
        static final boolean shared = ConfigProperty.SERIALIZER_SHARED.getBoolean(false);

        public byte getTypeId() {
            return SERIALIZER_TYPE_OBJECT;
        }

        public Object read(FastByteArrayInputStream bbis) throws Exception {
            ObjectInputStream in = new ObjectInputStream(bbis);
            if (shared) {
                return in.readObject();
            } else {
                return in.readUnshared();
            }
        }

        public void write(FastByteArrayOutputStream bbos, Object obj) throws Exception {
            ObjectOutputStream os = new ObjectOutputStream(bbos);
            if (shared) {
                os.writeObject(obj);
            } else {
                os.writeUnshared(obj);
            }
        }
    }

    interface TypeSerializer<T> {
        byte getTypeId();

        void write(FastByteArrayOutputStream bbos, T obj) throws Exception;

        T read(FastByteArrayInputStream bbis) throws Exception;
    }
}