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

import static com.hazelcast.impl.Constants.IO.BYTE_BUFFER_SIZE;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.config.ConfigProperty;
import static com.hazelcast.nio.BufferUtil.createNewData;
import static com.hazelcast.nio.BufferUtil.doHardCopy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;


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

    final BuffersOutputStream bbos;

    final BuffersInputStream bbis;

    final DataBufferProvider bufferProvider;

    public Serializer() {
        bbos = new BuffersOutputStream();
        bbis = new BuffersInputStream();
        bufferProvider = new DataBufferProvider();
        bbos.setBufferProvider(bufferProvider);
        bbis.setBufferProvider(bufferProvider);
    }

    public Data writeObject(Object obj) throws Exception {
        if (obj instanceof Data) {
            return doHardCopy((Data) obj);
        }
        Data data = createNewData();
        bufferProvider.setData(data);
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
        data.postRead();
        return data;
    }

    public Object readObject(Data data) {
        return readObject(data, true);
    }

    public Object readObject(Data data, boolean purgeData) {
        if (data == null || data.size() == 0)
            return null;
        Object result = null;
        try {
            bbis.reset();
            bufferProvider.setData(data);
            byte typeId = bbis.readByte();
            result = typeSerizalizers[typeId].read(bbis, data);
            if (purgeData) data.setNoData();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private static void registerTypeSerializer(TypeSerializer ts) {
        typeSerizalizers[ts.getTypeId()] = ts;
    }

    static class LongSerializer implements TypeSerializer<Long> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_LONG;
        }

        public Long read(BuffersInputStream bbis, Data data) throws Exception {
            return bbis.readLong();
        }

        public void write(BuffersOutputStream bbos, Long obj) throws Exception {
            bbos.writeLong(obj.longValue());
        }
    }

    static class IntegerSerializer implements TypeSerializer<Integer> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_INTEGER;
        }

        public Integer read(BuffersInputStream bbis, Data data) throws Exception {
            return bbis.readInt();
        }

        public void write(BuffersOutputStream bbos, Integer obj) throws Exception {
            bbos.writeInt(obj.intValue());
        }
    }

    static class ClassSerializer implements TypeSerializer<Class> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_CLASS;
        }

        public Class read(BuffersInputStream bbis, Data data) throws Exception {
            return Class.forName(bbis.readUTF());
        }

        public void write(BuffersOutputStream bbos, Class obj) throws Exception {
            bbos.writeUTF(obj.getName());
        }
    }

    static class StringSerializer implements TypeSerializer<String> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_STRING;
        }

        public String read(BuffersInputStream bbis, Data data) throws Exception {
            return bbis.readUTF();
        }

        public void write(BuffersOutputStream bbos, String obj) throws Exception {
            bbos.writeUTF(obj);
        }
    }

    static class ByteArraySerializer implements TypeSerializer<byte[]> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_BYTE_ARRAY;
        }

        public byte[] read(BuffersInputStream bbis, Data data) throws Exception {
            int size = bbis.readInt();
            byte[] bytes = new byte[size];
            bbis.read(bytes);
            return bytes;
        }

        public void write(BuffersOutputStream bbos, byte[] obj) throws Exception {
            bbos.writeInt(obj.length);
            bbos.write(obj);
        }
    }

    static class DataSerializer implements TypeSerializer<DataSerializable> {
        public byte getTypeId() {
            return SERIALIZER_TYPE_DATA;
        }

        public DataSerializable read(BuffersInputStream bbis, Data data) throws Exception {
            String className = bbis.readUTF();
            try {
                DataSerializable ds = (DataSerializable) Class.forName(className).newInstance();
                ds.readData(bbis);
                return ds;
            } catch (Exception e) {
                throw new IOException("Problem reading DataSerializable class : " + className + ", exception: " + e);
            } 
        }

        public void write(BuffersOutputStream bbos, DataSerializable obj) throws Exception {
            bbos.writeUTF(obj.getClass().getName());
            obj.writeData(bbos);
        }
    }

    static class ObjectSerializer implements TypeSerializer<Object> {
        static final boolean shared = ConfigProperty.SERIALIZER_SHARED.getBoolean(false);
        
        public byte getTypeId() {
            return SERIALIZER_TYPE_OBJECT;
        }

        public Object read(BuffersInputStream bbis, Data data) throws Exception {
            ObjectInputStream in = new ObjectInputStream(bbis);
            if (shared) {
                return in.readObject() ;
            } else {
                return in.readUnshared();
            }
        }

        public void write(BuffersOutputStream bbos, Object obj) throws Exception {
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

        void write(BuffersOutputStream bbos, T obj) throws Exception;

        T read(BuffersInputStream bbis, Data data) throws Exception;
    }

    public class DataBufferProvider implements BufferProvider {
        Data theData = null;

        public DataBufferProvider() {
            super();
        }

        public void setData(Data theData) {
            this.theData = theData;
        }

        public Data getData() {
            return theData;
        }

        public void addBuffer(ByteBuffer bb) {
            theData.add(bb);
        }

        public ByteBuffer getBuffer(int index) {
            if (index >= theData.lsData.size())
                return null;
            return theData.lsData.get(index);
        }

        public int size() {
            return theData.size;
        }

        public ByteBuffer takeEmptyBuffer() {
            ByteBuffer empty = ThreadContext.get().getBufferPool().obtain();
            if (empty.position() != 0 || empty.limit() != BYTE_BUFFER_SIZE)
                throw new RuntimeException("" + empty);
            return empty;
        }
    }
}