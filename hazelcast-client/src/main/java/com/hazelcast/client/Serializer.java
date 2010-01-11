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

package com.hazelcast.client;

import com.hazelcast.nio.DataSerializable;

import java.io.*;
import java.math.BigInteger;
import java.util.Date;

public class Serializer {
    private static final byte SERIALIZER_TYPE_DATA = 0;

    private static final byte SERIALIZER_TYPE_OBJECT = 1;

    private static final byte SERIALIZER_TYPE_BYTE_ARRAY = 2;

    private static final byte SERIALIZER_TYPE_INTEGER = 3;

    private static final byte SERIALIZER_TYPE_LONG = 4;

    private static final byte SERIALIZER_TYPE_CLASS = 5;

    private static final byte SERIALIZER_TYPE_STRING = 6;

    private static final byte SERIALIZER_TYPE_DATE = 7;

    private static final byte SERIALIZER_TYPE_BIG_INTEGER = 8;

    private static final int STRING_CHUNK_SIZE = 16 * 1024;

    public static byte[] toByte(Object object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        if (object == null) return new byte[0];
        try {
            if (object instanceof DataSerializable) {
                dos.writeByte(SERIALIZER_TYPE_DATA);
                dos.writeUTF(object.getClass().getName().replaceFirst("com.hazelcast.client", "com.hazelcast"));
                ((DataSerializable) object).writeData(dos);
            } else if (object instanceof String) {
                String string = (String) object;
                dos.writeByte(SERIALIZER_TYPE_STRING);
                int length = string.length();
                int chunkSize = length / STRING_CHUNK_SIZE + 1;
                for (int i = 0; i < chunkSize; i++) {
                    int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
                    int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
                    dos.writeUTF(string.substring(beginIndex, endIndex));
                }
            } else if (object instanceof byte[]) {
                byte[] bytes = (byte[]) object;
                dos.writeByte(SERIALIZER_TYPE_BYTE_ARRAY);
                dos.writeInt(bytes.length);
                dos.write(bytes);
            } else if (object instanceof Integer) {
                dos.writeByte(SERIALIZER_TYPE_INTEGER);
                dos.writeInt((Integer) object);
            } else if (object instanceof Long) {
                dos.writeByte(SERIALIZER_TYPE_LONG);
                dos.writeLong((Long) object);
            } else if (object instanceof Class) {
                dos.writeByte(SERIALIZER_TYPE_CLASS);
                dos.writeUTF(((Class<?>) object).getName());
            } else if (object instanceof Date) {
                dos.writeByte(SERIALIZER_TYPE_DATE);
                dos.writeLong(((Date) object).getTime());
            } else if (object instanceof BigInteger) {
                dos.writeByte(SERIALIZER_TYPE_BIG_INTEGER);
                byte[] bytes = ((BigInteger) object).toByteArray();
                dos.writeInt(bytes.length);
                dos.write(bytes);
            } else {
                dos.writeByte(SERIALIZER_TYPE_OBJECT);
                ObjectOutputStream os = new ObjectOutputStream(dos);
                os.writeObject(object);
                os.flush();
                os.close();
            }
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] bytes = bos.toByteArray();
        return bytes;
    }

    public static Object toObject(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);
        int type = bis.read();
        try {
            if (type == SERIALIZER_TYPE_DATA) {
                String className = dis.readUTF();
                if (className.equals("com.hazelcast.impl.Keys")) {
                    className = "com.hazelcast.client.impl.CollectionWrapper";
                } else if (className.equals("com.hazelcast.impl.CMap$Values")) {
                    className = "com.hazelcast.client.impl.Values";
                }
                DataSerializable data = (DataSerializable) Class.forName(className).newInstance();
                data.readData(dis);
                return data;
            } else if (type == SERIALIZER_TYPE_STRING) {
                StringBuilder result = new StringBuilder();
                while (dis.available() > 0) {
                    result.append(dis.readUTF());
                }
                return result.toString();
            } else if (type == SERIALIZER_TYPE_BYTE_ARRAY) {
                int size = dis.readInt();
                byte[] b = new byte[size];
                int redSize = bis.read(b);
                if (size != redSize) {
                    throw new RuntimeException("Couldn't read all of the data");
                }
                return b;
            } else if (type == SERIALIZER_TYPE_INTEGER) {
                return dis.readInt();
            } else if (type == SERIALIZER_TYPE_LONG) {
                return dis.readLong();
            } else if (type == SERIALIZER_TYPE_CLASS) {
                return Class.forName(dis.readUTF());
            } else if (type == SERIALIZER_TYPE_DATE) {
                return new Date(dis.readLong());
            } else if (type == SERIALIZER_TYPE_BIG_INTEGER) {
                byte[] byts = new byte[dis.readInt()];
                dis.read(byts);
                return new BigInteger(byts);
            } else if (type == SERIALIZER_TYPE_OBJECT) {
                ObjectInputStream os = new ObjectInputStream(dis);
                Object o = os.readObject();
                os.close();
                return o;
            }
            dis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
