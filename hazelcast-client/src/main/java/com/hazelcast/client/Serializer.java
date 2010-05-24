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

import com.hazelcast.impl.GroupProperties;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.FastByteArrayInputStream;
import com.hazelcast.nio.FastByteArrayOutputStream;

import java.io.*;
import java.math.BigInteger;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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

    private static final boolean gzipEnabled = GroupProperties.SERIALIZER_GZIP_ENABLED.getBoolean();

    public static byte[] toByte(Object object) {
        FastByteArrayOutputStream dos = new FastByteArrayOutputStream();
        dos.reset();
        if (object == null) return new byte[0];
        try {
            if (object instanceof DataSerializable) {
                dos.writeByte(SERIALIZER_TYPE_DATA);
                dos.writeUTF(object.getClass().getName().replaceFirst("com.hazelcast.client", "com.hazelcast"));
                ((DataSerializable) object).writeData(dos);
            } else if (object instanceof String) {
                String string = (String) object;
                dos.writeByte(SERIALIZER_TYPE_STRING);
                dos.writeUTF(string);
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
                if (gzipEnabled) writeGZip(dos, object);
                else writeNormal(dos, object);
            }
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dos.toByteArray();
    }

    public static Object toObject(byte[] bytes) {
        FastByteArrayInputStream dis = new FastByteArrayInputStream(bytes);
        int type = dis.read();
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
                return dis.readUTF();
            } else if (type == SERIALIZER_TYPE_BYTE_ARRAY) {
                int size = dis.readInt();
                byte[] b = new byte[size];
                int redSize = dis.read(b);
                if (size != 0 && size != redSize) {
                    throw new RuntimeException("Couldn't read all of the data Size: " + size + ", But I red:" + redSize);
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
                byte[] intBytes = new byte[dis.readInt()];
                dis.read(intBytes);
                return new BigInteger(intBytes);
            } else if (type == SERIALIZER_TYPE_OBJECT) {
                if (gzipEnabled) return readGZip(dis);
                else return readNormal(dis);
            }
            dis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void writeGZip(OutputStream out, Object object) throws IOException {
        OutputStream zos = new BufferedOutputStream(new GZIPOutputStream(out));
        ObjectOutputStream os = new ObjectOutputStream(zos);
        os.writeObject(object);
        os.flush();
        os.close();
    }

    private static void writeNormal(OutputStream out, Object object) throws IOException {
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(object);
        os.flush();
        os.close();
    }

    private static Object readGZip(InputStream is) throws Exception {
        InputStream zis = new BufferedInputStream(new GZIPInputStream(is));
        ObjectInputStream in = new ObjectInputStream(zis);
        Object o = in.readObject();
        in.close();
        return o;
    }

    private static Object readNormal(InputStream is) throws Exception {
        ObjectInputStream in = new ObjectInputStream(is);
        Object o = in.readObject();
        in.close();
        return o;
    }
}
