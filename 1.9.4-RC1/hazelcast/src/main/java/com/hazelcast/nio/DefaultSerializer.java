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

import com.hazelcast.impl.GroupProperties;

import java.io.*;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.hazelcast.nio.AbstractSerializer.newInstance;
import static com.hazelcast.nio.AbstractSerializer.newObjectInputStream;

public class DefaultSerializer implements CustomSerializer {

    private static final byte SERIALIZER_TYPE_OBJECT = 0;

    private static final byte SERIALIZER_TYPE_BYTE_ARRAY = 1;

    private static final byte SERIALIZER_TYPE_INTEGER = 2;

    private static final byte SERIALIZER_TYPE_LONG = 3;

    private static final byte SERIALIZER_TYPE_CLASS = 4;

    private static final byte SERIALIZER_TYPE_STRING = 5;

    private static final byte SERIALIZER_TYPE_DATE = 6;

    private static final byte SERIALIZER_TYPE_BIG_INTEGER = 7;

    private static final byte SERIALIZER_TYPE_EXTERNALIZABLE = 8;

    private static final int SERIALIZER_PRIORITY_OBJECT = Integer.MAX_VALUE;

    private static final int SERIALIZER_PRIORITY_BYTE_ARRAY = 100;

    private static final int SERIALIZER_PRIORITY_INTEGER = 300;

    private static final int SERIALIZER_PRIORITY_LONG = 200;

    private static final int SERIALIZER_PRIORITY_CLASS = 500;

    private static final int SERIALIZER_PRIORITY_STRING = 400;

    private static final int SERIALIZER_PRIORITY_DATE = 500;

    private static final int SERIALIZER_PRIORITY_BIG_INTEGER = 600;

    private static final int SERIALIZER_PRIORITY_EXTERNALIZABLE = 50;

    private static final boolean shared = GroupProperties.SERIALIZER_SHARED.getBoolean();
    private static final boolean gzipEnabled = GroupProperties.SERIALIZER_GZIP_ENABLED.getBoolean();

    private static final Collection<TypeSerializer> serializers = new TreeSet<TypeSerializer>(new Comparator<TypeSerializer>() {
        public int compare(TypeSerializer o1, TypeSerializer o2) {
            final int p1 = o1.priority();
            final int p2 = o2.priority();
            return p1 < p2 ? -1 : p1 == p2 ? (o1.getTypeId() - o2.getTypeId()) : 1;
        }
    });

    private TypeSerializer[] typeSerializer;

    static {
        registerSerializer(new ByteArraySerializer());
        registerSerializer(new LongSerializer());
        registerSerializer(new IntegerSerializer());
        registerSerializer(new StringSerializer());
        registerSerializer(new ClassSerializer());
        registerSerializer(new DateSerializer());
        registerSerializer(new BigIntegerSerializer());
        registerSerializer(new Externalizer());
        registerSerializer(new ObjectSerializer());
    }

    public static void registerSerializer(TypeSerializer ts) {
        if (ts != null) {
            serializers.add(ts);
        }
    }

    public DefaultSerializer() {
        this.typeSerializer = new TypeSerializer[serializers.size()];
        for (TypeSerializer ts : serializers) {
            this.typeSerializer[ts.getTypeId()] = ts;
        }
    }

    public void write(OutputStream os, Object obj) throws Exception {
        FastByteArrayOutputStream bos = (FastByteArrayOutputStream) os;
        byte typeId = -1;
        for (TypeSerializer ts : serializers) {
            if (ts.isSuitable(obj)) {
                this.typeSerializer[ts.getTypeId()] = ts;
                typeId = ts.getTypeId();
                break;
            }
        }
        if (typeId == -1) {
            throw new NotSerializableException("There is no suitable serializer for " + obj.getClass().getName());
        }
        bos.writeByte(typeId);
        this.typeSerializer[typeId].write(bos, obj);
    }

    public Object read(InputStream is) throws Exception {
        FastByteArrayInputStream bis = (FastByteArrayInputStream) is;
        final byte typeId = bis.readByte();
        if ((typeId < 0) || (typeId >= this.typeSerializer.length)) {
            throw new IllegalArgumentException("There is no suitable deserializer for type 0x"
                    + Integer.toHexString(typeId));
        }
        Object result = this.typeSerializer[typeId].read(bis);
        return result;
    }
//    public byte[] write(Object object) throws Exception {
//        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    public Object read(byte[] bytes) throws Exception {
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }

    public static class LongSerializer implements TypeSerializer<Long> {
        public final int priority() {
            return SERIALIZER_PRIORITY_LONG;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof Long;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_LONG;
        }

        public final Long read(final FastByteArrayInputStream bbis) throws Exception {
            return bbis.readLong();
        }

        public final void write(final FastByteArrayOutputStream bbos, final Long obj) throws Exception {
            bbos.writeLong(obj.longValue());
        }
    }

    public static class DateSerializer implements TypeSerializer<Date> {
        public int priority() {
            return SERIALIZER_PRIORITY_DATE;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof Date;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_DATE;
        }

        public final Date read(final FastByteArrayInputStream bbis) throws Exception {
            return new Date(bbis.readLong());
        }

        public final void write(final FastByteArrayOutputStream bbos, final Date obj) throws Exception {
            bbos.writeLong(obj.getTime());
        }
    }

    public static class BigIntegerSerializer implements TypeSerializer<BigInteger> {
        public final int priority() {
            return SERIALIZER_PRIORITY_BIG_INTEGER;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof BigInteger;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_BIG_INTEGER;
        }

        public final BigInteger read(final FastByteArrayInputStream bbis) throws Exception {
            final byte[] bytes = new byte[bbis.readInt()];
            bbis.readFully(bytes);
            return new BigInteger(bytes);
        }

        public final void write(final FastByteArrayOutputStream bbos, final BigInteger obj) throws Exception {
            final byte[] bytes = obj.toByteArray();
            bbos.writeInt(bytes.length);
            bbos.write(bytes);
        }
    }

    public static class IntegerSerializer implements TypeSerializer<Integer> {
        public final int priority() {
            return SERIALIZER_PRIORITY_INTEGER;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof Integer;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_INTEGER;
        }

        public final Integer read(final FastByteArrayInputStream bbis) throws Exception {
            return bbis.readInt();
        }

        public final void write(final FastByteArrayOutputStream bbos, final Integer obj) throws Exception {
            bbos.writeInt(obj.intValue());
        }
    }

    public static class ClassSerializer implements TypeSerializer<Class> {
        public final int priority() {
            return SERIALIZER_PRIORITY_CLASS;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof Class;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_CLASS;
        }

        public final Class read(final FastByteArrayInputStream bbis) throws Exception {
            return loadClass(bbis.readUTF());
        }

        protected Class loadClass(final String className) throws ClassNotFoundException {
            return AbstractSerializer.loadClass(className);
        }

        public final void write(final FastByteArrayOutputStream bbos, final Class obj) throws Exception {
            bbos.writeUTF(obj.getName());
        }
    }

    public static class StringSerializer implements TypeSerializer<String> {
        public final int priority() {
            return SERIALIZER_PRIORITY_STRING;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof String;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_STRING;
        }

        public final String read(final FastByteArrayInputStream bbis) throws Exception {
            return bbis.readUTF();
        }

        public final void write(final FastByteArrayOutputStream bbos, final String obj) throws Exception {
            bbos.writeUTF(obj);
        }
    }

    public static class ByteArraySerializer implements TypeSerializer<byte[]> {
        public final int priority() {
            return SERIALIZER_PRIORITY_BYTE_ARRAY;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof byte[];
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_BYTE_ARRAY;
        }

        public final byte[] read(final FastByteArrayInputStream bbis) throws Exception {
            final int size = bbis.readInt();
            final byte[] bytes = new byte[size];
            bbis.readFully(bytes);
            return bytes;
        }

        public final void write(final FastByteArrayOutputStream bbos, final byte[] obj) throws Exception {
            bbos.writeInt(obj.length);
            bbos.write(obj);
        }
    }

    public static class Externalizer implements TypeSerializer<Externalizable> {
        public final int priority() {
            return SERIALIZER_PRIORITY_EXTERNALIZABLE;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof Externalizable;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_EXTERNALIZABLE;
        }

        public final Externalizable read(final FastByteArrayInputStream bbis) throws Exception {
            final String className = bbis.readUTF();
            try {
                final Externalizable ds = (Externalizable) newInstance(AbstractSerializer.loadClass(className));
                ds.readExternal(newObjectInputStream(bbis));
                return ds;
            } catch (final Exception e) {
                throw new IOException("Problem reading Externalizable class : " + className + ", exception: " + e);
            }
        }

        public final void write(final FastByteArrayOutputStream bbos, final Externalizable obj) throws Exception {
            bbos.writeUTF(obj.getClass().getName());
            final ObjectOutputStream out = new ObjectOutputStream(bbos);
            obj.writeExternal(out);
            out.flush();
        }
    }

    public static class ObjectSerializer implements TypeSerializer<Object> {
        public final int priority() {
            return SERIALIZER_PRIORITY_OBJECT;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof Serializable;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_OBJECT;
        }

        public final Object read(final FastByteArrayInputStream bbis) throws Exception {
            if (gzipEnabled) {
                return readGZip(bbis);
            }
            return readNormal(bbis);
        }

        public final void write(final FastByteArrayOutputStream bbos, final Object obj) throws Exception {
            if (gzipEnabled) {
                writeGZip(bbos, obj);
            } else {
                writeNormal(bbos, obj);
            }
        }

        private Object readGZip(final FastByteArrayInputStream bbis) throws Exception {
            final InputStream zis = new BufferedInputStream(new GZIPInputStream(bbis));
            final ObjectInputStream in = newObjectInputStream(zis);
            Object result;
            if (shared) {
                result = in.readObject();
            } else {
                result = in.readUnshared();
            }
            in.close();
            return result;
        }

        private Object readNormal(final FastByteArrayInputStream bbis) throws Exception {
            final ObjectInputStream in = newObjectInputStream(bbis);
            Object result;
            if (shared) {
                result = in.readObject();
            } else {
                result = in.readUnshared();
            }
            in.close();
            return result;
        }

        private void writeGZip(final FastByteArrayOutputStream bbos, final Object obj) throws Exception {
            final OutputStream zos = new BufferedOutputStream(new GZIPOutputStream(bbos));
            final ObjectOutputStream os = new ObjectOutputStream(zos);
            if (shared) {
                os.writeObject(obj);
            } else {
                os.writeUnshared(obj);
            }
            os.flush();
            os.close();
        }

        private void writeNormal(final FastByteArrayOutputStream bbos, final Object obj) throws Exception {
            final ObjectOutputStream os = new ObjectOutputStream(bbos);
            if (shared) {
                os.writeObject(obj);
            } else {
                os.writeUnshared(obj);
            }
            os.flush();
            os.close();
        }
    }
}
