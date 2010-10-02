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

import java.io.*;
import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

public abstract class AbstractSerializer {

    private static final ILogger logger = Logger.getLogger(AbstractSerializer.class.getName());

    private static final byte SERIALIZER_TYPE_DATA = 0;

    private static final byte SERIALIZER_TYPE_OBJECT = 1;

    private static final byte SERIALIZER_TYPE_BYTE_ARRAY = 2;

    private static final byte SERIALIZER_TYPE_INTEGER = 3;

    private static final byte SERIALIZER_TYPE_LONG = 4;

    private static final byte SERIALIZER_TYPE_CLASS = 5;

    private static final byte SERIALIZER_TYPE_STRING = 6;

    private static final byte SERIALIZER_TYPE_DATE = 7;

    private static final byte SERIALIZER_TYPE_BIG_INTEGER = 8;

    private static final byte SERIALIZER_TYPE_EXTERNALIZABLE = 9;

    private static final boolean shared = GroupProperties.SERIALIZER_SHARED.getBoolean();
    private static final boolean gzipEnabled = GroupProperties.SERIALIZER_GZIP_ENABLED.getBoolean();

    private final TypeSerializer[] serializer;
    private final TypeSerializer[] typeSerializer;
    
    protected static TypeSerializer[] sort(final TypeSerializer[] serializers){
        Arrays.sort(serializers, new Comparator<TypeSerializer>() {
            public int compare(TypeSerializer o1, TypeSerializer o2) {
                final int p1 = o1.priority();
                final int p2 = o1.priority();
                return p1 < p2 ? -1 : p1 == p2 ? 0 : 1;
            }
        });
        return serializers;
    }

    public AbstractSerializer(final TypeSerializer[] serializer) {
        
        this.serializer = serializer;
        this.typeSerializer = new TypeSerializer[serializer.length];
        for(int i = 0; i < serializer.length; i++){
            this.typeSerializer[serializer[i].getTypeId()] = serializer[i];
        }
    }

    public static Object newInstance(final Class klass) throws Exception {
        final Constructor<?> constructor = klass.getDeclaredConstructor();
        if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
        return constructor.newInstance();
    }

    public static Class<?> classForName(final String className) throws ClassNotFoundException {
        return classForName(null, className);
    }

    public static Class<?> classForName(final ClassLoader classLoader, final String className) throws ClassNotFoundException {
        if (className == null) {
            throw new IllegalArgumentException("ClassName cannot be null!");
        }
        if (className.startsWith("com.hazelcast")) {
            return Class.forName(className, true, AbstractSerializer.class.getClassLoader());
        }
        ClassLoader theClassLoader = classLoader;
        if (theClassLoader == null) {
            theClassLoader = Thread.currentThread().getContextClassLoader();
        }
        if (theClassLoader != null) {
            return Class.forName(className, true, theClassLoader);
        }
        return Class.forName(className);
    }

    protected final void toByte(final FastByteArrayOutputStream bos, final Object object) {
        if (object == null) {
            return;
        }
        try{
            byte typeId = -1;
            for(int i = 0; i < this.serializer.length; i++){
                if (this.serializer[i].isSuitable(object)){
                    typeId = this.serializer[i].getTypeId();
                    break;
                }
            }
            if (typeId == -1){
                throw new NotSerializableException("There is no suitable serializer for " + object.getClass().getName());
            }
            bos.writeByte(typeId);
            this.typeSerializer[typeId].write(bos, object);
            bos.flush();
        } catch (final Throwable e){
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    protected final Object toObject(final FastByteArrayInputStream bis) {
        try {
            final byte typeId = bis.readByte();
            if ((typeId < 0) || (typeId >= this.typeSerializer.length)){
                throw new IllegalArgumentException("There is no suitable deseializer for type 0x"
                    + Integer.toHexString(typeId));
            }
            return this.typeSerializer[typeId].read(bis);
        } catch (final Throwable e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static class LongSerializer implements TypeSerializer<Long> {
        public final int priority() {
            return 200;
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
            return 500;
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
            return 600;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof BigInteger;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_BIG_INTEGER;
        }

        public final BigInteger read(final FastByteArrayInputStream bbis) throws Exception {
            final byte[] bytes = new byte[bbis.readInt()];
            bbis.read(bytes);
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
            return 300;
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
            return 500;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof Class;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_CLASS;
        }

        public final Class read(final FastByteArrayInputStream bbis) throws Exception {
            return classForName(bbis.readUTF());
        }

        protected Class classForName(final String className) throws ClassNotFoundException {
            return AbstractSerializer.classForName(className);
        }

        public final void write(final FastByteArrayOutputStream bbos, final Class obj) throws Exception {
            bbos.writeUTF(obj.getName());
        }
    }

    public static class StringSerializer implements TypeSerializer<String> {
        public final int priority() {
            return 400;
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
            return 100;
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
            bbis.read(bytes);
            return bytes;
        }

        public final void write(final FastByteArrayOutputStream bbos, final byte[] obj) throws Exception {
            bbos.writeInt(obj.length);
            bbos.write(obj);
        }
    }

    public static class DataSerializer implements TypeSerializer<DataSerializable> {
        public final int priority() {
            return 0;
        }

        public final boolean isSuitable(final Object obj) {
            return obj instanceof DataSerializable;
        }

        public final byte getTypeId() {
            return SERIALIZER_TYPE_DATA;
        }

        protected Class classForName(final String className) throws ClassNotFoundException {
            return AbstractSerializer.classForName(className);
        }

        protected String toClassName(final Class clazz) throws ClassNotFoundException {
            return clazz.getName();
        }

        public final DataSerializable read(final FastByteArrayInputStream bbis) throws Exception {
            final String className = bbis.readUTF();
            try {
                final DataSerializable ds = (DataSerializable) newInstance(classForName(className));
                ds.readData(bbis);
                return ds;
            } catch (final Exception e) {
                e.printStackTrace();
                throw new IOException("Problem reading DataSerializable class : " + className + ", exception: " + e);
            }
        }

        public final void write(final FastByteArrayOutputStream bbos, final DataSerializable obj) throws Exception {
            bbos.writeUTF(toClassName(obj.getClass()));
            obj.writeData(bbos);
        }
    }

    public static class Externalizer implements TypeSerializer<Externalizable> {
        public final int priority() {
            return 50;
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
            	final Externalizable ds = (Externalizable) newInstance(classForName(className));
                ds.readExternal(newObjectInputStream(bbis));
                return ds;
            } catch (final Exception e) {
                e.printStackTrace();
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

    public static final ObjectInputStream newObjectInputStream(final InputStream in) throws IOException {
        return new ObjectInputStream(in) {
            @Override
            protected Class<?> resolveClass(final ObjectStreamClass desc) throws ClassNotFoundException {
                return classForName(desc.getName());
            }
        };
    }

    public static class ObjectSerializer implements TypeSerializer<Object> {
        public final int priority() {
            return Integer.MAX_VALUE;
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

    public static interface TypeSerializer<T> {
        int priority();

        boolean isSuitable(Object obj);

        byte getTypeId();

        void write(FastByteArrayOutputStream bbos, T obj) throws Exception;

        T read(FastByteArrayInputStream bbis) throws Exception;
    }
}
