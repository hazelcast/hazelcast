/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.StreamSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_BIG_DECIMAL;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_BIG_INTEGER;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_CLASS;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_DATE;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_ENUM;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_EXTERNALIZABLE;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_SERIALIZABLE;
import static com.hazelcast.nio.IOUtil.newObjectInputStream;


public final class JavaDefaultSerializers {

    public static final class ExternalizableSerializer extends SingletonSerializer<Externalizable> {

        private final boolean gzipEnabled;

        public ExternalizableSerializer(boolean gzipEnabled) {
            this.gzipEnabled = gzipEnabled;
        }

        @Override
        public int getTypeId() {
            return JAVA_DEFAULT_TYPE_EXTERNALIZABLE;
        }

        @Override
        public Externalizable read(final ObjectDataInput in) throws IOException {
            final String className = in.readUTF();
            try {
                final Externalizable ds = ClassLoaderUtil.newInstance(in.getClassLoader(), className);
                final ObjectInputStream objectInputStream;
                final InputStream inputStream = (InputStream) in;
                if (gzipEnabled) {
                    objectInputStream = newObjectInputStream(in.getClassLoader(), new GZIPInputStream(inputStream));
                } else {
                    objectInputStream = newObjectInputStream(in.getClassLoader(), inputStream);
                }
                ds.readExternal(objectInputStream);
                return ds;
            } catch (final Exception e) {
                throw new HazelcastSerializationException("Problem while reading Externalizable class : "
                        + className + ", exception: " + e);
            }
        }

        @Override
        public void write(final ObjectDataOutput out, final Externalizable obj) throws IOException {
            out.writeUTF(obj.getClass().getName());
            final ObjectOutputStream objectOutputStream;
            final OutputStream outputStream = (OutputStream) out;
            GZIPOutputStream gzip = null;
            if (gzipEnabled) {
                gzip = new GZIPOutputStream(outputStream);
                objectOutputStream = new ObjectOutputStream(gzip);
            } else {
                objectOutputStream = new ObjectOutputStream(outputStream);
            }
            obj.writeExternal(objectOutputStream);
            // Force flush if not yet written due to internal behavior if pos < 1024
            objectOutputStream.flush();
            if (gzipEnabled) {
                gzip.finish();
            }
        }
    }

    public static final class BigIntegerSerializer extends SingletonSerializer<BigInteger> {

        @Override
        public int getTypeId() {
            return JAVA_DEFAULT_TYPE_BIG_INTEGER;
        }

        @Override
        public BigInteger read(final ObjectDataInput in) throws IOException {
            final byte[] bytes = new byte[in.readInt()];
            in.readFully(bytes);
            return new BigInteger(bytes);
        }

        @Override
        public void write(final ObjectDataOutput out, final BigInteger obj) throws IOException {
            final byte[] bytes = obj.toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    public static final class BigDecimalSerializer extends SingletonSerializer<BigDecimal> {

        final BigIntegerSerializer bigIntegerSerializer = new BigIntegerSerializer();

        @Override
        public int getTypeId() {
            return JAVA_DEFAULT_TYPE_BIG_DECIMAL;
        }

        @Override
        public BigDecimal read(final ObjectDataInput in) throws IOException {
            BigInteger bigInt = bigIntegerSerializer.read(in);
            int scale = in.readInt();
            return new BigDecimal(bigInt, scale);
        }

        @Override
        public void write(final ObjectDataOutput out, final BigDecimal obj) throws IOException {
            BigInteger bigInt = obj.unscaledValue();
            int scale = obj.scale();
            bigIntegerSerializer.write(out, bigInt);
            out.writeInt(scale);
        }
    }

    public static final class DateSerializer extends SingletonSerializer<Date> {

        @Override
        public int getTypeId() {
            return JAVA_DEFAULT_TYPE_DATE;
        }

        @Override
        public Date read(final ObjectDataInput in) throws IOException {
            return new Date(in.readLong());
        }

        @Override
        public void write(final ObjectDataOutput out, final Date obj) throws IOException {
            out.writeLong(obj.getTime());
        }
    }

    public static final class ClassSerializer extends SingletonSerializer<Class> {

        @Override
        public int getTypeId() {
            return JAVA_DEFAULT_TYPE_CLASS;
        }

        @Override
        public Class read(final ObjectDataInput in) throws IOException {
            try {
                return ClassLoaderUtil.loadClass(in.getClassLoader(), in.readUTF());
            } catch (ClassNotFoundException e) {
                throw new HazelcastSerializationException(e);
            }
        }

        @Override
        public void write(final ObjectDataOutput out, final Class obj) throws IOException {
            out.writeUTF(obj.getName());
        }
    }

    public static final class JavaSerializer extends SingletonSerializer<Object> {

        private final boolean shared;
        private final boolean gzipEnabled;

        public JavaSerializer(boolean shared, boolean gzipEnabled) {
            this.shared = shared;
            this.gzipEnabled = gzipEnabled;
        }

        @Override
        public int getTypeId() {
            return JAVA_DEFAULT_TYPE_SERIALIZABLE;
        }

        @Override
        public Object read(final ObjectDataInput in) throws IOException {
            final ObjectInputStream objectInputStream;
            final InputStream inputStream = (InputStream) in;
            if (gzipEnabled) {
                objectInputStream = newObjectInputStream(in.getClassLoader(), new GZIPInputStream(inputStream));
            } else {
                objectInputStream = newObjectInputStream(in.getClassLoader(), inputStream);
            }

            final Object result;
            try {
                if (shared) {
                    result = objectInputStream.readObject();
                } else {
                    result = objectInputStream.readUnshared();
                }
            } catch (ClassNotFoundException e) {
                throw new HazelcastSerializationException(e);
            }
            return result;
        }

        @SuppressFBWarnings("OS_OPEN_STREAM")
        @Override
        public void write(final ObjectDataOutput out, final Object obj) throws IOException {
            final ObjectOutputStream objectOutputStream;
            final OutputStream outputStream = (OutputStream) out;
            GZIPOutputStream gzip = null;
            if (gzipEnabled) {
                gzip = new GZIPOutputStream(outputStream);
                objectOutputStream = new ObjectOutputStream(gzip);
            } else {
                objectOutputStream = new ObjectOutputStream(outputStream);
            }
            if (shared) {
                objectOutputStream.writeObject(obj);
            } else {
                objectOutputStream.writeUnshared(obj);
            }
            // Force flush if not yet written due to internal behavior if pos < 1024
            objectOutputStream.flush();
            if (gzipEnabled) {
                gzip.finish();
            }
        }
    }

    public static final class EnumSerializer extends SingletonSerializer<Enum> {

        @Override
        public int getTypeId() {
            return JAVA_DEFAULT_TYPE_ENUM;
        }

        @Override
        public void write(ObjectDataOutput out, Enum obj) throws IOException {
            String name = obj.getDeclaringClass().getName();
            out.writeUTF(name);
            out.writeUTF(obj.name());
        }

        @Override
        public Enum read(ObjectDataInput in) throws IOException {
            String clazzName = in.readUTF();
            Class clazz;
            try {
                clazz = ClassLoaderUtil.loadClass(in.getClassLoader(), clazzName);
            } catch (ClassNotFoundException e) {
                throw new HazelcastSerializationException("Failed to deserialize enum: " + clazzName, e);
            }

            String name = in.readUTF();
            return Enum.valueOf(clazz, name);
        }
    }

    private abstract static class SingletonSerializer<T> implements StreamSerializer<T> {

        @Override
        public void destroy() {
        }
    }

    private JavaDefaultSerializers() {
    }

}
