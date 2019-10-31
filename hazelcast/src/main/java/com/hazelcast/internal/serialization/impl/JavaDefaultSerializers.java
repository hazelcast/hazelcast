/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.ClassNameFilter;
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

import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVASCRIPT_JSON_SERIALIZATION_TYPE;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_BIG_DECIMAL;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_BIG_INTEGER;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_CLASS;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_DATE;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_EXTERNALIZABLE;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.JAVA_DEFAULT_TYPE_SERIALIZABLE;
import static com.hazelcast.internal.nio.IOUtil.newObjectInputStream;
import static java.lang.Math.max;


public final class JavaDefaultSerializers {

    public static final class JavaSerializer extends SingletonSerializer<Object> {

        private final boolean shared;
        private final boolean gzipEnabled;
        private final ClassNameFilter classFilter;

        public JavaSerializer(boolean shared, boolean gzipEnabled, ClassNameFilter classFilter) {
            this.shared = shared;
            this.gzipEnabled = gzipEnabled;
            this.classFilter = classFilter;
        }

        @Override
        public int getTypeId() {
            return JAVA_DEFAULT_TYPE_SERIALIZABLE;
        }

        @Override
        public Object read(final ObjectDataInput in) throws IOException {
            if (gzipEnabled) {
                return readGzipped(((InputStream) in), in.getClassLoader());
            }
            return read(((InputStream) in), in.getClassLoader());
        }

        private Object read(InputStream in, ClassLoader classLoader) throws IOException {
            try {
                ObjectInputStream objectInputStream = newObjectInputStream(classLoader, classFilter, in);
                if (shared) {
                    return objectInputStream.readObject();
                }
                return objectInputStream.readUnshared();
            } catch (ClassNotFoundException e) {
                throw new HazelcastSerializationException(e);
            }
        }

        private Object readGzipped(InputStream in, ClassLoader classLoader) throws IOException {
            ExtendedGZipInputStream gzip = new ExtendedGZipInputStream(in);
            try {
                Object obj = read(gzip, classLoader);
                gzip.pushBackUnconsumedBytes();
                return obj;
            } finally {
                gzip.closeInflater();
            }
        }

        @SuppressFBWarnings("OS_OPEN_STREAM")
        @Override
        public void write(final ObjectDataOutput out, final Object obj) throws IOException {
            if (gzipEnabled) {
                writeGzipped(((OutputStream) out), obj);
            } else {
                write((OutputStream) out, obj);
            }
        }

        private void write(OutputStream out, Object obj) throws IOException {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
            if (shared) {
                objectOutputStream.writeObject(obj);
            } else {
                objectOutputStream.writeUnshared(obj);
            }
            // Force flush if not yet written due to internal behavior if pos < 1024
            objectOutputStream.flush();
        }

        private void writeGzipped(OutputStream out, Object obj) throws IOException {
            ExtendedGZipOutputStream gzip = new ExtendedGZipOutputStream(out);
            try {
                write(gzip, obj);
                gzip.finish();
            } finally {
                gzip.closeDeflater();
            }
        }
    }

    public static final class ExternalizableSerializer extends SingletonSerializer<Externalizable> {

        private final boolean gzipEnabled;
        private final ClassNameFilter classFilter;

        public ExternalizableSerializer(boolean gzipEnabled, ClassNameFilter classFilter) {
            this.gzipEnabled = gzipEnabled;
            this.classFilter = classFilter;
        }

        @Override
        public int getTypeId() {
            return JAVA_DEFAULT_TYPE_EXTERNALIZABLE;
        }

        @Override
        public Externalizable read(final ObjectDataInput in) throws IOException {
            String className = in.readUTF();
            try {
                if (gzipEnabled) {
                    return readGzipped(((InputStream) in), className, in.getClassLoader());
                }
                return read((InputStream) in, className, in.getClassLoader());
            } catch (Exception e) {
                throw new HazelcastSerializationException("Problem while reading Externalizable class: "
                        + className + ", exception: " + e);
            }
        }

        private Externalizable readGzipped(InputStream in, String className, ClassLoader classLoader) throws Exception {
            ExtendedGZipInputStream gzip = new ExtendedGZipInputStream(in);
            try {
                Externalizable external = read(gzip, className, classLoader);
                gzip.pushBackUnconsumedBytes();
                return external;
            } finally {
                gzip.closeInflater();
            }
        }

        private Externalizable read(InputStream in, String className, ClassLoader classLoader) throws Exception {
            if (classFilter != null) {
                classFilter.filter(className);
            }
            Externalizable ds = ClassLoaderUtil.newInstance(classLoader, className);
            ObjectInputStream objectInputStream = newObjectInputStream(classLoader, classFilter, in);
            ds.readExternal(objectInputStream);
            return ds;
        }

        @Override
        public void write(final ObjectDataOutput out, final Externalizable obj) throws IOException {
            out.writeUTF(obj.getClass().getName());

            if (gzipEnabled) {
                writeGzipped(((OutputStream) out), obj);
            } else {
                write((OutputStream) out, obj);
            }
        }

        private void writeGzipped(OutputStream out, Externalizable obj) throws IOException {
            ExtendedGZipOutputStream gzip = new ExtendedGZipOutputStream(out);
            try {
                write(gzip, obj);
                gzip.finish();
            } finally {
                gzip.closeDeflater();
            }
        }

        private void write(OutputStream outputStream, Externalizable obj) throws IOException {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
            obj.writeExternal(objectOutputStream);
            // Force flush if not yet written due to internal behavior if pos < 1024
            objectOutputStream.flush();
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

    public static final class HazelcastJsonValueSerializer extends SingletonSerializer<HazelcastJsonValue> {

        @Override
        public void write(ObjectDataOutput out, HazelcastJsonValue object) throws IOException {
            out.writeUTF(object.toString());
        }

        @Override
        public HazelcastJsonValue read(ObjectDataInput in) throws IOException {
            return new HazelcastJsonValue(in.readUTF());
        }

        @Override
        public int getTypeId() {
            return JAVASCRIPT_JSON_SERIALIZATION_TYPE;
        }
    }

    private abstract static class SingletonSerializer<T> implements StreamSerializer<T> {

        @Override
        public void destroy() {
        }
    }

    private JavaDefaultSerializers() {
    }

    /**
     * Gzip input stream consumes more bytes in the stream than it actually needs.
     * This class enables us to access internal inflater that keeps consumed buffer.
     * `pushBackUnconsumedBytes` method adjust the position so that, next data in the stream can be read correctly
     */
    private static final class ExtendedGZipInputStream extends GZIPInputStream {

        private static final int GZIP_TRAILER_SIZE = 8;

        private ExtendedGZipInputStream(InputStream in) throws IOException {
            super(in);
            assert in instanceof BufferObjectDataInput : "Unexpected input: " + in;
        }

        private void pushBackUnconsumedBytes() {
            int remaining = inf.getRemaining();
            BufferObjectDataInput bufferedInput = (BufferObjectDataInput) in;
            int position = bufferedInput.position();
            int rewindBack = max(0, remaining - ExtendedGZipInputStream.GZIP_TRAILER_SIZE);
            int newPosition = position - rewindBack;
            bufferedInput.position(newPosition);
        }

        /**
         * Only close inflater, we don't want to close underlying InputStream
         */
        private void closeInflater() {
            //GZIPInputStream allocates native resources
            //via inf.end() these resources are released.
            inf.end();
        }
    }

    private static final class ExtendedGZipOutputStream extends GZIPOutputStream {

        private ExtendedGZipOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        /**
         * Only close deflater, we don't want to close underlying OutputStream
         */
        private void closeDeflater() {
            //GZIPOutputStream allocates native resources
            //via def.end() these resources are released.
            def.end();
        }
    }

}
