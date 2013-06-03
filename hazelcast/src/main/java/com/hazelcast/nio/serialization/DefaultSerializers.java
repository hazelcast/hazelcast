/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.hazelcast.nio.ClassLoaderUtil.loadClass;
import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.nio.IOUtil.newObjectInputStream;
import static com.hazelcast.nio.serialization.SerializationConstants.*;

/**
 * @mdogan 6/18/12
 */
public class DefaultSerializers {

    public static final class BigIntegerSerializer extends SingletonTypeSerializer<BigInteger> {

        public int getTypeId() {
            return DEFAULT_TYPE_BIG_INTEGER;
        }

        public BigInteger read(final ObjectDataInput in) throws IOException {
            final byte[] bytes = new byte[in.readInt()];
            in.readFully(bytes);
            return new BigInteger(bytes);
        }

        public void write(final ObjectDataOutput out, final BigInteger obj) throws IOException {
            final byte[] bytes = obj.toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    public static final class BigDecimalSerializer extends SingletonTypeSerializer<BigDecimal> {

        final BigIntegerSerializer bigIntegerSerializer = new BigIntegerSerializer();

        public int getTypeId() {
            return DEFAULT_TYPE_BIG_DECIMAL;
        }

        public BigDecimal read(final ObjectDataInput in) throws IOException {
            BigInteger bigInt = bigIntegerSerializer.read(in);
            int scale = in.readInt();
            return new BigDecimal(bigInt, scale);
        }

        public void write(final ObjectDataOutput out, final BigDecimal obj) throws IOException {
            BigInteger bigInt = obj.unscaledValue();
            int scale = obj.scale();
            bigIntegerSerializer.write(out, bigInt);
            out.writeInt(scale);
        }
    }

    public static final class DateSerializer extends SingletonTypeSerializer<Date> {

        public int getTypeId() {
            return DEFAULT_TYPE_DATE;
        }

        public Date read(final ObjectDataInput in) throws IOException {
            return new Date(in.readLong());
        }

        public void write(final ObjectDataOutput out, final Date obj) throws IOException {
            out.writeLong(obj.getTime());
        }
    }

    public static final class ClassSerializer extends SingletonTypeSerializer<Class> {

        public int getTypeId() {
            return DEFAULT_TYPE_CLASS;
        }

        public Class read(final ObjectDataInput in) throws IOException {
            try {
                return loadClass(in.readUTF());
            } catch (ClassNotFoundException e) {
                throw new HazelcastSerializationException(e);
            }
        }

        public void write(final ObjectDataOutput out, final Class obj) throws IOException {
            out.writeUTF(obj.getName());
        }
    }

    public static final class Externalizer extends SingletonTypeSerializer<Externalizable> {

        public int getTypeId() {
            return DEFAULT_TYPE_EXTERNALIZABLE;
        }

        public Externalizable read(final ObjectDataInput in) throws IOException {
            final String className = in.readUTF();
            try {
                final Externalizable ds = (Externalizable) newInstance(loadClass(className));
                ds.readExternal(newObjectInputStream((InputStream) in));
                return ds;
            } catch (final Exception e) {
                throw new HazelcastSerializationException("Problem while reading Externalizable class : "
                                                          + className + ", exception: " + e);
            }
        }

        public void write(final ObjectDataOutput out, final Externalizable obj) throws IOException {
            out.writeUTF(obj.getClass().getName());
            final ObjectOutputStream objectOutputStream = new ObjectOutputStream((OutputStream) out);
            obj.writeExternal(objectOutputStream);
        }
    }

    public static final class ObjectSerializer extends SingletonTypeSerializer<Object> {

        private static final boolean shared = GroupProperties.SERIALIZER_SHARED.getBoolean();
        private static final boolean gzipEnabled = GroupProperties.SERIALIZER_GZIP_ENABLED.getBoolean();

        public int getTypeId() {
            return DEFAULT_TYPE_OBJECT;
        }

        public Object read(final ObjectDataInput in) throws IOException {
            final ObjectInputStream objectInputStream;
            final InputStream inputStream = (InputStream) in;
            if (gzipEnabled) {
                objectInputStream = newObjectInputStream(new BufferedInputStream(new GZIPInputStream(inputStream)));
            } else {
                objectInputStream = newObjectInputStream(inputStream);
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

        public void write(final ObjectDataOutput out, final Object obj) throws IOException {
            final ObjectOutputStream objectOutputStream;
            final OutputStream outputStream = (OutputStream) out;
            if (gzipEnabled) {
                objectOutputStream = new ObjectOutputStream(new BufferedOutputStream(
                        new GZIPOutputStream(outputStream)));
            } else {
                objectOutputStream = new ObjectOutputStream(outputStream);
            }
            try {
                if (shared) {
                    objectOutputStream.writeObject(obj);
                } else {
                    objectOutputStream.writeUnshared(obj);
                }
            } finally {
                IOUtil.closeResource(objectOutputStream);
            }
        }
    }

    private abstract static class SingletonTypeSerializer<T> implements TypeSerializer<T> {

        public void destroy() {
        }
    }

    private DefaultSerializers() {}

}
