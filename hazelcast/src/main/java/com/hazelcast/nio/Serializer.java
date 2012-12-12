/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.core.PartitionAware;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.core.ManagedContext;

import java.io.IOException;

public final class Serializer extends AbstractSerializer {

    private static final byte SERIALIZER_TYPE_DATA = 0;

    public Serializer() {
        super(new DataSerializer(), new DefaultSerializer());
    }

    public static Object newInstance(final Class klass) throws Exception {
        return AbstractSerializer.newInstance(klass);
    }

    public static Class<?> loadClass(final String className) throws ClassNotFoundException {
        return AbstractSerializer.loadClass(className);
    }

    public static Class<?> loadClass(final ClassLoader classLoader, final String className) throws ClassNotFoundException {
        return AbstractSerializer.loadClass(classLoader, className);
    }

    public Data writeObject(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Data) {
            return (Data) obj;
        }
        byte[] bytes = toByteArray(obj);
        if (bytes == null) {
            return null;
        } else {
            Data data = new Data(bytes);
            if (obj instanceof PartitionAware) {
                Data partitionKey = writeObject(((PartitionAware) obj).getPartitionKey());
                int partitionHash = (partitionKey == null) ? -1 : partitionKey.getPartitionHash();
                data.setPartitionHash(partitionHash);
            }
            return data;
        }
    }

    public Object readObject(final Data data) {
        if ((data == null) || (data.buffer == null) || (data.buffer.length == 0)) {
            return null;
        }
        byte[] byteArray = data.buffer;
        Object obj = toObject(byteArray);
        final ManagedContext managedContext = ThreadContext.get().getCurrentManagedContext();
        if (managedContext != null) {
            obj = managedContext.initialize(obj);
        }
        return obj;
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

        protected Class loadClass(final String className) throws ClassNotFoundException {
            return AbstractSerializer.loadClass(className);
        }

        protected String toClassName(final Object obj) throws ClassNotFoundException {
            return obj.getClass().getName();
        }

        public final DataSerializable read(final FastByteArrayInputStream bbis) throws Exception {
            final String className = bbis.readUTF();
            try {
                final DataSerializable ds = (DataSerializable) newInstance(loadClass(className));
                ds.readData(bbis);
                return ds;
            } catch (final Exception e) {
                throw new IOException("Problem reading DataSerializable class : " + className + ", exception: " + e);
            }
        }

        public final void write(final FastByteArrayOutputStream bbos, final DataSerializable obj) throws Exception {
            bbos.writeUTF(toClassName(obj));
            obj.writeData(bbos);
        }
    }
}


