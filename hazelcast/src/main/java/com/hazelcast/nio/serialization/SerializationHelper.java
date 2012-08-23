/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.FastDataInputStream;
import com.hazelcast.nio.FastDataOutputStream;
import com.hazelcast.nio.HazelcastSerializationException;

import java.io.NotSerializableException;
import java.util.logging.Level;

public final class SerializationHelper {

    private static final int OUTPUT_STREAM_BUFFER_SIZE = 100 << 10;

    private static final ILogger logger = Logger.getLogger(SerializationHelper.class.getName());

    private final ThreadContext context;
    private final SerializerRegistry defaultRegistry;
    private final int maxDepth = 5;
    private final SerializationBuffer[] bufferPool = new SerializationBuffer[maxDepth];
    private int depth = 0;

    public SerializationHelper(SerializerRegistry registry) {
        this(registry, null);
    }

    public SerializationHelper(ThreadContext ctx) {
        this(null, ctx);
    }

    private SerializationHelper(SerializerRegistry registry, ThreadContext ctx) {
        this.context = ctx;
        this.defaultRegistry = registry;
    }

    public byte[] toByteArray(Object obj) {
        if (obj == null) {
            return null;
        }
        incrementDepth();
        try {
            final SerializationBuffer buffer = getBuffer();
            return buffer.toByteArray(obj);
        } catch (Throwable e) {
            logger.log(Level.SEVERE, obj + " failed to serialize: " + e.getMessage(), e);
            if (e instanceof HazelcastSerializationException) {
                throw (HazelcastSerializationException) e;
            }
            throw new HazelcastSerializationException(e);
        } finally {
            decrementDepth();
        }
    }

    public Object toObject(byte[] byteArray) {
        if (byteArray == null || byteArray.length == 0) {
            return null;
        }
        incrementDepth();
        try {
            final SerializationBuffer buffer = getBuffer();
            return buffer.toObject(byteArray);
        } finally {
            decrementDepth();
        }
    }

    public Data writeObject(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Data) {
            return (Data) obj;
        }
        final byte[] bytes = toByteArray(obj);
        if (bytes == null) {
            return null;
        } else {
            final Data data = new Data(bytes);
            if (obj instanceof PartitionAware) {
                final Data partitionKey = writeObject(((PartitionAware) obj).getPartitionKey());
                final int partitionHash = (partitionKey == null) ? -1 : partitionKey.getPartitionHash();
                data.setPartitionHash(partitionHash);
            }
            return data;
        }
    }

    public Object readObject(final Data data) {
        if ((data == null) || (data.buffer == null) || (data.buffer.length == 0)) {
            return null;
        }
        final Object obj = toObject(data.buffer);
        final ManagedContext managedContext = context != null
                ? context.getCurrentManagedContext() : null;
        if (managedContext != null) {
            managedContext.initialize(obj);
        }
        return obj;
    }

    private SerializerRegistry getSerializerRegistry() {
        final SerializerRegistry serializerRegistry = context != null
                ? context.getCurrentSerializerRegistry() : defaultRegistry;
        if (serializerRegistry == null) {
            throw new HazelcastSerializationException("SerializerRegistry could not be found!");
        }
        return serializerRegistry;
    }

    private SerializationBuffer getBuffer() {
        final int index = depth - 1;
        SerializationBuffer buffer = bufferPool[index];
        if (buffer == null) {
            buffer = new SerializationBuffer();
            bufferPool[index] = buffer;
        }
        return buffer;
    }

    private void incrementDepth() {
        if (depth == maxDepth) {
            throw new HazelcastSerializationException("Inner serialization count exceeded! max: " + maxDepth);
        }
        depth++;
    }

    private void decrementDepth() {
        depth--;
    }

    public void destroy() {
        synchronized (bufferPool) {
            for (int i = 0; i < bufferPool.length; i++) {
                SerializationBuffer buffer = bufferPool[i];
                if (buffer != null) {
                    buffer.destroy();
                    bufferPool[i] = null;
                }
            }
        }
    }

    private class SerializationBuffer {

        private final FastDataOutputStream out;
        private final FastDataInputStream in;

        private SerializationBuffer() {
            this.in = new FastDataInputStream(new byte[0]);
            this.out = new FastDataOutputStream(OUTPUT_STREAM_BUFFER_SIZE);
        }

        private byte[] toByteArray(final Object object) {
            if (object == null) {
                return null;
            }
            out.reset();
            try {
                final TypeSerializer serializer = getSerializerRegistry().serializerFor(object.getClass());
                if (serializer == null) {
                    throw new NotSerializableException("There is no suitable serializer for " + object.getClass());
                }
                out.writeInt(serializer.getTypeId());
                serializer.write(out, object);
                out.flush();
                return out.toByteArray();
            } catch (Throwable e) {
                if (e instanceof HazelcastSerializationException) {
                    throw (HazelcastSerializationException) e;
                }
                throw new HazelcastSerializationException(e);
            } finally {
                if (out.size() > OUTPUT_STREAM_BUFFER_SIZE) {
                    out.set(new byte[OUTPUT_STREAM_BUFFER_SIZE]);
                }
            }
        }

        private Object toObject(byte[] byteArray) {
            int typeId = -1;
            in.set(byteArray, byteArray.length);
            try {
                typeId = in.readInt();
                final TypeSerializer serializer = getSerializerRegistry().serializerFor(typeId);
                if (serializer == null) {
                    throw new IllegalArgumentException("There is no suitable de-serializer for type " + typeId);
                }
                return serializer.read(in);
            } catch (Throwable e) {
                if (e instanceof HazelcastSerializationException) {
                    throw (HazelcastSerializationException) e;
                }
                throw new HazelcastSerializationException("Problem while serializing type " + typeId, e);
            } finally {
                in.set(null, 0);
            }
        }

        private void destroy() {
            out.set(null);
            in.set(null, 0);
        }
    }
}
