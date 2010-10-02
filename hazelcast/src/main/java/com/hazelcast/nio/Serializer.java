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

import java.util.logging.Level;

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

public final class Serializer extends AbstractSerializer {

    private static final ILogger logger = Logger.getLogger(Serializer.class.getName());

    private static int OUTPUT_STREAM_BUFFER_SIZE = 100 << 10;

    private static final TypeSerializer[] serializers =
        sort(new TypeSerializer[]{
            new DataSerializer(),
            new ByteArraySerializer(),
            new LongSerializer(),
            new IntegerSerializer(),
            new StringSerializer(),
            new ClassSerializer(),
            new DateSerializer(),
            new BigIntegerSerializer(),
            new Externalizer(),
            new ObjectSerializer()
        });


    final FastByteArrayOutputStream bbos;

    final FastByteArrayInputStream bbis;

    public Serializer() {
        super(serializers);
        this.bbos = new FastByteArrayOutputStream(OUTPUT_STREAM_BUFFER_SIZE);
        this.bbis = new FastByteArrayInputStream(new byte[10]);
    }

    public static Object newInstance(final Class klass) throws Exception {
        return AbstractSerializer.newInstance(klass);
    }

    public static Class<?> classForName(final String className) throws ClassNotFoundException {
        return AbstractSerializer.classForName(className);
    }

    public static Class<?> classForName(final ClassLoader classLoader, final String className) throws ClassNotFoundException {
        return AbstractSerializer.classForName(classLoader, className);
    }

    public Data writeObject(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Data) {
            return (Data) obj;
        }
        try {
            this.bbos.reset();
            toByte(this.bbos, obj);
            final Data data = new Data(this.bbos.toByteArray());
            if (this.bbos.size() > OUTPUT_STREAM_BUFFER_SIZE) {
                this.bbos.set(new byte[OUTPUT_STREAM_BUFFER_SIZE]);
            }
            return data;
        } catch (final Throwable e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public Object readObject(final Data data) {
        if ((data == null) || (data.buffer == null) || (data.buffer.length == 0)) {
            return null;
        }
        this.bbis.set(data.buffer, data.buffer.length);
        final Object obj = toObject(this.bbis);
        if (obj instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) obj).setHazelcastInstance(ThreadContext.get().getCurrentFactory());
        }
        return obj;
    }

}
