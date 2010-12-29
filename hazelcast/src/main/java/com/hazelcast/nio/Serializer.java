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

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.impl.ThreadContext;

public final class Serializer extends AbstractSerializer {

    private static final AbstractSerializer.TypeSerializer[] serializers =
            sort(new AbstractSerializer.TypeSerializer[]{
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

    public Serializer() {
        super(serializers);
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
        if (obj instanceof Data) {
            return (Data) obj;
        }
        byte[] bytes = toByteArray(obj);
        if (bytes == null) {
            return null;
        } else {
            return new Data(bytes);
        }
    }

    public Object readObject(final Data data) {
        if ((data == null) || (data.buffer == null) || (data.buffer.length == 0)) {
            return null;
        }
        byte[] byteArray = data.buffer;
        final Object obj = toObject(byteArray);
        if (obj instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) obj).setHazelcastInstance(ThreadContext.get().getCurrentFactory());
        }
        return obj;
    }

    public byte[] toByteArray(Object obj) {
        return super.toByteArray(obj);
    }
}
