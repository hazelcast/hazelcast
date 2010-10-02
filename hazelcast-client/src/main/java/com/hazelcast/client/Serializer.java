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

import com.hazelcast.nio.*;

public final class Serializer {

    private static class ClientSerializer extends AbstractSerializer {

        private static final TypeSerializer[] serializers =
            sort(new TypeSerializer[]{
                new ClientDataSerializer(),
                new ByteArraySerializer(),
                new LongSerializer(),
                new IntegerSerializer(),
                new StringSerializer(),
                new ClassSerializer(),
                new DateSerializer(),
                new BigIntegerSerializer(),
                new Externalizer(),
                new ObjectSerializer()});

        static final ClientSerializer instance = new ClientSerializer();
        
        public ClientSerializer() {
            super(serializers);
        }

        static class ClientDataSerializer extends DataSerializer {

            @Override
            protected Class classForName(final String className) throws ClassNotFoundException {
                String name = className;
                if (name.equals("com.hazelcast.impl.Keys")) {
                    name = "com.hazelcast.client.impl.CollectionWrapper";
                } else if (className.equals("com.hazelcast.impl.CMap$Values")) {
                    name = "com.hazelcast.client.impl.Values";
                }
                return super.classForName(name);
            }

            @Override
            protected String toClassName(final Class clazz) throws ClassNotFoundException {
                final String className = super.toClassName(clazz);
                if (!className.startsWith("com.hazelcast.client")) {
                    return className;
                }
                return "com.hazelcast" + className.substring("com.hazelcast.client".length());
            }
        }
        
        byte[] toByte(final Object object) {
            try {
                final FastByteArrayOutputStream dos = new FastByteArrayOutputStream();
                toByte(dos, object);
                dos.close();
                return dos.toByteArray();
            } catch (Throwable e){
                throw new RuntimeException(e);
            }
        }

        Object toObject(final byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            try {
                final FastByteArrayInputStream dis = new FastByteArrayInputStream(bytes);
                dis.set(bytes, bytes.length);
                final Object object = toObject(dis);
                dis.close();
                return object;
            } catch (final Throwable e){
                throw new RuntimeException(e);
            }
        }

    }

    public static byte[] toByte(final Object object) {
        return ClientSerializer.instance.toByte(object);
    }

    public static Object toObject(final byte[] bytes) {
        return ClientSerializer.instance.toObject(bytes);
    }

}
