/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public final class TestJavaSerializationUtils {

    private TestJavaSerializationUtils() {
    }

    public static <T extends Serializable> T serializeAndDeserialize(T object) throws IOException, ClassNotFoundException {
        byte[] array = serialize(object);
        return deserialize(array);
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserialize(byte[] array) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(array);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (T) ois.readObject();
    }

    public static byte[] serialize(Serializable entry) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(entry);
        return bos.toByteArray();
    }

    public static Serializable newSerializableObject(int id) {
        return new SerializableObject(id);
    }

    private static final class SerializableObject implements Serializable {

        private final int id;

        SerializableObject(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SerializableObject that = (SerializableObject) o;
            return (id == that.id);
        }

        @Override
        public int hashCode() {
            return id;
        }
    }
}
