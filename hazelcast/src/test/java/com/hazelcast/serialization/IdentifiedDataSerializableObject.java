/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.serialization;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

class IdentifiedDataSerializableObject {

    static IdentifiedDataSerializable object() {
        return new Person("Joe", "Doe", 28, 66.92f);
    }

    static InternalSerializationService serializationService() {
        return new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(IdentifiedDataSerializableFactory.FACTORY_ID, new IdentifiedDataSerializableFactory())
                .build();
    }

    public static class Person implements IdentifiedDataSerializable {

        private static final int CLASS_ID = 2;

        private String firstName;
        private String lastName;
        private int age;
        private float height;

        @SuppressWarnings("unused")
        Person() {
        }

        Person(String firstName, String lastName, int age, float height) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
            this.height = height;
        }

        @Override
        public int getFactoryId() {
            return IdentifiedDataSerializableFactory.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(firstName);
            out.writeUTF(lastName);
            out.writeInt(age);
            out.writeFloat(height);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.firstName = in.readUTF();
            this.lastName = in.readUTF();
            this.age = in.readInt();
            this.height = in.readFloat();
        }
    }

    private static class IdentifiedDataSerializableFactory implements DataSerializableFactory {

        public static final int FACTORY_ID = 1;

        @Override
        public IdentifiedDataSerializable create(int id) {
            if (id == Person.CLASS_ID) {
                return new Person();
            }
            throw new IllegalArgumentException("unknown id");
        }
    }
}
