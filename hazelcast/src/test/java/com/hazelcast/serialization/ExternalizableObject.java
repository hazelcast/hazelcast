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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ExternalizableObject {

    static Externalizable object() {
        return new Person("Joe", "Doe", 28, 66.92f);
    }

    static InternalSerializationService serializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    static class Person implements Externalizable {

        private String firstName;
        private String lastName;
        private int age;
        private float height;

        @SuppressWarnings({"unused"})
        Person() {
        }

        Person(String firstName, String lastName, int age, float height) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
            this.height = height;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(firstName);
            out.writeUTF(lastName);
            out.writeInt(age);
            out.writeFloat(height);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException {
            this.firstName = in.readUTF();
            this.lastName = in.readUTF();
            this.age = in.readInt();
            this.height = in.readFloat();
        }
    }
}
