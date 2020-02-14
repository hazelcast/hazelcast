package com.hazelcast.serialization;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

class DataSerializableObject {

    static DataSerializable object() {
        return new Person("Joe", "Doe", 28, 66.92f);
    }

    static InternalSerializationService serializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    public static class Person implements DataSerializable {

        private String firstName;
        private String lastName;
        private int age;
        private float height;

        @SuppressWarnings("unused")
        public Person() {
        }

        public Person(String firstName, String lastName, int age, float height) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
            this.height = height;
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
}
