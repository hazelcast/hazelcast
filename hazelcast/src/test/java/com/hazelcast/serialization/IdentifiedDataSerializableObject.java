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
        public Person() {
        }

        public Person(String firstName, String lastName, int age, float height) {
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
