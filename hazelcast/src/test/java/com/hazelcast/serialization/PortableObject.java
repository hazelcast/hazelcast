package com.hazelcast.serialization;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

class PortableObject {

    static Portable object() {
        return new Person("Joe", "Doe", 28, 66.92f);
    }

    static InternalSerializationService serializationService() {
        return new DefaultSerializationServiceBuilder()
                .addPortableFactory(PortableFactory.FACTORY_ID, new PortableFactory())
                .build();
    }

    public static class Person implements Portable {

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
            return PortableFactory.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("firstName", firstName);
            writer.writeUTF("lastName", lastName);
            writer.writeInt("age", age);
            writer.writeFloat("height", height);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.firstName = reader.readUTF("firstName");
            this.lastName = reader.readUTF("firstName");
            this.age = reader.readInt("age");
            this.height = reader.readFloat("height");
        }
    }

    static class PortableFactory implements com.hazelcast.nio.serialization.PortableFactory {

        public static final int FACTORY_ID = 2;

        @Override
        public Portable create(int id) {
            if (id == Person.CLASS_ID) {
                return new Person();
            }
            throw new IllegalArgumentException("unknown id");
        }
    }
}
