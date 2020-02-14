package com.hazelcast.serialization;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

class StreamObject {

    static Object object() {
        return new Person("Joe", "Doe", 28, 66.92f);
    }

    static InternalSerializationService serializationService() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder()
                .build();
        ((AbstractSerializationService) serializationService).register(Person.class, new PersonSerializer());
        return serializationService;
    }

    static class Person {

        private final String firstName;
        private final String lastName;
        private final int age;
        private final float height;

        public Person(String firstName, String lastName, int age, float height) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
            this.height = height;
        }
    }

    static class PersonSerializer implements StreamSerializer<Person> {

        private static final int TYPE_ID = 1;

        @Override
        public int getTypeId() {
            return TYPE_ID;
        }

        @Override
        public void write(ObjectDataOutput out, Person object) throws IOException {
            out.writeUTF(object.firstName);
            out.writeUTF(object.lastName);
            out.writeInt(object.age);
            out.writeFloat(object.height);
        }

        @Override
        public Person read(ObjectDataInput in) throws IOException {
            return new Person(in.readUTF(), in.readUTF(), in.readInt(), in.readFloat());
        }

        @Override
        public void destroy() {
        }
    }
}
