package com.hazelcast.serialization;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import java.io.Serializable;

class SerializableObject {

    static Serializable object() {
        return new Person("Joe", "Doe", 28, 66.92f);
    }

    static InternalSerializationService serializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    static class Person implements Serializable {

        private final String firstName;
        private final String lastName;
        private final int age;
        private final float height;

        private Person(String firstName, String lastName, int age, float height) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
            this.height = height;
        }
    }
}
