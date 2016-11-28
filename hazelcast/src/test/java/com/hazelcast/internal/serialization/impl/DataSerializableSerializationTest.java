package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.Version;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})

public class DataSerializableSerializationTest {

    private SerializationService ss = new DefaultSerializationServiceBuilder()
            .setVersion(InternalSerializationService.VERSION_1)
            .build();

    @Test
    public void serializeAndDeserialize_DataSerializable() {
        DSPerson person = new DSPerson("James Bond");

        DSPerson deserialized = ss.toObject(ss.toData(person));

        assertEquals(person.getClass(), deserialized.getClass());
        assertEquals(person.name, deserialized.name);
    }

    @Test
    public void serializeAndDeserialize_IdentifiedDataSerializable() {
        IDSPerson person = new IDSPerson("James Bond");
        SerializationService ss = new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(1, new IDSPersonFactory())
                .setVersion(InternalSerializationService.VERSION_1)
                .build();


        IDSPerson deserialized = ss.toObject(ss.toData(person));

        assertEquals(person.getClass(), deserialized.getClass());
        assertEquals(person.name, deserialized.name);
    }

    @Test
    public void serializeAndDeserialize_IdentifiedDataSerializable_versionedFactory() {
        IDSPerson person = new IDSPerson("James Bond");
        SerializationService ss = new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(1, new IDSPersonFactoryVersioned())
                .setVersion(InternalSerializationService.VERSION_1)
                .build();

        IDSPerson deserialized = ss.toObject(ss.toData(person));

        assertEquals(person.getClass(), deserialized.getClass());
        assertEquals(person.name, deserialized.name);
    }


    private static class DSPerson implements DataSerializable {

        private String name;

        DSPerson() {
        }

        DSPerson(String name) {
            this.name = name;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readUTF();
        }
    }

    private static class IDSPerson implements IdentifiedDataSerializable {

        private String name;

        IDSPerson() {
        }

        IDSPerson(String name) {
            this.name = name;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readUTF();
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getId() {
            return 2;
        }
    }

    private static class IDSPersonFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            return new IDSPerson();
        }
    }

    private static class IDSPersonFactoryVersioned implements VersionedDataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            return new IDSPerson();
        }

        @Override
        public IdentifiedDataSerializable create(int typeId, Version version) {
            throw new RuntimeException("Should not be used outside of the versioned context");
        }
    }

}
