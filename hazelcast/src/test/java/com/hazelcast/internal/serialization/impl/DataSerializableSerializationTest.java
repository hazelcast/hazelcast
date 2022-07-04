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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})

public class DataSerializableSerializationTest extends HazelcastTestSupport {

    private SerializationService ss = new DefaultSerializationServiceBuilder().setVersion(InternalSerializationService.VERSION_1)
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
        SerializationService ss = new DefaultSerializationServiceBuilder().addDataSerializableFactory(1, new IDSPersonFactory())
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
                .addDataSerializableFactory(1, new IDSPersonFactoryVersioned()).setVersion(InternalSerializationService.VERSION_1)
                .build();

        IDSPerson deserialized = ss.toObject(ss.toData(person));

        assertEquals(person.getClass(), deserialized.getClass());
        assertEquals(person.name, deserialized.name);
    }

    @Test
    public void testClarifiedExceptionsForUnsupportedClassTypes() {
        class LocalClass implements DataSerializable {
            @Override
            public void writeData(ObjectDataOutput out) {
            }

            @Override
            public void readData(ObjectDataInput in) {
            }
        }

        DataSerializable anonymousInstance = new DataSerializable() {
            @Override
            public void writeData(ObjectDataOutput out) {
            }

            @Override
            public void readData(ObjectDataInput in) {
            }
        };

        DataSerializable[] throwingInstances = {new LocalClass(), anonymousInstance, new NonStaticMemberClass()};

        for (DataSerializable throwingInstance : throwingInstances) {
            try {
                ss.toObject(ss.toData(throwingInstance));
            } catch (HazelcastSerializationException e) {
                assertInstanceOf(NoSuchMethodException.class, e.getCause());
                assertContains(e.getCause().getMessage(), "can't conform to DataSerializable");
                assertInstanceOf(NoSuchMethodException.class, e.getCause().getCause());
                continue;
            }
            fail("deserialization of '" + throwingInstance.getClass() + "' is expected to fail");
        }

        for (DataSerializable throwingInstance : throwingInstances) {
            try {
                ss.toObject(ss.toData(throwingInstance), throwingInstance.getClass());
            } catch (HazelcastSerializationException e) {
                assertInstanceOf(InstantiationException.class, e.getCause());
                assertContains(e.getCause().getMessage(), "can't conform to DataSerializable");
                assertInstanceOf(InstantiationException.class, e.getCause().getCause());
                continue;
            }
            fail("deserialization of '" + throwingInstance.getClass() + "' is expected to fail");
        }
    }

    public class NonStaticMemberClass implements DataSerializable {
        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
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
            out.writeString(name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readString();
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
            out.writeString(name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readString();
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
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
        public IdentifiedDataSerializable create(int typeId, Version version, Version wanProtocolVersion) {
            throw new RuntimeException("Should not be used outside of the versioned context");
        }
    }

}
