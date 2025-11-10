/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.strategy;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AttributePartitioningStrategyTest {
    private final AttributePartitioningStrategy strategy = new AttributePartitioningStrategy("id", "name");

    @Test
    public void test_partitionAware() {
        checkFailure(new PartitionAwarePojo());
    }

    @Test
    public void test_portablePojo() {
        check(new PortablePojo(1L, "test"));
        checkFailure(new PortablePojo(1L, null));
        checkFailure(new PortablePojo(null, "test"));
    }

    @Test
    public void test_nonSerializablePojo() {
        checkFailure(new NonSerializablePojo());
    }

    @Test
    public void test_json() {
        final String json = "{"
                + "\"name\":\"test\","
                + "\"id\":1"
                + "}";
        check(new HazelcastJsonValue(json));
        checkFailure(new HazelcastJsonValue("{}"));
        checkFailure(new HazelcastJsonValue("{\"name\":\"test\"}"));
        checkFailure(new HazelcastJsonValue("{\"id\": 1}"));
    }

    @Test
    public void test_pojo() {
        check(new RegularPojo(1L, "test"));
        check(new IDSPojo(1L, "test"));
        checkFailure(new RegularPojo(1L, null));
        checkFailure(new RegularPojo(null, "test"));
        checkFailure(new IDSPojo(1L, null));
        checkFailure(new IDSPojo(null, "test"));
    }

    @Test
    public void test_compact() {
        final GenericRecord record = GenericRecordBuilder.compact("testType")
                .setNullableInt64("id", 1L)
                .setString("name", "test")
                .build();
        final GenericRecord idLess = record.newBuilder()
                .setNullableInt64("id", null)
                .setString("name", "test")
                .build();
        final GenericRecord nameLess = record.newBuilder()
                .setNullableInt64("id", 1L)
                .setString("name", null)
                .build();

        check(record);
        checkFailure(idLess);
        checkFailure(nameLess);
    }

    @Test
    public void test_portable() {
        final ClassDefinition classDefinition = new ClassDefinitionBuilder(1, 1, 1)
                .addStringField("name")
                .addLongField("id")
                .build();
        final GenericRecord record = GenericRecordBuilder.portable(classDefinition)
                .setInt64("id", 1L)
                .setString("name", "test")
                .build();
        final GenericRecord nameLess = record.newBuilder()
                .setInt64("id", 1L)
                .setString("name", null)
                .build();

        check(record);
        checkFailure(nameLess);
    }

    private void check(final Object obj) {
        final Object[] key = (Object[]) strategy.getPartitionKey(obj);
        assertNotNull(key);
        assertEquals(2, key.length);
        assertEquals(key[0], 1L);
        assertEquals(key[1], "test");
    }

    private void checkFailure(final Object obj) {
        assertThatThrownBy(() -> strategy.getPartitionKey(obj))
                .isInstanceOf(HazelcastException.class)
                .hasMessageMatching("Cannot extract '?\\w+'? from the key");
    }

    public static class RegularPojo implements Serializable {
        private Long id;
        private String name;

        RegularPojo() { }

        RegularPojo(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class IDSPojo implements IdentifiedDataSerializable {
        private Long id;
        private String name;

        IDSPojo() { }

        IDSPojo(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        @Override
        public void writeData(final ObjectDataOutput out) throws IOException { }

        @Override
        public void readData(final ObjectDataInput in) throws IOException { }

        @Override
        public int getFactoryId() {
            return 0;
        }

        @Override
        public int getClassId() {
            return 0;
        }
    }

    public static class NonSerializablePojo { }

    public static class PartitionAwarePojo implements PartitionAware<Object> {
        @Override
        public Object getPartitionKey() {
            return null;
        }
    }

    public static class PortablePojoFactory implements PortableFactory {
        public static final int ID = 359;
        @Override
        public Portable create(final int classId) {
            return new PortablePojo();
        }
    }

    public static class PortablePojo implements Portable {
        private Long id;
        private String name;

        public PortablePojo() {
        }

        public PortablePojo(Long id, String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        @Override
        public int getFactoryId() {
            return PortablePojoFactory.ID;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(final PortableWriter writer) throws IOException {
            writer.writeLong("id", id);
            writer.writeString("name", name);
        }

        @Override
        public void readPortable(final PortableReader reader) throws IOException {
            id = reader.readLong("id");
            name = reader.readString("name");
        }
    }
}
