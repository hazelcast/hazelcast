/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
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

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AttributePartitioningStrategyTest {
    private final AttributePartitioningStrategy strategy = new AttributePartitioningStrategy("id", "name");

    @Test
    public void test_partitionAware() {
        assertNull(strategy.getPartitionKey(new PartitionAwarePojo()));
    }

    @Test
    public void test_portablePojo() {
        assertNull(strategy.getPartitionKey(new PortablePojo()));
    }

    @Test
    public void test_compactPojoOrNonSerializablePojo() {
        assertNull(strategy.getPartitionKey(new NonSerializablePojo()));
    }

    @Test
    public void test_json() {
        final String json = "{"
                + "\"name\":\"test\","
                + "\"id\":1"
                + "}";
        final Object[] key = (Object[]) strategy.getPartitionKey(new HazelcastJsonValue(json));
        assertEquals(1L, key[0]);
        assertEquals("test", key[1]);
        assertNull(strategy.getPartitionKey(new HazelcastJsonValue("{}")));
        assertNull(strategy.getPartitionKey(new HazelcastJsonValue("{\"name\":\"test\"}")));
        assertNull(strategy.getPartitionKey(new HazelcastJsonValue("{\"id\": 1}")));
    }

    @Test
    public void test_pojo() {
        final Object[] regularKey = (Object[]) strategy.getPartitionKey(new RegularPojo(1L, "test"));
        assertEquals(1L, regularKey[0]);
        assertEquals("test", regularKey[1]);
        final Object[] idsKey = (Object[]) strategy.getPartitionKey(new IDSPojo(1L, "test"));
        assertEquals(1L, idsKey[0]);
        assertEquals("test", idsKey[1]);
        assertNull(strategy.getPartitionKey(new RegularPojo(1L, null)));
        assertNull(strategy.getPartitionKey(new RegularPojo(null, "test")));
        assertNull(strategy.getPartitionKey(new IDSPojo(1L, null)));
        assertNull(strategy.getPartitionKey(new IDSPojo(null, "test")));
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

        final Object[] key = (Object[]) strategy.getPartitionKey(record);
        assertEquals(1L, key[0]);
        assertEquals("test", key[1]);
        assertNull(strategy.getPartitionKey(idLess));
        assertNull(strategy.getPartitionKey(nameLess));
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

        final Object[] key = (Object[]) strategy.getPartitionKey(record);
        assertEquals(1L, key[0]);
        assertEquals("test", key[1]);
        assertNull(strategy.getPartitionKey(nameLess));
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

    public static class PortablePojo implements Portable {
        @Override
        public int getFactoryId() {
            return 0;
        }

        @Override
        public int getClassId() {
            return 0;
        }

        @Override
        public void writePortable(final PortableWriter writer) throws IOException { }

        @Override
        public void readPortable(final PortableReader reader) throws IOException { }
    }
}
