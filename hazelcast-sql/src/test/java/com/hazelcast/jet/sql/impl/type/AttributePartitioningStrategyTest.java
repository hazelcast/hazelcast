/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.type;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.AttributePartitioningStrategy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class AttributePartitioningStrategyTest extends SqlJsonTestSupport {
    private static final ClassDefinition PORTABLE_CLASS = new ClassDefinitionBuilder(1, 1, 1)
            .addLongField("id")
            .addStringField("name")
            .addStringField("org")
            .build();

    private static final PartitioningStrategy STRATEGY = new AttributePartitioningStrategy("org");
    private static final String PARTITION_ATTRIBUTE = "1";
    private static final Object[] PARTITION_KEY = new Object[]{PARTITION_ATTRIBUTE};

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig().setProperty(PARTITION_COUNT.getName(), "3");
        config.getMapConfig("test").setPartitioningStrategyConfig(new PartitioningStrategyConfig()
                .setPartitioningStrategy(STRATEGY));
        config.getMapConfig("test").addIndexConfig(new IndexConfig(IndexConfig.DEFAULT_TYPE, "__key.org"));

        initialize(3, config);
    }

    @Test
    public void testJavaObject() {
        final IMap<JavaKey, Long> map = instance().getMap("test");

        for (long i = 0; i < 10000; i++) {
            map.put(new JavaKey(i, "key#" + i, PARTITION_ATTRIBUTE), i);
        }

        final HazelcastInstance owner = getOwner(PARTITION_KEY);
        final List<HazelcastInstance> nonOwners = getNonOwners(owner);

        assertEquals(10000, owner.getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(0).getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(1).getMap("test").getLocalMapStats().getOwnedEntryCount());
    }

    @Test
    public void testCompactObject() {
        final IMap<GenericRecord, Long> map = instance().getMap("test");

        for (long i = 0; i < 10000; i++) {
            map.put(GenericRecordBuilder.compact("testType")
                    .setInt64("id", i)
                    .setString("name", "key#" + i)
                    .setString("org", PARTITION_ATTRIBUTE)
                    .build(), i);
        }

        final HazelcastInstance owner = getOwner(PARTITION_KEY);
        final List<HazelcastInstance> nonOwners = getNonOwners(owner);

        assertEquals(10000, owner.getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(0).getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(1).getMap("test").getLocalMapStats().getOwnedEntryCount());
    }

    @Test
    public void testPortableObject() {
        final IMap<GenericRecord, Long> map = instance().getMap("test");

        for (long i = 0; i < 10000; i++) {
            map.put(GenericRecordBuilder.portable(PORTABLE_CLASS)
                    .setInt64("id", i)
                    .setString("name", "key#" + i)
                    .setString("org", PARTITION_ATTRIBUTE)
                    .build(), i);
        }

        final HazelcastInstance owner = getOwner(PARTITION_KEY);
        final List<HazelcastInstance> nonOwners = getNonOwners(owner);

        assertEquals(10000, owner.getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(0).getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(1).getMap("test").getLocalMapStats().getOwnedEntryCount());
    }

    @Test
    public void testJsonObject() {
        final String template = "{"
                + "\"id\": <id>,"
                + "\"name\": \"<name>\","
                + "\"org\": \"<org>\""
                + "}";
        final IMap<HazelcastJsonValue, Long> map = instance().getMap("test");

        for (long i = 0; i < 10000; i++) {
            final String key = template
                    .replace("<id>", String.valueOf(i))
                    .replace("<>", "key#" + i)
                    .replace("<org>", PARTITION_ATTRIBUTE);
            map.put(json(key), i);
        }

        final HazelcastInstance owner = getOwner(PARTITION_KEY);
        final List<HazelcastInstance> nonOwners = getNonOwners(owner);

        assertEquals(10000, owner.getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(0).getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(1).getMap("test").getLocalMapStats().getOwnedEntryCount());
    }

    private HazelcastInstance getOwner(Object partitionKey) {
        final Partition partition = instance().getPartitionService().getPartition(partitionKey);
        if (partition == null) {
            throw new HazelcastException("Can not find partition for key: " + partitionKey);
        }

        return Arrays.stream(instances())
                .filter(instance -> ((HazelcastInstanceProxy) instance).getOriginal().node.address
                        .equals(partition.getOwner().getAddress()))
                .findFirst()
                .orElseThrow(() -> new HazelcastException("Can not find partition owner"));
    }

    private List<HazelcastInstance> getNonOwners(final HazelcastInstance owner) {
        return Arrays.stream(instances())
                .filter(instance -> !instance.equals(owner))
                .collect(Collectors.toList());
    }

    public static class JavaKey implements Serializable {
        private Long id;
        private String name;
        private String org;

        public JavaKey() {
        }

        public JavaKey(final Long id, final String name, final String org) {
            this.id = id;
            this.name = name;
            this.org = org;
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

        public String getOrg() {
            return org;
        }

        public void setOrg(final String org) {
            this.org = org;
        }
    }
}
