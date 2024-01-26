/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.partition.Partition;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class AttributePartitioningStrategyIntegrationTest extends SimpleTestInClusterSupport {
    private static final ClassDefinition PORTABLE_CLASS = new ClassDefinitionBuilder(1, 1, 1)
            .addLongField("id")
            .addStringField("name")
            .addStringField("org")
            .build();

    private static final String PARTITION_ATTRIBUTE_VALUE = "1";

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig().setProperty(PARTITION_COUNT.getName(), "3");
        config.getMapConfig("test").getPartitioningAttributeConfigs()
                .add(new PartitioningAttributeConfig("org"));

        config.getSerializationConfig()
                .addDataSerializableFactoryClass(1, IDSKeyFactory.class);

        initializeWithClient(3, config, null);
    }

    @Test
    public void testJavaObject() {
        final IMap<JavaKey, Long> map = instance().getMap("test");

        for (long i = 0; i < 100; i++) {
            map.put(new JavaKey(i, "key#" + i, PARTITION_ATTRIBUTE_VALUE), i);
        }

        final HazelcastInstance owner = getOwner(PARTITION_ATTRIBUTE_VALUE);
        final List<HazelcastInstance> nonOwners = getNonOwners(owner);

        assertEquals(100, owner.getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(0).getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(1).getMap("test").getLocalMapStats().getOwnedEntryCount());
    }

    @Test
    public void testIDSObject() {
        final IMap<IDSKey, Long> map = instance().getMap("test");

        for (long i = 0; i < 100; i++) {
            map.put(new IDSKey(i, "key#" + i, PARTITION_ATTRIBUTE_VALUE), i);
        }

        final HazelcastInstance owner = getOwner(PARTITION_ATTRIBUTE_VALUE);
        final List<HazelcastInstance> nonOwners = getNonOwners(owner);

        assertEquals(100, owner.getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(0).getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(1).getMap("test").getLocalMapStats().getOwnedEntryCount());
    }

    @Test
    public void testCompactObject() {
        final IMap<GenericRecord, Long> map = instance().getMap("test");

        for (long i = 0; i < 100; i++) {
            map.put(GenericRecordBuilder.compact("testType")
                    .setInt64("id", i)
                    .setString("name", "key#" + i)
                    .setString("org", PARTITION_ATTRIBUTE_VALUE)
                    .build(), i);
        }

        final HazelcastInstance owner = getOwner(PARTITION_ATTRIBUTE_VALUE);
        final List<HazelcastInstance> nonOwners = getNonOwners(owner);

        assertEquals(100, owner.getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(0).getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(1).getMap("test").getLocalMapStats().getOwnedEntryCount());
    }

    @Test
    public void testPortableObject() {
        final IMap<GenericRecord, Long> map = instance().getMap("test");

        for (long i = 0; i < 100; i++) {
            map.put(GenericRecordBuilder.portable(PORTABLE_CLASS)
                    .setInt64("id", i)
                    .setString("name", "key#" + i)
                    .setString("org", PARTITION_ATTRIBUTE_VALUE)
                    .build(), i);
        }

        final HazelcastInstance owner = getOwner(PARTITION_ATTRIBUTE_VALUE);
        final List<HazelcastInstance> nonOwners = getNonOwners(owner);

        assertEquals(100, owner.getMap("test").getLocalMapStats().getOwnedEntryCount());
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

        for (long i = 0; i < 100; i++) {
            final String key = template
                    .replace("<id>", String.valueOf(i))
                    .replace("<name>", "key#" + i)
                    .replace("<org>", PARTITION_ATTRIBUTE_VALUE);
            map.put(new HazelcastJsonValue(key), i);
        }

        final HazelcastInstance owner = getOwner(PARTITION_ATTRIBUTE_VALUE);
        final List<HazelcastInstance> nonOwners = getNonOwners(owner);

        assertEquals(100, owner.getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(0).getMap("test").getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(1).getMap("test").getLocalMapStats().getOwnedEntryCount());
    }

    @Test
    public void testDynamicPartitioningAttributesConfiguration() {
        final List<PartitioningAttributeConfig> attributeConfigs =
                singletonList(new PartitioningAttributeConfig("name"));
        String mapName = randomName();
        final MapConfig mapConfig = new MapConfig(mapName).setPartitioningAttributeConfigs(attributeConfigs);

        client().getConfig().addMapConfig(mapConfig);

        for (final HazelcastInstance instance : instances()) {
            assertTrueEventually(() -> assertEquals(mapConfig, instance.getConfig().getMapConfig(mapName)));
        }

        final IMap<JavaKey, Long> map = instance().getMap(mapName);

        for (long i = 0; i < 100; i++) {
            map.put(new JavaKey(i, PARTITION_ATTRIBUTE_VALUE, "org#" + i), i);
        }

        final HazelcastInstance owner = getOwner(PARTITION_ATTRIBUTE_VALUE);
        final List<HazelcastInstance> nonOwners = getNonOwners(owner);

        assertEquals(100, owner.getMap(mapName).getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(0).getMap(mapName).getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, nonOwners.get(1).getMap(mapName).getLocalMapStats().getOwnedEntryCount());
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

    public static class IDSKey implements IdentifiedDataSerializable {
        private Long id;
        private String name;
        private String org;

        public IDSKey() {

        }

        public IDSKey(final Long id, final String name, final String org) {
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

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(id);
            out.writeString(name);
            out.writeString(org);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            id = in.readLong();
            name = in.readString();
            org = in.readString();
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }
    }

    public static class IDSKeyFactory implements DataSerializableFactory {

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            return typeId == 1 ? new IDSKey() : null;
        }
    }
}
