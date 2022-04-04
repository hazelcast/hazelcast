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

package com.hazelcast.map.merge;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMapAccessors;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.Partition;
import com.hazelcast.query.impl.GlobalIndexPartitionTracker;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Tests internal state:
 * <ul>
 *     <li>marked indexes state</li>
 *     <li>map-container state</li>
 * </ul>
 */
@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapInternalStateSplitBrainTest extends SplitBrainTestSupport {

    private static final int[] BRAINS = new int[]{3, 2};

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
        });
    }

    private static final int ENTRY_COUNT = 10_000;
    private static final String[] MAP_NAMES = {"mapNameA", "mapNameB"};

    private Map<String, List<GlobalIndexPartitionTracker.PartitionStamp>> stampsByMap_BeforeSplit = new HashMap<>();

    @Override
    protected int[] brains() {
        return BRAINS;
    }

    @Override
    protected Config config() {
        Config config = super.config();
        config.getMetricsConfig().setEnabled(false);
        MapConfig mapConfig = new MapConfig();
        return config.addMapConfig(mapConfig
                .setName("mapName*").setInMemoryFormat(inMemoryFormat).setBackupCount(1)
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "value")));
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);

        for (long key = 0; key < ENTRY_COUNT; key++) {
            String randomValue = randomString();
            for (String mapName : MAP_NAMES) {
                instances[0].getMap(mapName).put(key, new TestObject(key, randomValue));
            }
        }

        // collect mapContainer objects
        stampsByMap_BeforeSplit = toPerMapIndexStamps(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) throws Exception {
        System.out.println(toMemberToPartitionsMap(firstBrain[0]));
        System.out.println(toMemberToPartitionsMap(secondBrain[0]));
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        Map<UUID, PartitionIdSet> ownedPartitionsAfterHeal = toMemberToPartitionsMap(instances[0]);
        try {
            assertTrueEventually(() -> {

                Map<String, List<GlobalIndexPartitionTracker.PartitionStamp>> stampsByMap_After_Heal = toPerMapIndexStamps(instances);
                for (Map.Entry<String, List<GlobalIndexPartitionTracker.PartitionStamp>> e : stampsByMap_After_Heal.entrySet()) {
                    String mapName = e.getKey();
                    List<GlobalIndexPartitionTracker.PartitionStamp> after_stamps = e.getValue();
                    List<GlobalIndexPartitionTracker.PartitionStamp> before_stamps = stampsByMap_BeforeSplit.get(mapName);

                    assertEquals("ownedPartitionsAfterHeal=" + ownedPartitionsAfterHeal
                                    + ", " + stampsToString(after_stamps) + ", but expected " + stampsToString(before_stamps),
                            markedPartitionCount(after_stamps), markedPartitionCount(before_stamps));

                }
            });
        } catch (AssertionError e) {
            throw new AssertionError(mapPartitionSizesToString(instances) + "\n" + toOwnersMsg(instances), e);
        }
    }

    @NotNull
    private String mapPartitionSizesToString(HazelcastInstance[] instances) {
        String rs = "";
        for (HazelcastInstance instance : instances) {
            for (String mapName : MAP_NAMES) {
                MapServiceContext mapServiceContext = IMapAccessors.getMapServiceContext(instance.getMap(mapName));
                int partitionCount = mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount();
                rs += "\n";
                rs += "mapName=" + mapName + ", ";
                for (int i = 0; i < partitionCount; i++) {
                    RecordStore recordStore = mapServiceContext
                            .getPartitionContainer(i).getRecordStore(mapName);
                    int size = recordStore.size();
                    if (size > 0) {
                        rs += " pId:" + i + " size:" + size + " uuid:" + instance.getCluster().getLocalMember().getUuid();
                        rs += "\n";
                    }
                }
            }
        }
        return rs;
    }

    @NotNull
    private String toOwnersMsg(HazelcastInstance[] instances) {
        Map<UUID, List<GlobalIndexPartitionTracker.PartitionStamp>> uuidListMap = toPerMemberIndexStamps(instances);
        String members = "\n";
        Map<UUID, PartitionIdSet> uuidPartitionIdSetMap = toMemberToPartitionsMap(instances[0]);
        for (Map.Entry<UUID, PartitionIdSet> entry : uuidPartitionIdSetMap.entrySet()) {
            members += entry.getKey() + " --> " + entry.getValue() + ", " + uuidListMap.get(entry.getKey());
            members += "\n";
        }
        return members;
    }

    private String stampsToString(List<GlobalIndexPartitionTracker.PartitionStamp> stamps) {
        String size = "";
        for (GlobalIndexPartitionTracker.PartitionStamp stamp : stamps) {
            size += (stamp == null ? "null" : stamp.partitions);
        }
        return size;
    }

    private int markedPartitionCount(List<GlobalIndexPartitionTracker.PartitionStamp> stamps) {
        int count = 0;
        for (GlobalIndexPartitionTracker.PartitionStamp stamp : stamps) {
            count += (stamp == null ? 0 : stamp.partitions.size());
        }
        return count;
    }

    private static Map<String, List<GlobalIndexPartitionTracker.PartitionStamp>> toPerMapIndexStamps(HazelcastInstance[] instances) {
        Map<String, List<GlobalIndexPartitionTracker.PartitionStamp>> stampByMap = new HashMap<>();

        for (String mapName : MAP_NAMES) {
            for (HazelcastInstance instance : instances) {
                MapContainer container = IMapAccessors.getExistingMapContainer(instance.getMap(mapName));

                boolean globalIndex = container.getMapServiceContext().getNodeEngine()
                        .getProperties().getBoolean(ClusterProperty.GLOBAL_HD_INDEX_ENABLED);

                if (globalIndex) {
                    InternalIndex[] indexes = container.getIndexes().getIndexes();
                    for (InternalIndex index : indexes) {
                        stampByMap.computeIfAbsent(mapName,
                                s -> new ArrayList<>()).add(index.getPartitionStamp());
                    }
                } else {
                    int partitionCount = container.getMapServiceContext().getNodeEngine().getPartitionService().getPartitionCount();
                    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                        InternalIndex[] indexes = container.getIndexes(partitionId).getIndexes();
                        for (InternalIndex index : indexes) {
                            stampByMap.computeIfAbsent(mapName,
                                    s -> new ArrayList<>()).add(index.getPartitionStamp());
                        }
                    }
                }
            }
        }

        return stampByMap;
    }

    private static Map<UUID, List<GlobalIndexPartitionTracker.PartitionStamp>> toPerMemberIndexStamps(HazelcastInstance[] instances) {
        Map<UUID, List<GlobalIndexPartitionTracker.PartitionStamp>> stampByMap = new HashMap<>();

        for (String mapName : MAP_NAMES) {
            for (HazelcastInstance instance : instances) {
                MapContainer container = IMapAccessors.getExistingMapContainer(instance.getMap(mapName));

                boolean globalIndex = container.getMapServiceContext().getNodeEngine()
                        .getProperties().getBoolean(ClusterProperty.GLOBAL_HD_INDEX_ENABLED);
                if (globalIndex) {
                    InternalIndex[] indexes = container.getIndexes().getIndexes();
                    for (InternalIndex index : indexes) {
                        stampByMap.computeIfAbsent(instance.getCluster().getLocalMember().getUuid(),
                                s -> new ArrayList<>()).add(index.getPartitionStamp());
                    }
                } else {
                    int partitionCount = container.getMapServiceContext().getNodeEngine().getPartitionService().getPartitionCount();
                    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                        InternalIndex[] indexes = container.getIndexes(partitionId).getIndexes();
                        for (InternalIndex index : indexes) {
                            stampByMap.computeIfAbsent(instance.getCluster().getLocalMember().getUuid(),
                                    s -> new ArrayList<>()).add(index.getPartitionStamp());
                        }
                    }
                }
            }
        }

        return stampByMap;
    }

    private Map<UUID, PartitionIdSet> toMemberToPartitionsMap(HazelcastInstance instance1) {
        Map<UUID, PartitionIdSet> memberToPartitions = new HashMap<>();

        Set<Partition> partitions = instance1.getPartitionService().getPartitions();

        for (Partition partition : partitions) {
            Member owner = partition.getOwner();
            if (owner == null) {
                continue;
            }
            UUID member = owner.getUuid();

            memberToPartitions.computeIfAbsent(member, (key) -> new PartitionIdSet(partitions.size()))
                    .add(partition.getPartitionId());
        }
        return memberToPartitions;
    }

    private static class TestObject implements Serializable {

        private Long id;
        private String value;

        TestObject() {
        }

        TestObject(Long id, String value) {
            this.id = id;
            this.value = value;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
