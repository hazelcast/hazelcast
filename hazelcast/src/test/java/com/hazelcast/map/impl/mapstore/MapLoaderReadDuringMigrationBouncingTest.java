/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_CHUNKED_MAX_MIGRATING_DATA_IN_MB;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.Accessors.getSerializationService;
import static com.hazelcast.test.TestTaskExecutorUtil.runOnPartitionThread;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapLoaderReadDuringMigrationBouncingTest extends HazelcastTestSupport {

    protected static final String MAP_NAME = "readMutationDuringMigration";

    private static final int CLUSTER_SIZE = 3;
    private static final int TEST_DURATION_SECONDS = 30;
    private static final int TEST_PARTITION_COUNT = 17;
    private static final int ENTRIES_PER_PARTITION = 256;
    private static final int LOAD_BATCH_SIZE = 64;
    private static final int VALUE_SIZE_BYTES = 8 * 1024;
    private static final int MAP_LOADER_KEY_BASE = 1_000_000;
    private static final int MIN_READS_OBSERVED_DURING_MIGRATION = 10;
    private static final byte[] LOADED_VALUE = new byte[VALUE_SIZE_BYTES];

    private final AtomicInteger nextMapLoaderKey = new AtomicInteger(MAP_LOADER_KEY_BASE);
    private final MapLoader<Integer, byte[]> mapLoader = new TestMapLoader();

    @Rule
    public BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(this::getConfig)
                    .driverType(BounceTestConfiguration.DriverType.LITE_MEMBER)
                    .clusterSize(CLUSTER_SIZE)
                    .driverCount(1)
                    .build();

    @Before
    public void populateMap() {
        HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        Map<Integer, List<Integer>> keysByPartition = keysByPartition(steadyMember);
        Map<Integer, byte[]> entries = new HashMap<>(TEST_PARTITION_COUNT * ENTRIES_PER_PARTITION);
        byte[] value = new byte[VALUE_SIZE_BYTES];

        for (List<Integer> keys : keysByPartition.values()) {
            for (Integer key : keys) {
                entries.put(key, value);
            }
        }

        steadyMember.<Integer, byte[]>getMap(MAP_NAME).putAll(entries);
    }

    /**
     * Repeatedly starts public {@code getAll} reads for keys available only from MapLoader while their
     * primary partition is undergoing real chunked migration. The test snapshots the migrating source
     * record store and fails if a read changes its entry count or inserts any of the requested keys.
     */
    @Test
    public void getAll_doesNotMutateRecordStoreDuringMigration() {
        IMap<Integer, byte[]> map = bounceMemberRule.getNextTestDriver().getMap(MAP_NAME);
        AtomicInteger readsObservedDuringMigration = new AtomicInteger();

        bounceMemberRule.testRepeatedly(1,
                () -> assertRecordStoreIsNotMutatedDuringMigration(map, readsObservedDuringMigration),
                TEST_DURATION_SECONDS);

        assertTrue("Expected at least " + MIN_READS_OBSERVED_DURING_MIGRATION
                        + " getAll calls whose record store was observed while its primary partition was migrating,"
                        + " but observed " + readsObservedDuringMigration.get(),
                readsObservedDuringMigration.get() >= MIN_READS_OBSERVED_DURING_MIGRATION);
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(PARTITION_COUNT.getName(), String.valueOf(TEST_PARTITION_COUNT));
        config.setProperty(PARTITION_CHUNKED_MAX_MIGRATING_DATA_IN_MB.getName(), "1");

        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setMapStoreConfig(new MapStoreConfig()
                .setImplementation(mapLoader)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY));
        config.addMapConfig(mapConfig);
        return config;
    }

    private void assertRecordStoreIsNotMutatedDuringMigration(IMap<Integer, byte[]> map,
                                                              AtomicInteger readsObservedDuringMigration) {
        MigrationTarget target = findMigratingPrimary();
        if (target == null) {
            return;
        }

        Set<Integer> keys = nextMapLoaderKeys(target.partitionId());
        Set<Data> dataKeys = toDataKeys(target.member(), keys);
        RecordStoreState before = recordStoreStateIfMigrating(target, dataKeys);
        if (before == null) {
            return;
        }
        assertEquals("MapLoader keys must be absent before getAll", 0, before.presentReadKeyCount());

        Future<Map<Integer, byte[]>> readFuture = spawn(() -> map.getAll(keys));
        boolean observedDuringMigration = false;
        while (true) {
            RecordStoreState during = recordStoreStateIfMigrating(target, dataKeys);
            if (during == null) {
                break;
            }

            assertEquals("getAll mutated the record store while partition " + target.partitionId()
                    + " was migrating", before, during);
            observedDuringMigration = true;

            if (readFuture.isDone()) {
                // The read may have completed after the snapshot above. Take one more snapshot so a
                // mutation immediately before completion cannot escape the assertion.
                RecordStoreState afterRead = recordStoreStateIfMigrating(target, dataKeys);
                if (afterRead != null) {
                    assertEquals("getAll mutated the record store while partition " + target.partitionId()
                            + " was migrating", before, afterRead);
                }
                break;
            }
            sleepMillis(1);
        }

        if (observedDuringMigration) {
            readsObservedDuringMigration.incrementAndGet();
        }

        Map<Integer, byte[]> result = get(readFuture);
        assertEquals("getAll returned incomplete MapLoader results for partition " + target.partitionId(),
                keys, result.keySet());
    }

    private RecordStoreState recordStoreStateIfMigrating(MigrationTarget target, Set<Data> readKeys) {
        if (!target.member().getLifecycleService().isRunning()) {
            return null;
        }

        try {
            return runOnPartitionThread(target.member(), () -> {
                if (!target.isStillMigratingPrimary()) {
                    return null;
                }
                return recordStoreState(getRecordStore(target), readKeys);
            }, target.partitionId());
        } catch (RuntimeException e) {
            if (!target.member().getLifecycleService().isRunning()) {
                return null;
            }
            throw e;
        }
    }

    private RecordStore<?> getRecordStore(MigrationTarget target) {
        MapService mapService = getNodeEngineImpl(target.member()).getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        RecordStore<?> recordStore = mapServiceContext.getExistingRecordStore(target.partitionId(), MAP_NAME);
        assertNotNull("Missing record store for partition " + target.partitionId(), recordStore);
        return recordStore;
    }

    private static Set<Data> toDataKeys(HazelcastInstance member, Set<Integer> keys) {
        Set<Data> dataKeys = new HashSet<>(keys.size());
        for (Integer key : keys) {
            dataKeys.add(getSerializationService(member).toData(key));
        }
        return dataKeys;
    }

    private static RecordStoreState recordStoreState(RecordStore<?> recordStore, Set<Data> readKeys) {
        int presentReadKeyCount = 0;
        for (Data key : readKeys) {
            if (recordStore.getRecord(key) != null) {
                presentReadKeyCount++;
            }
        }
        return new RecordStoreState(recordStore.size(), presentReadKeyCount);
    }

    private static <T> T get(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for getAll", e);
        } catch (ExecutionException e) {
            throw new AssertionError("getAll failed", e.getCause());
        }
    }

    private Set<Integer> nextMapLoaderKeys(int partitionId) {
        HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        List<Integer> keys = new ArrayList<>(LOAD_BATCH_SIZE);
        while (keys.size() < LOAD_BATCH_SIZE) {
            int key = nextMapLoaderKey.getAndIncrement();
            if (steadyMember.getPartitionService().getPartition(key).getPartitionId() == partitionId) {
                keys.add(key);
            }
        }
        return Set.copyOf(keys);
    }

    private MigrationTarget findMigratingPrimary() {
        for (HazelcastInstance member : bounceMemberRule.getMembersSnapshot()) {
            if (member == null || !member.getLifecycleService().isRunning()) {
                continue;
            }

            IPartitionService partitionService = getPartitionService(member);
            for (int partitionId = 0; partitionId < TEST_PARTITION_COUNT; partitionId++) {
                IPartition partition = partitionService.getPartition(partitionId);
                if (partition.isLocal() && partition.isMigrating()) {
                    return new MigrationTarget(member, partitionId);
                }
            }
        }
        return null;
    }

    private static Map<Integer, List<Integer>> keysByPartition(HazelcastInstance instance) {
        Map<Integer, List<Integer>> result = new HashMap<>(TEST_PARTITION_COUNT);
        for (int partitionId = 0; partitionId < TEST_PARTITION_COUNT; partitionId++) {
            result.put(partitionId, new ArrayList<>(ENTRIES_PER_PARTITION));
        }

        for (int key = 0, remaining = TEST_PARTITION_COUNT * ENTRIES_PER_PARTITION; remaining > 0; key++) {
            int partitionId = instance.getPartitionService().getPartition(key).getPartitionId();
            List<Integer> keys = result.get(partitionId);
            if (keys.size() < ENTRIES_PER_PARTITION) {
                keys.add(key);
                remaining--;
            }
        }
        return result;
    }

    private record MigrationTarget(HazelcastInstance member, int partitionId) {

        private boolean isStillMigratingPrimary() {
            if (!member.getLifecycleService().isRunning()) {
                return false;
            }
            IPartition partition = getPartitionService(member).getPartition(partitionId);
            return partition.isLocal() && partition.isMigrating();
        }
    }

    private record RecordStoreState(int size, int presentReadKeyCount) {
    }

    private static final class TestMapLoader implements MapLoader<Integer, byte[]> {

        @Override
        public byte[] load(Integer key) {
            return LOADED_VALUE;
        }

        @Override
        public Map<Integer, byte[]> loadAll(Collection<Integer> keys) {
            Map<Integer, byte[]> result = new HashMap<>(keys.size());
            for (Integer key : keys) {
                result.put(key, LOADED_VALUE);
            }
            return result;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            return List.of();
        }
    }
}
