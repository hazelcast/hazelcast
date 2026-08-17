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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.map.IMapAccessors;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getPartitionService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that read-only operations do not mutate a {@link RecordStore}
 * while its partition is migrating. Mutations caused by reads (expired-entry
 * eviction, access-metadata updates, and read-through MapLoader inserts) can
 * interfere with the live iterator used by chunked migration.
 */
@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RecordStoreMigrationReadTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "offload: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Parameterized.Parameter
    public boolean offload;

    @Test
    public void get_doesNotEvictExpiredEntryDuringMigration() {
        int key = 1;
        String mapName = randomMapName();
        HazelcastInstance instance = createHazelcastInstance(expiryConfig(mapName));
        IMap<Integer, String> map = instance.getMap(mapName);

        map.set(key, "value", 1, TimeUnit.MILLISECONDS);
        DefaultRecordStore recordStore = getRecordStore(instance, map, key);

        assertReadDuringMigration(instance, recordStore, () -> {
            sleepAtLeastMillis(10);

            assertNull("Expired entry should not be visible during migration", map.get(key));
            assertEquals("Expired entry must not be evicted during migration", 1, recordStore.size());
        });

        assertNull("Expired entry should still not be visible after migration", map.get(key));
        assertEquals("Expired entry should be evicted once migration is over", 0, recordStore.size());
    }

    @Test
    public void get_retriesMapStoreMissDuringMigration() throws Exception {
        int key = 1;
        String mapName = randomMapName();
        HazelcastInstance instance = createHazelcastInstance(mapStoreConfig(mapName, key));
        IMap<Integer, String> map = instance.getMap(mapName);
        DefaultRecordStore recordStore = getRecordStore(instance, map, key);

        String value = assertMapStoreReadRetriesDuringMigration(instance, recordStore, () -> map.get(key));

        assertEquals("Get should load the entry once migration is over", "loaded", value);
        assertEquals("Entry should be loaded once migration is over", 1, recordStore.size());
    }

    @Ignore
    @Test
    public void checkIfLoaded_retriesInsteadOfTriggeringLoadingDuringMigration() {
        int key = 1;
        String mapName = randomMapName();
        HazelcastInstance instance = createHazelcastInstance(mapStoreConfig(mapName, key));
        IMap<Integer, String> map = instance.getMap(mapName);
        DefaultRecordStore recordStore = getRecordStore(instance, map, key);

        CompletableFuture<Void> loadingFuture = new CompletableFuture<>();
        recordStore.getLoadingFutures().add(loadingFuture);
        InternalPartitionImpl partition = getInternalPartition(instance, recordStore.getPartitionId());
        partition.setMigrating();
        try {
            assertThrows(PartitionMigratingException.class, recordStore::checkIfLoaded);
        } finally {
            partition.resetMigrating();
            recordStore.getLoadingFutures().remove(loadingFuture);
        }
    }

    @Ignore
    @Test
    public void readPaths_doNotUpdateAccessMetadataDuringMigration() {
        int key = 1;
        String mapName = randomMapName();
        Config config = expiryConfig(mapName);
        config.getMapConfig(mapName)
                .setMaxIdleSeconds(60)
                .setPerEntryStatsEnabled(true);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, String> map = instance.getMap(mapName);
        map.set(key, "in-memory");
        DefaultRecordStore recordStore = getRecordStore(instance, map, key);
        Record<?> record = recordStore.iterator().next().getValue();
        int hitsBefore = record.getHits();

        assertReadDuringMigration(instance, recordStore, () -> {
            assertEquals("in-memory", map.get(key));
            assertTrue(map.containsKey(key));
            assertEquals(Collections.singletonMap(key, "in-memory"),
                    map.getAll(Collections.singleton(key)));
            assertEquals("Reads must not update access metadata during migration",
                    hitsBefore, record.getHits());
        });

        assertEquals("in-memory", map.get(key));
        assertTrue("Reads should update access metadata after migration",
                record.getHits() > hitsBefore);
    }

    @Test
    public void containsKey_retriesMapStoreMissDuringMigration() throws Exception {
        int key = 1;
        String mapName = randomMapName();
        HazelcastInstance instance = createHazelcastInstance(mapStoreConfig(mapName, key));
        IMap<Integer, String> map = instance.getMap(mapName);
        DefaultRecordStore recordStore = getRecordStore(instance, map, key);

        boolean containsKey = assertMapStoreReadRetriesDuringMigration(
                instance, recordStore, () -> map.containsKey(key));

        assertTrue("ContainsKey should find the entry once migration is over", containsKey);
        assertEquals("Entry should be loaded once migration is over", 1, recordStore.size());
    }

    @Test
    public void getAll_doesNotEvictExpiredEntryDuringMigration() {
        int key = 1;
        String mapName = randomMapName();
        HazelcastInstance instance = createHazelcastInstance(expiryConfig(mapName));
        IMap<Integer, String> map = instance.getMap(mapName);

        map.set(key, "value", 1, TimeUnit.MILLISECONDS);
        DefaultRecordStore recordStore = getRecordStore(instance, map, key);

        assertReadDuringMigration(instance, recordStore, () -> {
            sleepAtLeastMillis(10);

            Map<Integer, String> entries = map.getAll(Collections.singleton(key));
            assertTrue("Expired entry should not be visible during migration", entries.isEmpty());
            assertEquals("Expired entry must not be evicted during migration", 1, recordStore.size());
        });

        Map<Integer, String> entries = map.getAll(Collections.singleton(key));
        assertTrue("Expired entry should still not be visible after migration", entries.isEmpty());
        assertEquals("Expired entry should be evicted once migration is over", 0, recordStore.size());
    }

    @Test
    public void getAll_retriesMapStoreMissDuringMigration() throws Exception {
        int key = 1;
        String mapName = randomMapName();
        HazelcastInstance instance = createHazelcastInstance(mapStoreConfig(mapName, key));
        IMap<Integer, String> map = instance.getMap(mapName);
        DefaultRecordStore recordStore = getRecordStore(instance, map, key);

        Map<Integer, String> entries = assertMapStoreReadRetriesDuringMigration(
                instance, recordStore, () -> map.getAll(Collections.singleton(key)));

        assertEquals("GetAll should load the entry once migration is over",
                Collections.singletonMap(key, "loaded"), entries);
        assertEquals("Entry should be loaded once migration is over", 1, recordStore.size());
    }

    @Test
    public void readPaths_doNotEvictExpiredEntryInPassiveCluster() {
        int key = 1;
        String mapName = randomMapName();
        HazelcastInstance instance = createHazelcastInstance(expiryConfig(mapName));
        IMap<Integer, String> map = instance.getMap(mapName);

        map.set(key, "value", 1, TimeUnit.MILLISECONDS);
        DefaultRecordStore recordStore = getRecordStore(instance, map, key);

        assertReadInPassiveCluster(instance, () -> {
            sleepAtLeastMillis(10);

            assertNull("Expired entry should not be visible in passive cluster", map.get(key));
            assertEquals("Expired entry must not be evicted in passive cluster", 1, recordStore.size());
        });

        assertNull("Expired entry should still not be visible after cluster is active", map.get(key));
        assertEquals("Expired entry should be evicted once cluster is active", 0, recordStore.size());
    }

    @Test
    public void readPaths_rejectMapStoreMissInPassiveCluster() {
        int key = 1;
        String mapName = randomMapName();
        HazelcastInstance instance = createHazelcastInstance(mapStoreConfig(mapName, key));
        IMap<Integer, String> map = instance.getMap(mapName);
        DefaultRecordStore recordStore = getRecordStore(instance, map, key);

        assertReadInPassiveCluster(instance, () -> {
            assertEquals("Record store should be empty before passive reads", 0, recordStore.size());

            assertThrows(IllegalStateException.class, () -> map.get(key));
            assertThrows(IllegalStateException.class, () -> map.containsKey(key));
            assertThrows(IllegalStateException.class, () -> map.getAll(Collections.singleton(key)));

            assertEquals("Missing entry must not be loaded in passive cluster", 0, recordStore.size());
        });

        assertEquals("Entry should be loaded once cluster is active", "loaded", map.get(key));
        assertEquals("Entry should be loaded once cluster is active", 1, recordStore.size());
    }

    private Config expiryConfig(String mapName) {
        Config config = new Config();
        config.setProperty(MapServiceContext.PROP_FORCE_OFFLOAD_ALL_OPERATIONS, String.valueOf(offload));
        config.getMapConfig(mapName);
        return config;
    }

    private Config mapStoreConfig(String mapName, int key) {
        MapConfig mapConfig = new MapConfig(mapName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(new SimpleMapLoader<>(Collections.singletonMap(key, "loaded")));
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY);
        mapStoreConfig.setOffload(offload);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        Config config = new Config();
        config.addMapConfig(mapConfig);
        return config;
    }

    private void assertReadDuringMigration(HazelcastInstance instance, DefaultRecordStore recordStore, Runnable assertion) {
        InternalPartitionImpl partition = getInternalPartition(instance, recordStore.getPartitionId());
        partition.setMigrating();
        try {
            assertion.run();
        } finally {
            partition.resetMigrating();
        }
    }

    private <T> T assertMapStoreReadRetriesDuringMigration(HazelcastInstance instance,
                                                           DefaultRecordStore recordStore,
                                                           Callable<T> read) throws Exception {
        assertEquals("Record store should initially be empty", 0, recordStore.size());

        InternalPartitionImpl partition = getInternalPartition(instance, recordStore.getPartitionId());
        partition.setMigrating();
        Future<T> readFuture = spawn(read);
        try {
            sleepAtLeastMillis(1_000);

            assertFalse("Read should wait for a stable partition instead of returning a false negative",
                    readFuture.isDone());
            assertEquals("Missing entry must not be loaded during migration", 0, recordStore.size());
        } finally {
            partition.resetMigrating();
        }

        assertCompletesEventually(readFuture);
        return readFuture.get();
    }

    private void assertReadInPassiveCluster(HazelcastInstance instance, Runnable assertion) {
        warmUpPartitions(instance);
        instance.getCluster().changeClusterState(ClusterState.PASSIVE);
        try {
            assertion.run();
        } finally {
            instance.getCluster().changeClusterState(ClusterState.ACTIVE);
        }
    }

    private DefaultRecordStore getRecordStore(HazelcastInstance instance, IMap<Integer, String> map, int key) {
        MapServiceContext mapServiceContext = IMapAccessors.getMapServiceContext(map);
        int partitionId = mapServiceContext.getNodeEngine().getPartitionService().getPartitionId(key);
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        return (DefaultRecordStore) container.getRecordStore(map.getName());
    }

    private InternalPartitionImpl getInternalPartition(HazelcastInstance instance, int partitionId) {
        IPartitionService partitionService = getPartitionService(instance);
        return (InternalPartitionImpl) partitionService.getPartition(partitionId);
    }

    private record SimpleMapLoader<K, V>(
            Map<K, V> data) implements MapLoader<K, V> {

        @Override
        public V load(K key) {
            return data.get(key);
        }

        @Override
        public Map<K, V> loadAll(Collection<K> keys) {
            Map<K, V> result = new HashMap<>();
            for (K key : keys) {
                if (data.containsKey(key)) {
                    result.put(key, data.get(key));
                }
            }
            return result;
        }

        @Override
        public Iterable<K> loadAllKeys() {
            return data.keySet();
        }
    }
}
