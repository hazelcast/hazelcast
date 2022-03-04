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

package com.hazelcast.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.eviction.impl.comparator.LRUEvictionPolicyComparator;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.MemoryInfoAccessor;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.eviction.EvictionChecker;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.eviction.EvictorImpl;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.config.MaxSizePolicy.FREE_HEAP_PERCENTAGE;
import static com.hazelcast.config.MaxSizePolicy.FREE_HEAP_SIZE;
import static com.hazelcast.config.MaxSizePolicy.PER_NODE;
import static com.hazelcast.config.MaxSizePolicy.PER_PARTITION;
import static com.hazelcast.config.MaxSizePolicy.USED_HEAP_PERCENTAGE;
import static com.hazelcast.config.MaxSizePolicy.USED_HEAP_SIZE;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EvictionMaxSizePolicyTest extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 271;
    private static final int PARTITION_MAX_PARALLEL_MIGRATIONS = 1;

    @Test
    public void testPerNodePolicy() {
        int nodeCount = 1;
        testPerNodePolicy(nodeCount);
    }

    @Test
    public void testPerNodePolicy_withManyNodes() {
        int nodeCount = 2;
        testPerNodePolicy(nodeCount);
    }

    @Test
    public void testPerNodePolicy_afterGracefulShutdown() {
        int nodeCount = 3;
        int perNodeMaxSize = 1000;
        int numberOfPuts = 5000;

        // eviction takes place if a partitions size exceeds this number
        // see EvictionChecker#toPerPartitionMaxSize
        double maxPartitionSize = 1D * nodeCount * perNodeMaxSize / PARTITION_COUNT;

        String mapName = "testPerNodePolicy_afterGracefulShutdown";
        Config config = createConfig(PER_NODE, perNodeMaxSize, mapName);
        Set<Object> evictedKeySet = Collections.newSetFromMap(new ConcurrentHashMap<>());
        // populate map from one of the nodes
        List<HazelcastInstance> nodes = createNodes(nodeCount, config);
        assertClusterSize(3, nodes.toArray(new HazelcastInstance[0]));

        IMap map = nodes.get(0).getMap(mapName);
        for (int i = 0; i < numberOfPuts; i++) {
            map.put(i, i);
        }

        int initialMapSize = map.size();

        nodes.get(1).getMap(mapName).addEntryListener((EntryEvictedListener) event -> {
            evictedKeySet.add(event.getKey());
        }, true);

        nodes.get(0).shutdown();

        assertTrueEventually(() -> {
            for (HazelcastInstance node : nodes) {
                if (node.getLifecycleService().isRunning()) {
                    int currentMapSize = node.getMap(mapName).size();
                    int evictedKeyCount = evictedKeySet.size();
                    String message = format("initialMapSize=%d, evictedKeyCount=%d, map size is %d and it should be smaller "
                                    + "than maxPartitionSize * PARTITION_COUNT which is %.0f",
                            initialMapSize, evictedKeyCount, currentMapSize, maxPartitionSize * PARTITION_COUNT);

                    assertEquals(message, initialMapSize, evictedKeyCount + currentMapSize);
                    // current map size should approximately be around (nodeCount - 1) * perNodeMaxSize.
                    assertTrue(message, ((nodeCount - 1) * perNodeMaxSize)
                            + (PARTITION_COUNT / (nodeCount - 1)) >= currentMapSize);
                }
            }

            // check also backup entry count is around perNodeMaxSize.
            IMap<Object, Object> map1 = nodes.get(1).getMap(mapName);
            IMap<Object, Object> map2 = nodes.get(2).getMap(mapName);
            long totalBackupEntryCount = getTotalBackupEntryCount(map1, map2);
            assertTrue("totalBackupEntryCount=" + totalBackupEntryCount, ((nodeCount - 1) * perNodeMaxSize)
                    + (PARTITION_COUNT / (nodeCount - 1)) >= totalBackupEntryCount);
        });
    }

    private static long getTotalBackupEntryCount(IMap... maps) {
        long total = 0;
        for (IMap map : maps) {
            total += map.getLocalMapStats().getBackupEntryCount();
        }
        return total;
    }

    /**
     * Eviction starts if a partitions' size exceeds this number:
     *
     * double maxPartitionSize = 1D * nodeCount * perNodeMaxSize / PARTITION_COUNT;
     *
     * when calculated `maxPartitionSize` is under 1, we should forcibly set it
     * to 1, otherwise all puts will immediately be removed by eviction.
     */
    @Test
    public void testPerNodePolicy_does_not_cause_unexpected_eviction_when_translated_partition_size_under_one() {
        String mapName = randomMapName();
        int maxSize = PARTITION_COUNT / 2;
        Config config = createConfig(PER_NODE, maxSize, mapName);
        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap(mapName);

        for (int i = 0; i < PARTITION_COUNT; i++) {
            String keyForPartition = generateKeyForPartition(node, i);
            map.put(keyForPartition, i);
            assertEquals(i, map.get(keyForPartition));
        }
    }

    @Test
    public void testOwnerAndBackupEntryCountsAreEqualAfterEviction_whenPerNodeMaxSizePolicyIsUsed() throws Exception {
        String mapName = randomMapName();
        Config config = createConfig(PER_NODE, 300, mapName);
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance node1 = instanceFactory.newHazelcastInstance(config);
        HazelcastInstance node2 = instanceFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map1 = node1.getMap(mapName);
        for (int i = 0; i < 2222; i++) {
            map1.put(i, i);
        }

        IMap map2 = node2.getMap(mapName);
        LocalMapStats localMapStats1 = map1.getLocalMapStats();
        LocalMapStats localMapStats2 = map2.getLocalMapStats();

        assertEqualsEventually(() -> localMapStats2.getBackupEntryCount(), localMapStats1.getOwnedEntryCount());
        assertEqualsEventually(() -> localMapStats1.getBackupEntryCount(), localMapStats2.getOwnedEntryCount());
    }

    @Test
    public void testPerPartitionPolicy() {
        final int perPartitionMaxSize = 1;
        final int nodeCount = 1;
        final String mapName = randomMapName();
        final Config config = createConfig(PER_PARTITION, perPartitionMaxSize, mapName);
        final Collection<IMap> maps = createMaps(mapName, config, nodeCount);
        populateMaps(maps, 1000);

        assertPerPartitionPolicyWorks(maps, perPartitionMaxSize);
    }

    @Test
    public void testUsedHeapSizePolicy() {
        final int perNodeHeapMaxSizeInMegaBytes = 10;
        final int nodeCount = 1;
        final String mapName = randomMapName();
        final Config config = createConfig(USED_HEAP_SIZE, perNodeHeapMaxSizeInMegaBytes, mapName);
        final Collection<IMap> maps = createMaps(mapName, config, nodeCount);
        setTestSizeEstimator(maps, MEGABYTES.toBytes(1));
        populateMaps(maps, 100);

        assertUsedHeapSizePolicyWorks(maps, perNodeHeapMaxSizeInMegaBytes);
    }

    @Test
    public void testFreeHeapSizePolicy() {
        final int freeHeapMinSizeInMegaBytes = 10;
        final int nodeCount = 1;
        final String mapName = randomMapName();
        final Config config = createConfig(FREE_HEAP_SIZE, freeHeapMinSizeInMegaBytes, mapName);
        final Collection<IMap> maps = createMaps(mapName, config, nodeCount);

        // make available free heap memory 5MB.
        // availableFree = maxMemoryMB - (totalMemoryMB - freeMemoryMB);
        final int totalMemoryMB = 15;
        final int freeMemoryMB = 0;
        final int maxMemoryMB = 20;

        setMockRuntimeMemoryInfoAccessor(maps, totalMemoryMB, freeMemoryMB, maxMemoryMB);

        populateMaps(maps, 100);

        // expecting map size = 0.
        // Since we are mocking free heap size and
        // it is always below the allowed min free heap size for this test.
        assertUsedFreeHeapPolicyTriggersEviction(maps);
    }

    @Test
    public void testUsedHeapPercentagePolicy() {
        final int maxUsedHeapPercentage = 49;
        final int nodeCount = 1;
        final int putCount = 1;
        final String mapName = randomMapName();
        final Config config = createConfig(USED_HEAP_PERCENTAGE, maxUsedHeapPercentage, mapName);
        final Collection<IMap> maps = createMaps(mapName, config, nodeCount);

        // here order of `setTestMaxRuntimeMemoryInMegaBytes` and `setTestSizeEstimator`
        // should not be changed.
        setTestMaxRuntimeMemoryInMegaBytes(maps, 40);

        // object can be key or value, so heap-cost of a record = keyCost + valueCost
        final long oneObjectHeapCostInMegaBytes = 10;
        setTestSizeEstimator(maps, MEGABYTES.toBytes(oneObjectHeapCostInMegaBytes));

        populateMaps(maps, putCount);

        assertUsedHeapPercentagePolicyTriggersEviction(maps, putCount);
    }

    private void setTestMaxRuntimeMemoryInMegaBytes(Collection<IMap> maps, int maxMemoryMB) {
        setMockRuntimeMemoryInfoAccessor(maps, -1, -1, maxMemoryMB);
    }

    @Test
    public void testFreeHeapPercentagePolicy() {
        final int minFreeHeapPercentage = 51;
        final int nodeCount = 1;
        final int putCount = 1000;
        final String mapName = randomMapName();
        final Config config = createConfig(FREE_HEAP_PERCENTAGE, minFreeHeapPercentage, mapName);
        final Collection<IMap> maps = createMaps(mapName, config, nodeCount);

        // make available free heap memory 20MB.
        // availableFree = maxMemoryMB - (totalMemoryMB - freeMemoryMB);
        final int totalMemoryMB = 25;
        final int freeMemoryMB = 5;
        final int maxMemoryMB = 40;

        setMockRuntimeMemoryInfoAccessor(maps, totalMemoryMB, freeMemoryMB, maxMemoryMB);

        populateMaps(maps, putCount);

        // expecting map size = 0.
        // Since we are mocking free heap size and
        // it is always below the allowed min free heap size for this test.
        assertUsedFreeHeapPolicyTriggersEviction(maps);
    }

    void testPerNodePolicy(int nodeCount) {
        final int perNodeMaxSize = 300;
        final String mapName = randomMapName();
        final Config config = createConfig(PER_NODE, perNodeMaxSize, mapName);
        final Collection<IMap> maps = createMaps(mapName, config, nodeCount);
        populateMaps(maps, 1000);

        assertPerNodePolicyWorks(maps, perNodeMaxSize, nodeCount);
    }

    void setTestSizeEstimator(Collection<IMap> maps, long oneEntryHeapCostInMegaBytes) {
        for (IMap map : maps) {
            setTestSizeEstimator(map, oneEntryHeapCostInMegaBytes);
        }
    }

    public static void setTestSizeEstimator(IMap map, final long oneEntryHeapCostInBytes) {
        final MapProxyImpl mapProxy = (MapProxyImpl) map;
        final MapService mapService = (MapService) mapProxy.getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            final Address owner = partitionService.getPartitionOwner(i);
            if (nodeEngine.getThisAddress().equals(owner)) {
                final PartitionContainer container = mapServiceContext.getPartitionContainer(i);
                if (container == null) {
                    continue;
                }
                final RecordStore recordStore = container.getRecordStore(map.getName());
                final DefaultRecordStore defaultRecordStore = (DefaultRecordStore) recordStore;
                defaultRecordStore.setSizeEstimator(new EntryCostEstimator() {

                    long size;

                    @Override
                    public long getEstimate() {
                        return size;
                    }

                    @Override
                    public void adjustEstimateBy(long size) {
                        this.size += size;
                    }

                    @Override
                    public long calculateValueCost(Object record) {
                        if (record == null) {
                            return 0L;
                        }
                        return oneEntryHeapCostInBytes;
                    }

                    @Override
                    public long calculateEntryCost(Object key, Object record) {
                        if (record == null) {
                            return 0L;
                        }
                        return 2 * oneEntryHeapCostInBytes;
                    }

                    @Override
                    public void reset() {
                        size = 0;
                    }
                });
            }
        }
    }

    public static void setMockRuntimeMemoryInfoAccessor(IMap map, final long totalMemoryMB, final long freeMemoryMB,
                                                        final long maxMemoryMB) {
        final MapProxyImpl mapProxy = (MapProxyImpl) map;
        final MapService mapService = (MapService) mapProxy.getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        MemoryInfoAccessor memoryInfoAccessor = new MemoryInfoAccessor() {
            @Override
            public long getTotalMemory() {
                return MEGABYTES.toBytes(totalMemoryMB);
            }

            @Override
            public long getFreeMemory() {
                return MEGABYTES.toBytes(freeMemoryMB);
            }

            @Override
            public long getMaxMemory() {
                return MEGABYTES.toBytes(maxMemoryMB);
            }
        };

        MapContainer mapContainer = mapServiceContext.getMapContainer(map.getName());
        EvictionChecker evictionChecker = new EvictionChecker(memoryInfoAccessor, mapServiceContext);
        IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        Evictor evictor = new TestEvictor(LRUEvictionPolicyComparator.INSTANCE, evictionChecker, partitionService);
        mapContainer.setEvictor(evictor);
    }

    private static final class TestEvictor extends EvictorImpl {

        TestEvictor(EvictionPolicyComparator evictionPolicyComparator,
                    EvictionChecker evictionChecker,
                    IPartitionService partitionService) {
            super(evictionPolicyComparator, evictionChecker, 1, partitionService);
        }

        @Override
        public boolean checkEvictable(RecordStore recordStore) {
            return evictionChecker.checkEvictable(recordStore);
        }
    }

    void setMockRuntimeMemoryInfoAccessor(Collection<IMap> maps, final long totalMemory,
                                          final long freeMemory, final long maxMemory) {
        for (IMap map : maps) {
            setMockRuntimeMemoryInfoAccessor(map, totalMemory, freeMemory, maxMemory);
        }
    }

    void populateMaps(Collection<IMap> maps, int putOperationCount) {
        final IMap<Integer, Integer> map = maps.iterator().next();
        for (int i = 0; i < putOperationCount; i++) {
            map.put(i, i);
        }
    }

    Collection<IMap> createMaps(String mapName, Config config, int nodeCount) {
        final List<IMap> maps = new ArrayList<IMap>();
        Collection<HazelcastInstance> nodes = createNodes(nodeCount, config);
        for (HazelcastInstance node : nodes) {
            final IMap map = node.getMap(mapName);
            maps.add(map);
        }
        return maps;
    }

    Config createConfig(MaxSizePolicy maxSizePolicy, int maxSize, String mapName) {
        Config config = getConfig();
        config.getMetricsConfig().setEnabled(false);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        config.setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_MIGRATIONS.getName(), String.valueOf(PARTITION_MAX_PARALLEL_MIGRATIONS));

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setBackupCount(1);
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
        evictionConfig.setMaxSizePolicy(maxSizePolicy);
        evictionConfig.setSize(maxSize);

        return config;
    }

    List<HazelcastInstance> createNodes(int nodeCount, Config config) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        factory.newInstances(config);
        return new ArrayList<>(factory.getAllHazelcastInstances());
    }

    int getSize(Collection<IMap> maps) {
        if (maps == null || maps.isEmpty()) {
            throw new IllegalArgumentException("No IMap found.");
        }
        final IMap map = maps.iterator().next();
        return map.size();
    }


    long getHeapCost(Collection<IMap> maps) {
        if (maps == null || maps.isEmpty()) {
            throw new IllegalArgumentException("No IMap found.");
        }
        long heapCost = 0L;
        for (IMap map : maps) {
            heapCost += map.getLocalMapStats().getHeapCost();
        }
        return heapCost;
    }

    void assertPerNodePolicyWorks(final Collection<IMap> maps, final int perNodeMaxSize, final int nodeCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final int mapSize = getSize(maps);
                final String message = format("map size is %d and it should be smaller "
                                + "than perNodeMaxSize * nodeCount which is %d",
                        mapSize, perNodeMaxSize * nodeCount);

                assertTrue(message, mapSize <= perNodeMaxSize * nodeCount);
            }
        });
    }

    void assertPerPartitionPolicyWorks(final Collection<IMap> maps, final int perPartitionMaxSize) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final int mapSize = getSize(maps);
                final String message = format("map size is %d and it should be smaller "
                                + "than perPartitionMaxSize * PARTITION_COUNT which is %d",
                        mapSize, perPartitionMaxSize * PARTITION_COUNT);

                assertTrue(message, mapSize <= perPartitionMaxSize * PARTITION_COUNT);
            }
        });
    }

    void assertUsedHeapSizePolicyWorks(final Collection<IMap> maps, final int maxSizeInMegaBytes) {
        assertTrueEventually(new AssertTask() {

            @Override
            public void run() throws Exception {
                final long heapCost = getHeapCost(maps);
                final String message = format("heap cost is %d and it should be smaller "
                                + "than allowed max heap size %d in bytes",
                        heapCost, MEGABYTES.toBytes(maxSizeInMegaBytes));

                assertTrue(message, heapCost <= MEGABYTES.toBytes(maxSizeInMegaBytes));
            }
        });
    }

    void assertUsedHeapPercentagePolicyTriggersEviction(final Collection<IMap> maps, final int putCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final int size = getSize(maps);
                final long heapCost = getHeapCost(maps);
                final String message = format("map size is %d, heap cost is %d in "
                                + "bytes but total memory is %d in bytes",
                        size, heapCost, Runtime.getRuntime().totalMemory());

                assertTrue(message, size < putCount);
            }
        });
    }

    void assertUsedFreeHeapPolicyTriggersEviction(final Collection<IMap> maps) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final int size = getSize(maps);
                final String message = format("map size is %d", size);

                assertEquals(message, 0, size);
            }
        });
    }
}
