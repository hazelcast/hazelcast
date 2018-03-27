/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.eviction.MapEvictionPolicy;
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
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.MemoryInfoAccessor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_PARTITION;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EvictionMaxSizePolicyTest extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 271;

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
        int nodeCount = 2;
        int perNodeMaxSize = 1000;

        // eviction takes place if a partitions size exceeds this number
        // see EvictionChecker#translatePerNodeSizeToRecordStoreSize
        double maxPartitionSize = 1D * nodeCount * perNodeMaxSize / PARTITION_COUNT;

        String mapName = "testPerNodePolicy_afterGracefulShutdown";
        Config config = createConfig(PER_NODE, perNodeMaxSize, mapName);

        // populate map from one of the nodes
        Collection<HazelcastInstance> nodes = createNodes(nodeCount, config);
        for (HazelcastInstance node : nodes) {
            IMap map = node.getMap(mapName);
            for (int i = 0; i < 5000; i++) {
                map.put(i, i);
            }

            node.shutdown();
            break;
        }

        for (HazelcastInstance node : nodes) {
            if (node.getLifecycleService().isRunning()) {
                int mapSize = node.getMap(mapName).size();
                String message = format("map size is %d and it should be smaller "
                                + "than maxPartitionSize * PARTITION_COUNT which is %.0f",
                        mapSize,  maxPartitionSize * PARTITION_COUNT);

                System.err.println(message);

                assertTrue(message, mapSize <= maxPartitionSize * PARTITION_COUNT);
            }
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

        assertEquals(localMapStats1.getOwnedEntryCount(), localMapStats2.getBackupEntryCount());
        assertEquals(localMapStats2.getOwnedEntryCount(), localMapStats1.getBackupEntryCount());
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

    static public void setTestSizeEstimator(IMap map, final long oneEntryHeapCostInBytes) {
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

        MapEvictionPolicy mapEvictionPolicy = mapContainer.getMapConfig().getMapEvictionPolicy();
        EvictionChecker evictionChecker = new EvictionChecker(memoryInfoAccessor, mapServiceContext);
        IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        Evictor evictor = new TestEvictor(mapEvictionPolicy, evictionChecker, partitionService);
        mapContainer.setEvictor(evictor);
    }

    private static final class TestEvictor extends EvictorImpl {

        TestEvictor(MapEvictionPolicy mapEvictionPolicy, EvictionChecker evictionChecker, IPartitionService partitionService) {
            super(mapEvictionPolicy, evictionChecker, partitionService);
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

    Config createConfig(MaxSizeConfig.MaxSizePolicy maxSizePolicy, int maxSize, String mapName) {
        Config config = getConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));

        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(maxSizePolicy);
        msc.setSize(maxSize);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setEvictionPercentage(25);
        mapConfig.setMaxSizeConfig(msc);
        mapConfig.setMinEvictionCheckMillis(0L);

        return config;
    }

    Collection<HazelcastInstance> createNodes(int nodeCount, Config config) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        factory.newInstances(config);
        return factory.getAllHazelcastInstances();
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
