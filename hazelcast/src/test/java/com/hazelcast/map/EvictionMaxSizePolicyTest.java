package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.proxy.MapProxyImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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
    public void testPerPartitionPolicy() {
        final int perPartitionMaxSize = 1;
        final int nodeCount = 1;
        final String mapName = randomMapName();
        final Config config = createConfig(MaxSizeConfig.MaxSizePolicy.PER_PARTITION, perPartitionMaxSize, mapName);
        final Collection<IMap> maps = createMaps(mapName, config, nodeCount);
        testMaxSizePolicy(maps, 1000);

        assertPerPartitionPolicyWorks(maps, perPartitionMaxSize);

    }

    @Test
    public void testUsedHeapSizePolicy() {
        final int perNodeHeapMaxSizeInMegaBytes = 1;
        final int nodeCount = 1;
        final String mapName = randomMapName();
        final Config config = createConfig(MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE, perNodeHeapMaxSizeInMegaBytes, mapName);
        final Collection<IMap> maps = createMaps(mapName, config, nodeCount);
        setTestSizeEstimator(maps, 1);
        testMaxSizePolicy(maps, 100);

        assertUsedHeapSizePolicyWorks(maps, perNodeHeapMaxSizeInMegaBytes);
    }


    @Test
    public void testUsedHeapPercentagePolicy() {
        final int maxUsedHeapPercentage = 60;
        final int nodeCount = 1;
        final int putCount = 1000;
        final String mapName = randomMapName();
        final Config config = createConfig(MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE, maxUsedHeapPercentage, mapName);
        final Collection<IMap> maps = createMaps(mapName, config, nodeCount);
        // pick an enormously big heap cost for an entry for testing.
        final long oneEntryHeapCostInMegaBytes = 1 << 30;
        setTestSizeEstimator(maps, oneEntryHeapCostInMegaBytes);
        testMaxSizePolicy(maps, putCount);

        assertUsedHeapPercentagePolicyTriggersEviction(maps, putCount);
    }

    private void testPerNodePolicy(int nodeCount) {
        final int perNodeMaxSize = 100;
        final String mapName = randomMapName();
        final Config config = createConfig(MaxSizeConfig.MaxSizePolicy.PER_NODE, perNodeMaxSize, mapName);
        final Collection<IMap> maps = createMaps(mapName, config, nodeCount);
        testMaxSizePolicy(maps, 1000);

        assertPerNodePolicyWorks(maps, perNodeMaxSize, nodeCount);
    }


    private void setTestSizeEstimator(Collection<IMap> maps, long oneEntryHeapCostInMegaBytes) {
        for (IMap map : maps) {
            setTestSizeEstimator(map, oneEntryHeapCostInMegaBytes);
        }
    }

    private void setTestSizeEstimator(IMap map, final long oneEntryHeapCostInMegaBytes) {
        final MapProxyImpl mapProxy = (MapProxyImpl) map;
        final MapService mapService = (MapService) mapProxy.getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            final Address owner = partitionService.getPartitionOwner(i);
            if (nodeEngine.getThisAddress().equals(owner)) {
                final PartitionContainer container = mapServiceContext.getPartitionContainer(i);
                if (container == null) {
                    continue;
                }
                final RecordStore recordStore = container.getRecordStore(map.getName());
                final DefaultRecordStore defaultRecordStore = (DefaultRecordStore) recordStore;
                defaultRecordStore.setSizeEstimator(new SizeEstimator() {

                    long size;

                    @Override
                    public long getSize() {
                        return size;
                    }

                    @Override
                    public void add(long size) {
                        this.size += size;
                    }

                    @Override
                    public long getCost(Object record) {
                        return convertMegaBytesToBytes(oneEntryHeapCostInMegaBytes);
                    }

                    @Override
                    public void reset() {
                        size = 0;
                    }
                });
            }
        }

    }

    public void testMaxSizePolicy(Collection<IMap> maps, int putOperationCount) {
        final IMap<Integer, Integer> map = maps.iterator().next();
        for (int i = 0; i < putOperationCount; i++) {
            map.put(i, i);
        }
    }

    private Collection<IMap> createMaps(String mapName, Config config, int nodeCount) {
        final List<IMap> maps = new ArrayList<IMap>();
        final HazelcastInstance[] nodes = createNodes(nodeCount, config);
        for (HazelcastInstance node : nodes) {
            final IMap map = node.getMap(mapName);
            maps.add(map);
        }
        return maps;
    }

    private Config createConfig(MaxSizeConfig.MaxSizePolicy maxSizePolicy, int maxSize, String mapName) {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(PARTITION_COUNT));

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mapConfig.setEvictionPercentage(25);

        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(maxSizePolicy);
        msc.setSize(maxSize);

        mapConfig.setMaxSizeConfig(msc);
        mapConfig.setMinEvictionCheckMillis(0L);
        return config;
    }

    private HazelcastInstance[] createNodes(int nodeCount, Config config) {
        return createHazelcastInstanceFactory(nodeCount).newInstances(config);
    }

    private long convertMegaBytesToBytes(long mb) {
        return mb * 1024 * 1024;
    }

    private int getSize(Collection<IMap> maps) {
        if (maps == null || maps.isEmpty()) {
            throw new IllegalArgumentException("No IMap found.");
        }
        final IMap map = maps.iterator().next();
        return map.size();
    }


    private long getHeapCost(Collection<IMap> maps) {
        if (maps == null || maps.isEmpty()) {
            throw new IllegalArgumentException("No IMap found.");
        }
        long heapCost = 0L;
        for (IMap map : maps) {
            heapCost += map.getLocalMapStats().getHeapCost();
        }
        return heapCost;
    }


    private void assertPerNodePolicyWorks(Collection<IMap> maps, int perNodeMaxSize, int nodeCount) {
        final int mapSize = getSize(maps);
        final String message = String.format("map size is %d and it should be smaller "
                        + "than perNodeMaxSize * nodeCount which is %d",
                mapSize, perNodeMaxSize * nodeCount);

        assertTrue(message, mapSize <= perNodeMaxSize * nodeCount);
    }


    private void assertPerPartitionPolicyWorks(Collection<IMap> maps, int perPartitionMaxSize) {
        final int mapSize = getSize(maps);
        final String message = String.format("map size is %d and it should be smaller "
                        + "than perPartitionMaxSize * PARTITION_COUNT which is %d",
                mapSize, perPartitionMaxSize * PARTITION_COUNT);

        assertTrue(message, mapSize <= perPartitionMaxSize * PARTITION_COUNT);
    }

    private void assertUsedHeapSizePolicyWorks(Collection<IMap> maps, int maxSizeInMegaBytes) {
        final long heapCost = getHeapCost(maps);
        final String message = String.format("heap cost is %d and it should be smaller "
                        + "than allowed max heap size %d in bytes",
                heapCost, convertMegaBytesToBytes(maxSizeInMegaBytes));

        assertTrue(message, heapCost <= convertMegaBytesToBytes(maxSizeInMegaBytes));
    }

    private void assertUsedHeapPercentagePolicyTriggersEviction(Collection<IMap> maps, int putCount) {
        final int size = getSize(maps);
        final long heapCost = getHeapCost(maps);
        final String message = String.format("map size is %d, heap cost is %d in "
                        + "bytes but total memory is %d in bytes",
                size, heapCost, Runtime.getRuntime().totalMemory());

        assertTrue(message, size < putCount);
    }


}
