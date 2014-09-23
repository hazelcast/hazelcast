package com.hazelcast.map.mapstore;

import static java.lang.Math.max;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapStoreEvictionTest extends HazelcastTestSupport {

    private int entryCount = 10 * 1000;
    private int nodes = 2;
    private int maxSizePerNode = entryCount / 4;
    private int maxSizePerCluster = maxSizePerNode * nodes;

    @Test(timeout = 120000)
    public void testLoadsLessThanMaxSize_whenEvictionEnabled() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = newConfigWithEviction(entryCount, maxSizePerNode, "map");

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);
        assertClusterSizeEventually(nodes, instance1);

        IMap map = instance1.getMap("map");

        assertEquals(maxSizePerCluster, max(maxSizePerCluster, map.size()));
    }

    private static Config newConfigWithEviction(int size, int maxSize, String mapName) {
        Config cfg = new Config();

        cfg.setGroupConfig(new GroupConfig(MapStoreEvictionTest.class.getSimpleName()));

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
        .setEnabled(true)
        .setImplementation( new SimpleMapLoader(size, false) )
        .setInitialLoadMode( MapStoreConfig.InitialLoadMode.EAGER );

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig(maxSize, MaxSizeConfig.MaxSizePolicy.PER_NODE);

        cfg.getMapConfig(mapName)
        .setMapStoreConfig(mapStoreConfig)
        .setMaxSizeConfig(maxSizeConfig)
        .setEvictionPercentage(10)
        .setEvictionPolicy(MapConfig.EvictionPolicy.LRU)
        .setMergePolicy(LatestUpdateMapMergePolicy.class.getName());

        return cfg;
    }

}
