package com.hazelcast.map.mapstore;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapStoreEvictionTest extends HazelcastTestSupport {

    private int mapStoreEntryCount = 10 * 1000;
    private int nodes = 2;
    private int maxSizePerNode = mapStoreEntryCount / 4;
    private int maxSizePerCluster = maxSizePerNode * nodes;
    private AtomicInteger loadedValueCount = new AtomicInteger();
    private TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(nodes);

    @Test(timeout = 120000)
    public void testLoadsAll_whenEvictionDisabled() throws Exception {
        Config cfg = newConfig("map", false);

        HazelcastInstance instances[] = nodeFactory.newInstances(cfg);
        assertClusterSizeEventually(nodes, instances[0]);
        IMap<Object, Object> map = instances[0].getMap("map");

        assertEquals(mapStoreEntryCount, loadedValueCount.get());
        assertEquals(mapStoreEntryCount, map.size());
    }

    @Test(timeout = 120000)
    public void testLoadsLessThanMaxSize_whenEvictionEnabled() throws Exception {
        Config cfg = newConfig("map", true);

        HazelcastInstance instances[] = nodeFactory.newInstances(cfg);
        assertClusterSizeEventually(nodes, instances[0]);
        IMap<Object, Object> map = instances[0].getMap("map");

        assertEquals(maxSizePerCluster, loadedValueCount.get());
        assertTrue(maxSizePerCluster >= map.size());
    }

    private Config newConfig(String mapName, boolean sizeLimited) {
        Config cfg = new Config();

        cfg.setGroupConfig(new GroupConfig(getClass().getSimpleName()));

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
        .setImplementation( new CountingMapLoader(mapStoreEntryCount, loadedValueCount) )
        .setInitialLoadMode( MapStoreConfig.InitialLoadMode.EAGER );

        MapConfig mapConfig = cfg.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        if( sizeLimited ) {
            MaxSizeConfig maxSizeConfig = new MaxSizeConfig(maxSizePerNode, MaxSizeConfig.MaxSizePolicy.PER_NODE);
            mapConfig.setMaxSizeConfig(maxSizeConfig);
        }

        return cfg;
    }

    private static class CountingMapLoader extends SimpleMapLoader {

        private AtomicInteger loadedValueCount;

        CountingMapLoader(int size, AtomicInteger loadedValueCount) {
            super(size, false);
            this.loadedValueCount = loadedValueCount;
        }

        @Override
        public Map loadAll(Collection keys) {
            loadedValueCount.addAndGet(keys.size());
            return super.loadAll(keys);
        }
    }
}
