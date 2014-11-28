package com.hazelcast.map.mapstore;


import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
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

import static com.hazelcast.test.TimeConstants.MINUTE;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapStoreEvictionTest extends HazelcastTestSupport {

    private int mapStoreEntryCount = 1000;
    private int nodes = 2;
    private int maxSizePerNode = mapStoreEntryCount / 4;
    private int maxSizePerCluster = maxSizePerNode * nodes;
    private AtomicInteger loadedValueCount = new AtomicInteger();
    private TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(nodes);

    @Test(timeout = 2 * MINUTE)
    public void testLoadsAll_whenEvictionDisabled() throws Exception {
        Config cfg = newConfig("map", false);

        HazelcastInstance hz = nodeFactory.newInstances(cfg)[0];
        assertClusterSizeEventually(nodes, hz);
        IMap<Object, Object> map = hz.getMap("map");
        waitClusterForSafeState(hz);

        assertEquals(mapStoreEntryCount, map.size());
        assertEquals(mapStoreEntryCount, loadedValueCount.get());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsLessThanMaxSize_whenEvictionEnabled() throws Exception {
        Config cfg = newConfig("map", true);

        HazelcastInstance hz = nodeFactory.newInstances(cfg)[0];
        assertClusterSizeEventually(nodes, hz);
        IMap<Object, Object> map = hz.getMap("map");
        waitAllForSafeState();

        assertFalse(map.isEmpty());
        assertTrue(maxSizePerCluster >= map.size());
        assertTrue(maxSizePerCluster >= loadedValueCount.get());
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
            mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
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
