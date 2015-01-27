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

import static com.hazelcast.test.TimeConstants.MINUTE;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapStoreEvictionTest extends HazelcastTestSupport {

    private static final int MAP_STORE_ENTRY_COUNT = 1000;
    private static final int NODE_COUNT = 2;
    private static final int MAX_SIZE_PER_NODE = MAP_STORE_ENTRY_COUNT / 4;
    private static final int MAX_SIZE_PER_CLUSTER = MAX_SIZE_PER_NODE * NODE_COUNT;

    private CountingMapLoader loader;
    private TestHazelcastInstanceFactory nodeFactory;

    @Before
    public void setUp() throws Exception {
        loader = new CountingMapLoader(MAP_STORE_ENTRY_COUNT);
        nodeFactory = createHazelcastInstanceFactory(NODE_COUNT);
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsAll_whenEvictionDisabled() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertEquals(MAP_STORE_ENTRY_COUNT, loader.getLoadedValueCount());
        assertTrue(loader.isLoadAllKeysClosed());
    }

    private IMap<Object, Object> getMap(final String mapName, Config cfg) {
        HazelcastInstance hz = nodeFactory.newInstances(cfg)[0];
        assertClusterSizeEventually(NODE_COUNT, hz);
        IMap<Object, Object> map = hz.getMap(mapName);
        waitClusterForSafeState(hz);

        return map;
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsLessThanMaxSize_whenEvictionEnabled() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, true);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertFalse(map.isEmpty());
        assertTrue(MAX_SIZE_PER_CLUSTER >= map.size());
        assertTrue(MAX_SIZE_PER_CLUSTER >= loader.getLoadedValueCount());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsLessThanMaxSize_whenEvictionEnabledAndReloaded() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, true);

        IMap<Object, Object> map = getMap(mapName, cfg);
        map.evictAll();
        loader.reset();
        map.loadAll(true);

        assertFalse(map.isEmpty());
        assertTrue(MAX_SIZE_PER_CLUSTER >= map.size());
        assertTrue(MAX_SIZE_PER_CLUSTER >= loader.getLoadedValueCount());
        assertTrue(loader.isLoadAllKeysClosed());
    }

    private Config newConfig(String mapName, boolean sizeLimited) {
        Config cfg = new Config();
        cfg.setGroupConfig(new GroupConfig(getClass().getSimpleName()));

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setImplementation(loader)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);

        MapConfig mapConfig = cfg.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        if (sizeLimited) {
            MaxSizeConfig maxSizeConfig = new MaxSizeConfig(MAX_SIZE_PER_NODE, MaxSizeConfig.MaxSizePolicy.PER_NODE);
            mapConfig.setMaxSizeConfig(maxSizeConfig);
            mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        }

        return cfg;
    }

}
