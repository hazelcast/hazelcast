package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static com.hazelcast.test.TimeConstants.MINUTE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapLoaderMultiNodeTest extends HazelcastTestSupport {

    protected static final int MAP_STORE_ENTRY_COUNT = 10000;
    protected static final int BATCH_SIZE = 100;
    protected static final int NODE_COUNT = 3;

    protected TestHazelcastInstanceFactory nodeFactory;
    protected CountingMapLoader mapLoader;
    protected final String mapName = getClass().getSimpleName();

    @Before
    public void setUp() throws Exception {
        nodeFactory = createHazelcastInstanceFactory(NODE_COUNT + 2);
        mapLoader = new CountingMapLoader(MAP_STORE_ENTRY_COUNT);
    }

    @Test(timeout = MINUTE)
    public void testLoads_whenMapLazyAndCheckingSize() throws Exception {
        Config cfg = newConfig(mapName, LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertSizeAndLoadCount(map);
    }

    @Test(timeout = MINUTE)
    public void testLoadsAll_whenMapCreatedInEager() throws Exception {
        Config cfg = newConfig(mapName, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertSizeAndLoadCount(map);
    }

    @Test(timeout = MINUTE)
    public void testLoadsNothing_whenMapCreatedLazy() throws Exception {
        Config cfg = newConfig(mapName, InitialLoadMode.LAZY);

        getMap(mapName, cfg);

        assertEquals(0, mapLoader.getLoadedValueCount());
    }

    @Test(timeout = MINUTE)
    public void testLoadsMap_whenLazyAndValueRetrieved() throws Exception {
        Config cfg = newConfig(mapName, InitialLoadMode.LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertEquals(1, map.get(1));
        assertSizeAndLoadCount(map);
    }

    @Test(timeout = MINUTE)
    public void testLoadsAll_whenLazyModeAndLoadAll() throws Exception {
        Config cfg = newConfig(mapName, LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);
        map.loadAll(true);

        assertEquals(1, mapLoader.getLoadAllKeysInvocations());
        assertSizeAndLoadCount(map);
    }

    @Test(timeout = MINUTE)
    public void testDoesNotLoadAgain_whenLoadedAndNodeAdded() throws Exception {
        Config cfg = newConfig(mapName, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);
        nodeFactory.newHazelcastInstance(cfg);

        assertEquals(1, mapLoader.getLoadAllKeysInvocations());
        assertSizeAndLoadCount(map);
    }

    @Test(timeout = MINUTE)
    public void testDoesNotLoadAgain_whenLoadedLazyAndNodeAdded() throws Exception {
        Config cfg = newConfig(mapName, LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);
        map.loadAll(true);
        nodeFactory.newHazelcastInstance(cfg);

        assertEquals(1, mapLoader.getLoadAllKeysInvocations());
        assertSizeAndLoadCount(map);
    }

    @Test(timeout = MINUTE)
    public void testLoadAgain_whenLoadedAllCalledMultipleTimes() throws Exception {
        Config cfg = newConfig(mapName, LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);
        map.loadAll(true);
        map.loadAll(true);

        assertEquals(2, mapLoader.getLoadAllKeysInvocations());
        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertEquals(2 * MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    @Test(timeout = MINUTE)
    public void testLoadsOnce_whenSizeCheckedTwice() throws Exception {

        mapLoader = new CountingMapLoader(MAP_STORE_ENTRY_COUNT, true);
        Config cfg = newConfig(mapName, LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);
        map.size();
        map.size();

        assertEquals(1, mapLoader.getLoadAllKeysInvocations());
        assertSizeAndLoadCount(map);
    }

    protected void assertSizeAndLoadCount(IMap<Object, Object> map) {
        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertEquals(MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    protected IMap<Object, Object> getMap(final String mapName, Config cfg) {
        HazelcastInstance hz = nodeFactory.newInstances(cfg, NODE_COUNT)[0];
        assertClusterSizeEventually(NODE_COUNT, hz);
        IMap<Object, Object> map = hz.getMap(mapName);
        waitClusterForSafeState(hz);
        return map;
    }

    protected Config newConfig(String mapName, MapStoreConfig.InitialLoadMode loadMode) {
        return newConfig(mapName, loadMode, 1, mapLoader);
    }

    protected Config newConfig(String mapName, MapStoreConfig.InitialLoadMode loadMode, int backups, MapLoader loader) {
        Config cfg = getConfig();
        cfg.setGroupConfig(new GroupConfig(getClass().getSimpleName()));
        cfg.setProperty(GroupProperty.MAP_LOAD_CHUNK_SIZE.getName(), Integer.toString(BATCH_SIZE));
        cfg.setProperty(GroupProperty.PARTITION_COUNT.getName(), "31");

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setImplementation(loader).setInitialLoadMode(loadMode);

        cfg.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig).setBackupCount(backups);

        return cfg;
    }
}
