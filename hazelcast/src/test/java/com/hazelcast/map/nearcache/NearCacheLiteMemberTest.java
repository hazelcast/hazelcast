package com.hazelcast.map.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheLiteMemberTest {

    private TestHazelcastInstanceFactory factory;

    private String mapName;

    private HazelcastInstance instance;

    private HazelcastInstance lite;

    @Before
    public void init() {
        factory = new TestHazelcastInstanceFactory(2);
        mapName = randomMapName();
        instance = factory.newHazelcastInstance(createConfig(mapName, false));
        lite = factory.newHazelcastInstance(createConfig(mapName, true));
    }

    @After
    public void destroy() {
        factory.terminateAll();
    }

    @Test
    public void testPut() {
        testPut(instance, lite, mapName);
    }

    @Test
    public void testPutAll() {
        testPutAll(instance, lite, mapName);
    }

    @Test
    public void testPutTransient() {
        testPutTransient(instance, lite, mapName);
    }

    @Test
    public void testSet() {
        testSet(instance, lite, mapName);
    }

    @Test
    public void testUpdate() {
        testUpdate(instance, lite, mapName);
    }

    @Test
    public void testUpdateWithSet() {
        testUpdateWithSet(instance, lite, mapName);
    }

    @Test
    public void testUpdateWithPutAll() {
        testUpdateWithPutAll(instance, lite, mapName);
    }

    @Test
    public void testReplace() {
        testReplace(instance, lite, mapName);
    }

    @Test
    public void testEvict() {
        testEvict(instance, lite, mapName);
    }

    @Test
    public void testRemove() {
        testRemove(instance, lite, mapName);
    }

    @Test
    public void testDelete() {
        testDelete(instance, lite, mapName);
    }

    @Test
    public void testClear() {
        testClear(instance, lite, mapName);
    }

    @Test
    public void testEvictAll() {
        testEvictAll(instance, lite, mapName);
    }

    @Test
    public void testExecuteOnKey() {
        testExecuteOnKey(instance, lite, mapName);
    }

    @Test
    public void testExecuteOnKeys() {
        testExecuteOnKeys(instance, lite, mapName);
    }

    public static void testPut(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);

        int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        for (int i = 0; i < count; i++) {
            liteMap.get(i);
        }

        assertLiteMemberNearCacheNonEmpty(lite, mapName);
    }

    public static void testPutAll(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);

        Map<Object, Object> localMap = new HashMap<Object, Object>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            localMap.put(i, i);
        }

        map.putAll(localMap);

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        for (int i = 0; i < count; i++) {
            liteMap.get(i);
        }

        assertLiteMemberNearCacheNonEmpty(lite, mapName);
    }

    public static void testPutTransient(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);

        int count = 100;
        for (int i = 0; i < count; i++) {
            map.putTransient(i, i, 0, MILLISECONDS);
        }

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        for (int i = 0; i < count; i++) {
            liteMap.get(i);
        }

        assertLiteMemberNearCacheNonEmpty(lite, mapName);
    }

    public static void testSet(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);

        int count = 100;
        for (int i = 0; i < count; i++) {
            map.set(i, i);
        }

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        for (int i = 0; i < count; i++) {
            liteMap.get(i);
        }

        assertLiteMemberNearCacheNonEmpty(lite, mapName);
    }

    public static void testUpdate(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);
        map.put(1, 1);

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        liteMap.get(1);

        map.put(1, 2);

        assertNullNearCacheEntryEventually(lite, mapName, 1);
    }

    public static void testUpdateWithSet(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);
        map.put(1, 1);

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        liteMap.get(1);

        map.set(1, 2);

        assertNullNearCacheEntryEventually(lite, mapName, 1);
    }

    public static void testUpdateWithPutAll(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);
        map.put(1, 1);

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        liteMap.get(1);

        Map<Object, Object> localMap = new HashMap<Object, Object>();
        localMap.put(1, 2);
        map.putAll(localMap);

        assertNullNearCacheEntryEventually(lite, mapName, 1);
    }

    public static void testReplace(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);
        map.put(1, 1);

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        liteMap.get(1);

        map.replace(1, 2);

        assertNullNearCacheEntryEventually(lite, mapName, 1);
    }

    public static void testEvict(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);
        map.put(1, 1);

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        liteMap.get(1);

        map.evict(1);

        assertNearCacheIsEmptyEventually(lite, mapName);
    }

    public static void testRemove(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);
        map.put(1, 1);

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        liteMap.get(1);

        map.remove(1);

        assertNearCacheIsEmptyEventually(lite, mapName);
    }

    public static void testDelete(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);
        map.put(1, 1);

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        liteMap.get(1);

        map.delete(1);

        assertNearCacheIsEmptyEventually(lite, mapName);
    }

    public static void testClear(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);

        int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        for (int i = 0; i < count; i++) {
            liteMap.get(i);
        }

        map.clear();

        assertNearCacheIsEmptyEventually(lite, mapName);
    }

    public static void testEvictAll(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);

        int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        for (int i = 0; i < count; i++) {
            liteMap.get(i);
        }

        map.evictAll();

        assertNearCacheIsEmptyEventually(lite, mapName);
    }

    public static void testExecuteOnKey(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);
        map.put(1, 1);

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        liteMap.get(1);

        map.executeOnKey(1, new DummyEntryProcessor(2));

        assertNearCacheIsEmptyEventually(lite, mapName);
    }

    public static void testExecuteOnKeys(HazelcastInstance instance, HazelcastInstance lite, String mapName) {
        IMap<Object, Object> map = instance.getMap(mapName);
        map.put(1, 1);

        IMap<Object, Object> liteMap = lite.getMap(mapName);
        liteMap.get(1);

        Set<Object> keySet = new HashSet<Object>();
        keySet.add(1);
        map.executeOnKeys(keySet, new DummyEntryProcessor(2));

        assertNearCacheIsEmptyEventually(lite, mapName);
    }

    private static class DummyEntryProcessor implements EntryProcessor<Object, Object>, Serializable {

        private final Object newValue;

        DummyEntryProcessor(Object newValue) {
            this.newValue = newValue;
        }

        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            return entry.setValue(newValue);
        }

        @Override
        public EntryBackupProcessor<Object, Object> getBackupProcessor() {
            return null;
        }
    }

    public static Config createConfig(String mapName, boolean liteMember) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);

        Config config = new Config();
        config.setLiteMember(liteMember);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        return config;
    }

    private static MapService getMapService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
    }

    private static NearCache<Data, Object> getNearCache(HazelcastInstance instance, String mapName) {
        return getMapService(instance)
                .getMapServiceContext()
                .getNearCacheProvider()
                .getOrCreateNearCache(mapName);
    }

    private static void assertNullNearCacheEntryEventually(final HazelcastInstance instance, String mapName, Object key) {
        final NearCache<Data, Object> nearCache = getNearCache(instance, mapName);
        final Data keyData = toData(instance, key);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull(toObject(instance, nearCache.get(keyData)));
            }
        });
    }

    private static void assertLiteMemberNearCacheNonEmpty(HazelcastInstance instance, String mapName) {
        NearCache nearCache = getNearCache(instance, mapName);
        int sizeAfterPut = nearCache.size();
        assertTrue("NearCache size should be > 0 but was " + sizeAfterPut, sizeAfterPut > 0);
    }

    private static void assertNearCacheIsEmptyEventually(HazelcastInstance instance, String mapName) {
        final NearCache nearCache = getNearCache(instance, mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int size = nearCache.size();
                assertEquals("Lite member Near Cache size should be 0 after evict but was " + size, 0, size);
            }
        });
    }

    private static Data toData(HazelcastInstance instance, Object obj) {
        return getMapService(instance).getMapServiceContext().toData(obj);
    }

    private static Object toObject(HazelcastInstance instance, Object obj) {
        return getMapService(instance).getMapServiceContext().toObject(obj);
    }
}
