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

package com.hazelcast.internal.adapter;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.expiry.ExpiryPolicy;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IMapDataStructureAdapterTest extends HazelcastTestSupport {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-debug-map.xml");

    private DataStructureLoader mapStore = new IMapMapStore();

    private IMap<Integer, String> map;
    private IMap<Integer, String> mapWithLoader;

    private IMapDataStructureAdapter<Integer, String> adapter;
    private IMapDataStructureAdapter<Integer, String> adapterWithLoader;

    @Before
    public void setUp() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY)
                .setImplementation(mapStore);

        Config config = new Config();
        config.getMapConfig("IMapDataStructureAdapterLoaderTest")
                .setMapStoreConfig(mapStoreConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config);

        map = hazelcastInstance.getMap("IMapDataStructureAdapterTest");
        mapWithLoader = hazelcastInstance.getMap("IMapDataStructureAdapterLoaderTest");

        adapter = new IMapDataStructureAdapter<Integer, String>(map);
        adapterWithLoader = new IMapDataStructureAdapter<Integer, String>(mapWithLoader);
    }

    @Test
    public void testSize() {
        map.put(23, "foo");
        map.put(42, "bar");

        assertEquals(2, adapter.size());
    }

    @Test
    public void testGet() {
        map.put(42, "foobar");

        String result = adapter.get(42);
        assertEquals("foobar", result);
    }

    @Test
    public void testGetAsync() throws Exception {
        map.put(42, "foobar");

        Future<String> future = adapter.getAsync(42).toCompletableFuture();
        String result = future.get();
        assertEquals("foobar", result);
    }

    @Test
    public void testSet() {
        adapter.set(23, "test");

        assertEquals("test", map.get(23));
    }

    @Test
    public void testSetAsync() throws Exception {
        map.put(42, "oldValue");

        Future<Void> future = adapter.setAsync(42, "newValue").toCompletableFuture();
        Void oldValue = future.get();

        assertNull(oldValue);
        assertEquals("newValue", map.get(42));
    }

    @Test
    public void testSetAsyncWithTtl() {
        adapter.setAsync(42, "value", 1000, TimeUnit.MILLISECONDS);
        String value = map.get(42);
        if (value != null) {
            assertEquals("value", value);

            sleepMillis(1100);
            assertNull(map.get(42));
        }
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testSetAsyncWithExpiryPolicy() {
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, TimeUnit.MILLISECONDS);
        adapter.setAsync(42, "value", expiryPolicy);
    }

    @Test
    public void testPut() {
        map.put(42, "oldValue");

        String oldValue = adapter.put(42, "newValue");

        assertEquals("oldValue", oldValue);
        assertEquals("newValue", map.get(42));
    }

    @Test
    public void testPutAsync() throws Exception {
        map.put(42, "oldValue");

        Future<String> future = adapter.putAsync(42, "newValue").toCompletableFuture();
        String oldValue = future.get();

        assertEquals("oldValue", oldValue);
        assertEquals("newValue", map.get(42));
    }

    @Test
    public void testPutAsyncWithTtl() throws Exception {
        map.put(42, "oldValue");

        Future<String> future = adapter.putAsync(42, "newValue", 1000, TimeUnit.MILLISECONDS).toCompletableFuture();
        String oldValue = future.get();
        String newValue = map.get(42);

        assertEquals("oldValue", oldValue);
        if (newValue != null) {
            assertEquals("newValue", newValue);

            sleepMillis(1100);
            assertNull(map.get(42));
        }
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testPutAsyncWithExpiryPolicy() {
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, TimeUnit.MILLISECONDS);
        adapter.putAsync(42, "value", expiryPolicy);
    }

    @Test
    public void testPutTransient() {
        adapter.putTransient(42, "value", 1000, TimeUnit.MILLISECONDS);

        String value = map.get(42);
        if (value != null) {
            assertEquals("value", value);

            sleepMillis(1100);
            assertNull(map.get(42));
        }
    }

    @Test
    public void testPutIfAbsent() {
        map.put(42, "oldValue");

        assertTrue(adapter.putIfAbsent(23, "newValue"));
        assertFalse(adapter.putIfAbsent(42, "newValue"));

        assertEquals("newValue", map.get(23));
        assertEquals("oldValue", map.get(42));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testPutIfAbsentAsync() {
        adapter.putIfAbsentAsync(23, "value");
    }

    @Test
    public void testReplace() {
        map.put(42, "oldValue");

        String oldValue = adapter.replace(42, "newValue");

        assertEquals("oldValue", oldValue);
        assertEquals("newValue", map.get(42));
    }

    @Test
    public void testReplaceWithOldValue() {
        map.put(42, "oldValue");

        assertFalse(adapter.replace(42, "foobar", "newValue"));
        assertTrue(adapter.replace(42, "oldValue", "newValue"));

        assertEquals("newValue", map.get(42));
    }

    @Test
    public void testRemove() {
        map.put(23, "value-23");
        assertTrue(map.containsKey(23));

        assertEquals("value-23", adapter.remove(23));
        assertFalse(map.containsKey(23));
    }

    @Test
    public void testRemoveWithOldValue() {
        map.put(23, "value-23");
        assertTrue(map.containsKey(23));

        assertFalse(adapter.remove(23, "foobar"));
        assertTrue(adapter.remove(23, "value-23"));
        assertFalse(map.containsKey(23));
    }

    @Test
    public void testRemoveAsync() throws Exception {
        map.put(23, "value-23");
        assertTrue(map.containsKey(23));

        String value = adapter.removeAsync(23).toCompletableFuture().get();
        assertEquals("value-23", value);

        assertFalse(map.containsKey(23));
    }

    @Test
    public void testDelete() {
        map.put(23, "value-23");
        assertTrue(map.containsKey(23));

        adapter.delete(23);
        assertFalse(map.containsKey(23));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testDeleteAsync() {
        adapter.deleteAsync(23);
    }

    @Test
    public void testEvict() {
        mapWithLoader.put(23, "value-23");
        mapWithLoader.put(42, "value-42");
        mapWithLoader.put(65, "value-65");

        adapterWithLoader.evict(42);

        assertEquals(2, mapWithLoader.size());
        assertTrue(mapWithLoader.containsKey(23));
        assertFalse(mapWithLoader.containsKey(42));
        assertTrue(mapWithLoader.containsKey(65));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testInvoke() {
        adapter.invoke(23, new ICacheReplaceEntryProcessor(), "value", "newValue");
    }

    @Test
    public void testExecuteOnKey() {
        map.put(23, "value-23");
        map.put(42, "value-42");

        String result = (String) adapter.executeOnKey(23, new IMapReplaceEntryProcessor("value", "newValue"));
        assertEquals("newValue-23", result);

        assertEquals("newValue-23", map.get(23));
        assertEquals("value-42", map.get(42));
    }

    @Test
    public void testExecuteOnKeys() {
        map.put(23, "value-23");
        map.put(42, "value-42");
        map.put(65, "value-65");

        Set<Integer> keys = new HashSet<Integer>(asList(23, 65, 88));
        Map<Integer, Object> resultMap = adapter.executeOnKeys(keys, new IMapReplaceEntryProcessor("value", "newValue"));
        assertEquals(2, resultMap.size());
        assertEquals("newValue-23", resultMap.get(23));
        assertEquals("newValue-65", resultMap.get(65));

        assertEquals("newValue-23", map.get(23));
        assertEquals("value-42", map.get(42));
        assertEquals("newValue-65", map.get(65));
        assertNull(map.get(88));
    }

    @Test
    public void testExecuteOnEntries() {
        map.put(23, "value-23");
        map.put(42, "value-42");

        Map<Integer, Object> resultMap = adapter.executeOnEntries(new IMapReplaceEntryProcessor("value", "newValue"));
        assertEquals(2, resultMap.size());
        assertEquals("newValue-23", resultMap.get(23));
        assertEquals("newValue-42", resultMap.get(42));

        assertEquals("newValue-23", map.get(23));
        assertEquals("newValue-42", map.get(42));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecuteOnEntriesWithPredicate() {
        map.put(23, "value-23");
        map.put(42, "value-42");
        map.put(65, "value-65");

        Predicate<Integer, Object> predicate = Predicates.sql("__key IN (23, 65)");
        Map<Integer, Object> resultMap = adapter.executeOnEntries(new IMapReplaceEntryProcessor("value", "newValue"), predicate);
        assertEquals(2, resultMap.size());
        assertEquals("newValue-23", resultMap.get(23));
        assertEquals("newValue-65", resultMap.get(65));

        assertEquals("newValue-23", map.get(23));
        assertEquals("newValue-65", map.get(65));
    }

    @Test
    public void testContainsKey() {
        map.put(23, "value-23");

        assertTrue(adapter.containsKey(23));
        assertFalse(adapter.containsKey(42));
    }

    @Test
    public void testLoadAll() {
        mapWithLoader.put(23, "value-23");
        mapStore.setKeys(singleton(23));

        adapterWithLoader.loadAll(true);
        adapterWithLoader.waitUntilLoaded();

        assertEquals("newValue-23", mapWithLoader.get(23));
    }

    @Test
    public void testLoadAllWithKeys() {
        mapWithLoader.put(23, "value-23");

        adapterWithLoader.loadAll(Collections.singleton(23), true);

        assertEquals("newValue-23", mapWithLoader.get(23));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testLoadAllWithListener() {
        adapter.loadAll(Collections.<Integer>emptySet(), true, null);
    }

    @Test
    public void testGetAll() {
        map.put(23, "value-23");
        map.put(42, "value-42");

        Map<Integer, String> expectedResult = new HashMap<Integer, String>();
        expectedResult.put(23, "value-23");
        expectedResult.put(42, "value-42");

        Map<Integer, String> result = adapter.getAll(expectedResult.keySet());
        assertEquals(expectedResult, result);
    }

    @Test
    public void testPutAll() {
        Map<Integer, String> expectedResult = new HashMap<Integer, String>();
        expectedResult.put(23, "value-23");
        expectedResult.put(42, "value-42");

        adapter.putAll(expectedResult);

        assertEquals(expectedResult.size(), map.size());
        for (Integer key : expectedResult.keySet()) {
            assertTrue(map.containsKey(key));
        }
    }

    @Test
    public void testRemoveAll() {
        map.put(23, "value-23");
        map.put(42, "value-42");

        adapter.removeAll();

        assertEquals(0, map.size());
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testRemoveAllWithKeys() {
        adapter.removeAll(singleton(42));
    }

    @Test
    public void testEvictAll() {
        mapWithLoader.put(23, "value-23");
        mapWithLoader.put(42, "value-42");
        mapWithLoader.put(65, "value-65");

        adapterWithLoader.evictAll();

        assertEquals(0, mapWithLoader.size());
        assertFalse(mapWithLoader.containsKey(23));
        assertFalse(mapWithLoader.containsKey(42));
        assertFalse(mapWithLoader.containsKey(65));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testInvokeAll() {
        Set<Integer> keys = new HashSet<Integer>(asList(23, 65, 88));
        adapter.invokeAll(keys, new ICacheReplaceEntryProcessor(), "value", "newValue");
    }

    @Test
    public void testClear() {
        map.put(23, "foobar");

        adapter.clear();

        assertEquals(0, map.size());
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testClose() {
        adapter.close();
    }

    @Test
    public void testDestroy() {
        map.put(23, "foobar");

        adapter.destroy();

        assertTrue(map.isEmpty());
    }

    @Test
    public void testGetLocalMapStats() {
        assertNotNull(adapter.getLocalMapStats());

        assertEquals(0, adapter.getLocalMapStats().getOwnedEntryCount());

        adapter.put(23, "value-23");
        assertEquals(1, adapter.getLocalMapStats().getOwnedEntryCount());
    }
}
