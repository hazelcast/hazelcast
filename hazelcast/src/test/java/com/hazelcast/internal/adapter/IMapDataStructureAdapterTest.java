/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IMapDataStructureAdapterTest extends HazelcastTestSupport {

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
                .setClassName(null)
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
    public void testGet() {
        map.put(42, "foobar");

        String result = adapter.get(42);
        assertEquals("foobar", result);
    }

    @Test
    public void testGetAsync() throws Exception {
        map.put(42, "foobar");

        Future<String> future = adapter.getAsync(42);
        String result = future.get();
        assertEquals("foobar", result);
    }

    @Test
    public void testSet() {
        adapter.set(23, "test");

        assertEquals("test", map.get(23));
    }

    @Test
    public void testPut() {
        map.put(42, "oldValue");

        String oldValue = adapter.put(42, "newValue");

        assertEquals("oldValue", oldValue);
        assertEquals("newValue", map.get(42));
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

        adapter.remove(23);
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

        String value = adapter.removeAsync(23).get();
        assertEquals("value-23", value);

        assertFalse(map.containsKey(23));
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

    @Test
    public void testGetLocalMapStats() {
        assertNotNull(adapter.getLocalMapStats());

        assertEquals(0, adapter.getLocalMapStats().getOwnedEntryCount());

        adapter.put(23, "value-23");
        assertEquals(1, adapter.getLocalMapStats().getOwnedEntryCount());
    }
}
