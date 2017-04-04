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

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.cache.impl.HazelcastServerCachingProvider.createCachingProvider;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ICacheDataStructureAdapterTest extends HazelcastTestSupport {

    private ICache<Integer, String> cache;
    private ICacheDataStructureAdapter<Integer, String> adapter;

    @Before
    public void setUp() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();
        HazelcastServerCachingProvider cachingProvider = createCachingProvider(hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CacheConfig<Integer, String> cacheConfig = new CacheConfig<Integer, String>();

        cache = (ICache<Integer, String>) cacheManager.createCache("CacheDataStructureAdapterTest", cacheConfig);
        adapter = new ICacheDataStructureAdapter<Integer, String>(cache);
    }

    @Test
    public void testGet() {
        cache.put(42, "foobar");

        String result = adapter.get(42);
        assertEquals("foobar", result);
    }

    @Test
    public void testGetAsync() throws Exception {
        cache.put(42, "foobar");

        Future<String> future = adapter.getAsync(42);
        String result = future.get();
        assertEquals("foobar", result);
    }

    @Test
    public void testSet() {
        adapter.set(23, "test");

        assertEquals("test", cache.get(23));
    }

    @Test
    public void testPut() {
        cache.put(42, "oldValue");

        String oldValue = adapter.put(42, "newValue");

        assertEquals("oldValue", oldValue);
        assertEquals("newValue", cache.get(42));
    }

    @Test
    public void testPutIfAbsent() {
        cache.put(42, "oldValue");

        assertTrue(adapter.putIfAbsent(23, "newValue"));
        assertFalse(adapter.putIfAbsent(42, "newValue"));

        assertEquals("newValue", cache.get(23));
        assertEquals("oldValue", cache.get(42));
    }

    @Test
    public void testPutIfAbsentAsync() throws Exception {
        cache.put(42, "oldValue");

        assertTrue(adapter.putIfAbsentAsync(23, "newValue").get());
        assertFalse(adapter.putIfAbsentAsync(42, "newValue").get());

        assertEquals("newValue", cache.get(23));
        assertEquals("oldValue", cache.get(42));
    }

    @Test
    public void testReplace() {
        cache.put(42, "oldValue");

        String oldValue = adapter.replace(42, "newValue");

        assertEquals("oldValue", oldValue);
        assertEquals("newValue", cache.get(42));
    }

    @Test
    public void testReplaceWithOldValue() {
        cache.put(42, "oldValue");

        assertFalse(adapter.replace(42, "foobar", "newValue"));
        assertTrue(adapter.replace(42, "oldValue", "newValue"));

        assertEquals("newValue", cache.get(42));
    }

    @Test
    public void testRemove() {
        cache.put(23, "value-23");
        assertTrue(cache.containsKey(23));

        adapter.remove(23);
        assertFalse(cache.containsKey(23));
    }

    @Test
    public void testRemoveWithOldValue() {
        cache.put(23, "value-23");
        assertTrue(cache.containsKey(23));

        assertFalse(adapter.remove(23, "foobar"));
        assertTrue(adapter.remove(23, "value-23"));
        assertFalse(cache.containsKey(23));
    }

    @Test
    public void testRemoveAsync() throws Exception {
        cache.put(23, "value-23");
        assertTrue(cache.containsKey(23));

        String value = adapter.removeAsync(23).get();
        assertEquals("value-23", value);

        assertFalse(cache.containsKey(23));
    }

    @Test
    public void testContainsKey() {
        cache.put(23, "value-23");

        assertTrue(adapter.containsKey(23));
        assertFalse(adapter.containsKey(42));
    }

    @Test
    public void testGetAll() {
        cache.put(23, "value-23");
        cache.put(42, "value-42");

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

        assertEquals(expectedResult.size(), cache.size());
        for (Integer key : expectedResult.keySet()) {
            assertTrue(cache.containsKey(key));
        }
    }

    @Test
    public void testRemoveAll() {
        cache.put(23, "value-23");
        cache.put(42, "value-42");

        adapter.removeAll();

        assertEquals(0, cache.size());
    }

    @Test
    public void testRemoveAllWithKeys() {
        cache.put(23, "value-23");
        cache.put(42, "value-42");

        adapter.removeAll(singleton(42));

        assertEquals(1, cache.size());
        assertTrue(cache.containsKey(23));
        assertFalse(cache.containsKey(42));
    }

    @Test
    public void testClear() {
        cache.put(23, "foobar");

        adapter.clear();

        assertEquals(0, cache.size());
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testGetLocalMapStats() {
        adapter.getLocalMapStats();
    }
}
