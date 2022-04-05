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
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessorResult;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ICacheDataStructureAdapterTest extends HazelcastTestSupport {

    private ICache<Integer, String> cache;
    private ICache<Integer, String> cacheWithLoader;

    private ICacheDataStructureAdapter<Integer, String> adapter;
    private ICacheDataStructureAdapter<Integer, String> adapterWithLoader;

    @Before
    public void setUp() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();
        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CacheConfig<Integer, String> cacheConfig = new CacheConfig<Integer, String>();

        CacheConfiguration<Integer, String> cacheConfigWithLoader = new CacheConfig<Integer, String>()
                .setReadThrough(true)
                .setCacheLoaderFactory(FactoryBuilder.factoryOf(ICacheCacheLoader.class));

        cache = (ICache<Integer, String>) cacheManager.createCache("CacheDataStructureAdapterTest", cacheConfig);
        cacheWithLoader = (ICache<Integer, String>) cacheManager.createCache("CacheDataStructureAdapterLoaderTest",
                cacheConfigWithLoader);

        adapter = new ICacheDataStructureAdapter<Integer, String>(cache);
        adapterWithLoader = new ICacheDataStructureAdapter<Integer, String>(cacheWithLoader);
    }

    @Test
    public void testSize() {
        cache.put(23, "foo");
        cache.put(42, "bar");

        assertEquals(2, adapter.size());
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

        Future<String> future = adapter.getAsync(42).toCompletableFuture();
        String result = future.get();
        assertEquals("foobar", result);
    }

    @Test
    public void testSet() {
        adapter.set(23, "test");

        assertEquals("test", cache.get(23));
    }

    @Test
    public void testSetAsync() throws Exception {
        cache.put(42, "oldValue");

        Future<Void> future = adapter.setAsync(42, "newValue").toCompletableFuture();
        Void oldValue = future.get();

        assertNull(oldValue);
        assertEquals("newValue", cache.get(42));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testSetAsyncWithTtl() {
        adapter.setAsync(42, "value", 1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testSetAsyncWithExpiryPolicy() throws Exception {
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1000, 1, 1, TimeUnit.MILLISECONDS);
        adapter.setAsync(42, "value", expiryPolicy).toCompletableFuture().get();
        String value = cache.get(42);
        if (value != null) {
            assertEquals("value", value);

            sleepMillis(1100);
            assertNull(cache.get(42));
        }
    }

    @Test
    public void testPut() {
        cache.put(42, "oldValue");

        String oldValue = adapter.put(42, "newValue");

        assertEquals("oldValue", oldValue);
        assertEquals("newValue", cache.get(42));
    }

    @Test
    public void testPutAsync() throws Exception {
        cache.put(42, "oldValue");

        Future<String> future = adapter.putAsync(42, "newValue").toCompletableFuture();
        String oldValue = future.get();

        assertEquals("oldValue", oldValue);
        assertEquals("newValue", cache.get(42));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testPutAsyncWithTtl() {
        adapter.putAsync(42, "value", 1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testPutAsyncWithExpiryPolicy() throws Exception {
        cache.put(42, "oldValue");

        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1000, 1, 1, TimeUnit.MILLISECONDS);
        Future<String> future = adapter.putAsync(42, "newValue", expiryPolicy).toCompletableFuture();
        String oldValue = future.get();
        String newValue = cache.get(42);

        assertEquals("oldValue", oldValue);
        if (newValue != null) {
            assertEquals("newValue", newValue);

            sleepMillis(1100);
            assertNull(cache.get(42));
        }
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testPutTransient() {
        adapter.putTransient(42, "value", 1, TimeUnit.MILLISECONDS);
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

        assertTrue(adapter.putIfAbsentAsync(23, "newValue").toCompletableFuture().get());
        assertFalse(adapter.putIfAbsentAsync(42, "newValue").toCompletableFuture().get());

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

        assertEquals("value-23", adapter.remove(23));
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

        String value = adapter.removeAsync(23).toCompletableFuture().get();
        assertEquals("value-23", value);

        assertFalse(cache.containsKey(23));
    }

    @Test
    public void testDelete() {
        cache.put(23, "value-23");
        assertTrue(cache.containsKey(23));

        adapter.delete(23);
        assertFalse(cache.containsKey(23));
    }

    @Test
    public void testDeleteAsync() throws Exception {
        cache.put(23, "value-23");
        assertTrue(cache.containsKey(23));

        adapter.deleteAsync(23).toCompletableFuture().get();

        assertFalse(cache.containsKey(23));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testEvict() {
        adapter.evict(23);
    }

    @Test
    public void testInvoke() {
        cache.put(23, "value-23");
        cache.put(42, "value-42");

        String result = adapter.invoke(23, new ICacheReplaceEntryProcessor(), "value", "newValue");
        assertEquals("newValue-23", result);

        assertEquals("newValue-23", cache.get(23));
        assertEquals("value-42", cache.get(42));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testExecuteOnKey() {
        adapter.executeOnKey(23, new IMapReplaceEntryProcessor("value", "newValue"));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testExecuteOnKeys() {
        Set<Integer> keys = new HashSet<Integer>(singleton(23));
        adapter.executeOnKeys(keys, new IMapReplaceEntryProcessor("value", "newValue"));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testExecuteOnEntries() {
        adapter.executeOnEntries(new IMapReplaceEntryProcessor("value", "newValue"));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testExecuteOnEntriesWithPredicate() {
        adapter.executeOnEntries(new IMapReplaceEntryProcessor("value", "newValue"), Predicates.alwaysTrue());
    }

    @Test
    public void testContainsKey() {
        cache.put(23, "value-23");

        assertTrue(adapter.containsKey(23));
        assertFalse(adapter.containsKey(42));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testLoadAll() {
        adapterWithLoader.loadAll(true);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testLoadAllWithKeys() {
        adapterWithLoader.loadAll(Collections.<Integer>emptySet(), true);
    }

    @Test
    public void testLoadAllWithListener() {
        ICacheCompletionListener listener = new ICacheCompletionListener();
        cacheWithLoader.put(23, "value-23");

        adapterWithLoader.loadAll(Collections.singleton(23), true, listener);
        listener.await();

        assertEquals("newValue-23", cacheWithLoader.get(23));
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

    @Test(expected = MethodNotAvailableException.class)
    public void testEvictAll() {
        adapter.evictAll();
    }

    @Test
    public void testInvokeAll() {
        cache.put(23, "value-23");
        cache.put(42, "value-42");
        cache.put(65, "value-65");

        Set<Integer> keys = new HashSet<Integer>(asList(23, 65, 88));
        Map<Integer, EntryProcessorResult<String>> resultMap = adapter.invokeAll(keys, new ICacheReplaceEntryProcessor(),
                "value", "newValue");
        assertEquals(2, resultMap.size());
        assertEquals("newValue-23", resultMap.get(23).get());
        assertEquals("newValue-65", resultMap.get(65).get());

        assertEquals("newValue-23", cache.get(23));
        assertEquals("value-42", cache.get(42));
        assertEquals("newValue-65", cache.get(65));
        assertNull(cache.get(88));
    }

    @Test
    public void testClear() {
        cache.put(23, "foobar");

        adapter.clear();

        assertEquals(0, cache.size());
    }

    @Test
    public void testClose() {
        adapter.close();

        assertTrue(cache.isClosed());
    }

    @Test
    public void testDestroy() {
        adapter.destroy();

        assertTrue(cache.isDestroyed());
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testGetLocalMapStats() {
        adapter.getLocalMapStats();
    }
}
