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

package com.hazelcast.internal.nearcache;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSize;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheStats;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.isCacheOnUpdate;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.setEvictionConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheEvictionsEventually;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSizeEventually;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Contains the logic code for unified Near Cache tests.
 *
 * @param <NK> key type of the tested Near Cache
 * @param <NV> value type of the tested Near Cache
 */
public abstract class AbstractNearCacheBasicTest<NK, NV> extends HazelcastTestSupport {

    /**
     * The default count to be inserted into the Near Caches.
     */
    protected static final int DEFAULT_RECORD_COUNT = 1000;

    /**
     * The default name used for the data structures which have a Near Cache.
     */
    protected static final String DEFAULT_NEAR_CACHE_NAME = "defaultNearCache";

    /**
     * The {@link NearCacheConfig} used by the Near Cache tests.
     *
     * Needs to be set by the implementations of this class in their {@link org.junit.Before} methods.
     */
    protected NearCacheConfig nearCacheConfig;

    /**
     * Creates the {@link NearCacheTestContext} used by the Near Cache tests.
     *
     * @param <K> key type of the created {@link DataStructureAdapter}
     * @param <V> value type of the created {@link DataStructureAdapter}
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createContext();

    protected final void populateMap(NearCacheTestContext<Integer, String, NK, NV> context) {
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            context.dataAdapter.put(i, "value-" + i);
        }
    }

    protected final void populateNearCache(NearCacheTestContext<Integer, String, NK, NV> context) {
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            context.nearCacheAdapter.get(i);
        }
    }

    @SuppressWarnings("unchecked")
    private NK getNearCacheKey(NearCacheTestContext<Integer, String, NK, NV> context, int key) {
        return (NK) context.serializationService.toData(key);
    }

    /**
     * Checks that the Near Cache never returns its internal {@link NearCache#CACHED_AS_NULL} to the public API.
     */
    @Test
    public void whenEmptyMap_thenPopulatedNearCacheShouldReturnNull_neverNULLOBJECT() {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            // populate Near Cache
            assertNull("Expected null from original data structure for key " + i, context.nearCacheAdapter.get(i));
            // fetch value from Near Cache
            assertNull("Expected null from Near cached data structure for key " + i, context.nearCacheAdapter.get(i));

            // fetch internal value directly from Near Cache
            NK key = getNearCacheKey(context, i);
            NV value = context.nearCache.get(key);
            if (value != null) {
                // the internal value should either be `null` or `CACHED_AS_NULL`
                assertEquals("Expected CACHED_AS_NULL in Near Cache for key " + i,
                        NearCache.CACHED_AS_NULL, context.nearCache.get(key));
            }
        }
    }

    /**
     * Checks that the Near Cache updates value for keys which are already in the Near Cache,
     * even if the Near Cache is full an the eviction is disabled (via {@link com.hazelcast.config.EvictionPolicy#NONE}.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenCacheIsFull_thenPutOnSameKeyShouldUpdateValue_withUpdateOnNearCacheAdapter() {
        int size = DEFAULT_RECORD_COUNT / 2;
        setEvictionConfig(nearCacheConfig, NONE, ENTRY_COUNT, size);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        populateMap(context);
        populateNearCache(context);

        assertNearCacheSize(context, size);
        assertEquals("value-1", context.nearCacheAdapter.get(1));

        context.nearCacheAdapter.put(1, "newValue");

        long expectedMisses = getExpectedMissesWithLocalUpdatePolicy(context);
        long expectedHits = getExpectedHitsWithLocalUpdatePolicy(context);

        assertEquals("newValue", context.nearCacheAdapter.get(1));
        assertEquals("newValue", context.nearCacheAdapter.get(1));

        assertNearCacheStats(context, size, expectedHits, expectedMisses);
    }

    /**
     * Checks that the Near Cache updates value for keys which are already in the Near Cache,
     * even if the Near Cache is full an the eviction is disabled (via {@link com.hazelcast.config.EvictionPolicy#NONE}.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenCacheIsFull_thenPutOnSameKeyShouldUpdateValue_withUpdateOnDataAdapter() {
        final int size = DEFAULT_RECORD_COUNT / 2;
        setEvictionConfig(nearCacheConfig, NONE, ENTRY_COUNT, size);
        nearCacheConfig.setInvalidateOnChange(true);
        final NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        populateMap(context);
        populateNearCache(context);

        assertNearCacheSize(context, size);
        assertEquals("value-1", context.nearCacheAdapter.get(1));

        context.dataAdapter.put(1, "newValue");

        // we have to use assertTrueEventually since the invalidation is done asynchronously
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                long expectedMisses = getExpectedMissesWithLocalUpdatePolicy(context);
                long expectedHits = getExpectedHitsWithLocalUpdatePolicy(context);

                assertEquals("newValue", context.nearCacheAdapter.get(1));
                assertEquals("newValue", context.nearCacheAdapter.get(1));

                assertNearCacheStats(context, size, expectedHits, expectedMisses);
            }
        });
    }

    private long getExpectedMissesWithLocalUpdatePolicy(NearCacheTestContext<Integer, String, NK, NV> context) {
        if (isCacheOnUpdate(nearCacheConfig)) {
            // we expect the first and second get() to be hits, since the value should be already be cached
            return context.stats.getMisses();
        }
        // we expect the first get() to be a miss, due to the replaced / invalidated value
        return context.stats.getMisses() + 1;
    }

    private long getExpectedHitsWithLocalUpdatePolicy(NearCacheTestContext<Integer, String, NK, NV> context) {
        if (isCacheOnUpdate(nearCacheConfig)) {
            // we expect the first and second get() to be hits, since the value should be already be cached
            return context.stats.getHits() + 2;
        }
        // we expect the second get() to be a hit, since it should be served from the Near Cache
        return context.stats.getHits() + 1;
    }

    /**
     * Checks that the Near Cache values are eventually invalidated when {@link DataStructureAdapter#putAll(Map)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenPutAllIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenPutAllIsUsed_thenNearCacheShouldBeInvalidated(true);
    }

    /**
     * Checks that the Near Cache values are eventually invalidated when {@link DataStructureAdapter#putAll(Map)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenPutAllIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenPutAllIsUsed_thenNearCacheShouldBeInvalidated(false);
    }

    private void whenPutAllIsUsed_thenNearCacheShouldBeInvalidated(boolean useNearCacheAdapter) {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        populateMap(context);
        populateNearCache(context);

        Map<Integer, String> invalidationMap = new HashMap<Integer, String>(DEFAULT_RECORD_COUNT);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            invalidationMap.put(i, "newValue-" + i);
        }

        // this should invalidate the Near Cache
        DataStructureAdapter<Integer, String> adapter = useNearCacheAdapter ? context.nearCacheAdapter : context.dataAdapter;
        adapter.putAll(invalidationMap);

        assertNearCacheSizeEventually(context, 0, "Invalidation is not working on putAll()");
    }

    /**
     * Checks that the Near Cache values are eventually invalidated when {@link DataStructureAdapter#replace(Object, Object)}
     * is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenReplaceIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenReplaceIsUsed_thenNearCacheShouldBeInvalidated(true, false);
    }

    /**
     * Checks that the Near Cache values are eventually invalidated when {@link DataStructureAdapter#replace(Object, Object)}
     * is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenReplaceIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenReplaceIsUsed_thenNearCacheShouldBeInvalidated(false, false);
    }

    /**
     * Checks that the Near Cache values are eventually invalidated when
     * {@link DataStructureAdapter#replace(Object, Object, Object)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenReplaceWithOldValueIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenReplaceIsUsed_thenNearCacheShouldBeInvalidated(true, true);
    }

    /**
     * Checks that the Near Cache values are eventually invalidated when
     * {@link DataStructureAdapter#replace(Object, Object, Object)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenReplaceWithOldValueIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenReplaceIsUsed_thenNearCacheShouldBeInvalidated(false, true);
    }

    private void whenReplaceIsUsed_thenNearCacheShouldBeInvalidated(boolean useNearCacheAdapter, boolean useOldValue) {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        populateMap(context);
        populateNearCache(context);

        // this should invalidate the Near Cache
        DataStructureAdapter<Integer, String> adapter = useNearCacheAdapter ? context.nearCacheAdapter : context.dataAdapter;
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            if (useOldValue) {
                assertTrue(adapter.replace(i, "value-" + i, "newValue-" + i));
            } else {
                assertEquals("value-" + i, adapter.replace(i, "newValue-" + i));
            }
        }

        int expectedNearCacheSize = useNearCacheAdapter && isCacheOnUpdate(nearCacheConfig) ? DEFAULT_RECORD_COUNT : 0;
        assertNearCacheSizeEventually(context, expectedNearCacheSize, "Invalidation is not working on replace()");
    }

    @Test
    public void testGetAsyncPopulatesNearCache() throws Exception {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        populateMap(context);

        // populate Near Cache with getAsync()
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            Future future = context.nearCacheAdapter.getAsync(i);
            future.get();
        }

        // generate Near Cache hits
        populateNearCache(context);

        long ownedEntryCount = context.stats.getOwnedEntryCount();
        assertTrue(format("Near Cache should be populated but current size is %d", ownedEntryCount), ownedEntryCount > 0);
    }

    /**
     * Checks that the Near Cache keys are correctly checked when {@link DataStructureAdapter#containsKey(Object)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void testContainsKey_withUpdateOnNearCacheAdapter() {
        testContainsKey(true);
    }

    /**
     * Checks that the memory costs are calculated correctly.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void testContainsKey_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        testContainsKey(false);
    }

    private void testContainsKey(boolean useNearCacheAdapter) {
        final NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        // populate map
        context.dataAdapter.put(1, "value1");
        context.dataAdapter.put(2, "value2");
        context.dataAdapter.put(3, "value3");

        // populate Near Cache
        context.nearCacheAdapter.get(1);
        context.nearCacheAdapter.get(2);
        context.nearCacheAdapter.get(3);

        assertTrue(context.nearCacheAdapter.containsKey(1));
        assertTrue(context.nearCacheAdapter.containsKey(2));
        assertTrue(context.nearCacheAdapter.containsKey(3));
        assertFalse(context.nearCacheAdapter.containsKey(5));

        // remove a key which is in the Near Cache
        DataStructureAdapter<Integer, String> adapter = useNearCacheAdapter ? context.nearCacheAdapter : context.dataAdapter;
        adapter.remove(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(context.nearCacheAdapter.containsKey(1));
                assertTrue(context.nearCacheAdapter.containsKey(2));
                assertTrue(context.nearCacheAdapter.containsKey(3));
                assertFalse(context.nearCacheAdapter.containsKey(5));
            }
        });
    }

    /**
     * Checks that the {@link com.hazelcast.monitor.NearCacheStats} are calculated correctly.
     */
    @Test
    public void testNearCacheStats() {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        // populate map
        populateMap(context);
        assertNearCacheStats(context, 0, 0, 0);

        // populate Near Cache
        populateNearCache(context);
        assertNearCacheStats(context, DEFAULT_RECORD_COUNT, 0, DEFAULT_RECORD_COUNT);

        // make some hits
        populateNearCache(context);
        assertNearCacheStats(context, DEFAULT_RECORD_COUNT, DEFAULT_RECORD_COUNT, DEFAULT_RECORD_COUNT);
    }

    /**
     * Checks that the memory costs are calculated correctly.
     *
     * This variant uses a single-threaded approach to fill the Near Cache with data.
     */
    @Test
    public void testNearCacheMemoryCostCalculation() {
        testNearCacheMemoryCostCalculation(1);
    }

    /**
     * Checks that the memory costs are calculated correctly.
     *
     * This variant uses a multi-threaded approach to fill the Near Cache with data.
     */
    @Test
    public void testNearCacheMemoryCostCalculation_withConcurrentCacheMisses() {
        testNearCacheMemoryCostCalculation(10);
    }

    private void testNearCacheMemoryCostCalculation(int threadCount) {
        nearCacheConfig.setInvalidateOnChange(true);
        final NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        populateMap(context);

        final CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        Runnable task = new Runnable() {
            @Override
            public void run() {
                populateNearCache(context);
                countDownLatch.countDown();
            }
        };

        ExecutorService executorService = newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(task);
        }
        assertOpenEventually(countDownLatch);

        // the Near Cache is filled, we should see some memory costs now
        if (context.hasLocalData && nearCacheConfig.getInMemoryFormat() != OBJECT) {
            // the heap costs are just calculated if there is local data which is not in OBJECT in-memory-format
            assertTrue("The Near Cache is filled, there should be some owned entry memory costs",
                    context.stats.getOwnedEntryMemoryCost() > 0);
            if (context.nearCacheAdapter.getLocalMapStats() != null && nearCacheConfig.getInMemoryFormat() == BINARY) {
                assertTrue("The Near Cache is filled, there should be some heap costs",
                        context.nearCacheAdapter.getLocalMapStats().getHeapCost() > 0);
            }
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            context.nearCacheAdapter.remove(i);
        }
        assertNearCacheSizeEventually(context, 0);

        // the Near Cache is empty, we shouldn't see memory costs anymore
        assertEquals("The Near Cache is empty, there should be no owned entry memory costs",
                0, context.stats.getOwnedEntryMemoryCost());
        if (context.nearCacheAdapter.getLocalMapStats() != null) {
            // this assert will work in all scenarios, since the default value should be 0 if no costs are calculated
            assertEquals("The Near Cache is empty, there should be no heap costs", 0,
                    context.nearCacheAdapter.getLocalMapStats().getHeapCost());
        }
    }

    /**
     * Checks that the Near Cache eviction works as expected if the Near Cache is full.
     */
    @Test
    public void testNearCacheEviction() {
        setEvictionConfig(nearCacheConfig, LRU, ENTRY_COUNT, DEFAULT_RECORD_COUNT);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        // all Near Cache implementations use the same eviction algorithm, which evicts a single entry
        int expectedEvictions = 1;

        // populate map with an extra entry
        populateMap(context);
        context.dataAdapter.put(DEFAULT_RECORD_COUNT, "value-" + DEFAULT_RECORD_COUNT);

        // populate Near Caches
        populateNearCache(context);

        // we expect (size + the extra entry - the expectedEvictions) entries in the Near Cache
        long expectedOwnedEntryCount = DEFAULT_RECORD_COUNT + 1 - expectedEvictions;
        long expectedHits = context.stats.getHits();
        long expectedMisses = context.stats.getMisses() + 1;

        // trigger eviction via fetching the extra entry
        context.nearCacheAdapter.get(DEFAULT_RECORD_COUNT);

        assertNearCacheEvictionsEventually(context, expectedEvictions);
        assertNearCacheStats(context, expectedOwnedEntryCount, expectedHits, expectedMisses, expectedEvictions, 0);
    }
}
