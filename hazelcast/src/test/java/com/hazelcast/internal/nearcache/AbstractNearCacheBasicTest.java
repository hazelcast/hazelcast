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
import com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods;
import com.hazelcast.internal.adapter.ICacheCompletionListener;
import com.hazelcast.internal.adapter.ICacheReplaceEntryProcessor;
import com.hazelcast.internal.adapter.IMapReplaceEntryProcessor;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import javax.cache.processor.EntryProcessorResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheContent;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheEvictionsEventually;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSize;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSizeEventually;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheStats;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertThatMemoryCostsAreGreaterThanZero;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertThatMemoryCostsAreZero;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assumeThatLocalUpdatePolicyIsCacheOnUpdate;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assumeThatLocalUpdatePolicyIsInvalidate;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assumeThatMethodIsAvailable;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getFromNearCache;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getFuture;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.isCacheOnUpdate;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.setEvictionConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.waitUntilLoaded;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.warmupPartitionsAndWaitForAllSafeState;
import static java.lang.String.format;
import static java.util.Arrays.asList;
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
@SuppressWarnings("WeakerAccess")
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
     * Defines all {@link DataStructureMethods} which are using EntryProcessors.
     */
    private static final List<DataStructureMethods> ENTRY_PROCESSOR_METHODS = asList(
            DataStructureMethods.INVOKE,
            DataStructureMethods.EXECUTE_ON_KEY,
            DataStructureMethods.EXECUTE_ON_KEYS,
            DataStructureMethods.INVOKE_ALL
    );

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
    protected <K, V> NearCacheTestContext<K, V, NK, NV> createContext() {
        return createContext(false);
    }

    /**
     * Creates the {@link NearCacheTestContext} used by the Near Cache tests.
     *
     * @param loaderEnabled determines if a loader should be configured
     * @param <K>        key type of the created {@link DataStructureAdapter}
     * @param <V>        value type of the created {@link DataStructureAdapter}
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createContext(boolean loaderEnabled);

    protected final void populateDataAdapter(NearCacheTestContext<Integer, String, NK, NV> context) {
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            context.dataAdapter.put(i, "value-" + i);
        }
    }

    protected final void populateNearCache(NearCacheTestContext<Integer, String, NK, NV> context) {
        populateNearCache(context, DataStructureMethods.GET);
    }

    protected void populateNearCache(NearCacheTestContext<Integer, String, NK, NV> context, DataStructureMethods method) {
        populateNearCache(context, method, DEFAULT_RECORD_COUNT);
    }

    private void populateNearCache(NearCacheTestContext<Integer, String, NK, NV> context, DataStructureMethods method, int size) {
        switch (method) {
            case GET:
                for (int i = 0; i < size; i++) {
                    String value = context.nearCacheAdapter.get(i);
                    assertEquals("value-" + i, value);
                }
                break;
            case GET_ASYNC:
                List<Future<String>> futures = new ArrayList<Future<String>>(size);
                for (int i = 0; i < size; i++) {
                    futures.add(context.nearCacheAdapter.getAsync(i));
                }
                for (int i = 0; i < size; i++) {
                    String value = getFuture(futures.get(i), "Could not get value for index " + i + " via getAsync()");
                    assertEquals("value-" + i, value);
                }
                break;
            case GET_ALL:
                Set<Integer> getAllSet = new HashSet<Integer>(size);
                for (int i = 0; i < size; i++) {
                    getAllSet.add(i);
                }
                Map<Integer, String> resultMap = context.nearCacheAdapter.getAll(getAllSet);
                assertEquals(size, resultMap.size());
                for (int i = 0; i < size; i++) {
                    String value = resultMap.get(i);
                    assertEquals("value-" + i, value);
                }
                break;
            default:
                throw new IllegalArgumentException("Unexpected method: " + method);
        }
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#GET} is used
     * and that the {@link com.hazelcast.monitor.NearCacheStats} are calculated correctly.
     */
    @Test
    public void whenGetIsUsed_thenNearCacheShouldBePopulated() {
        whenGetIsUsed_thenNearCacheShouldBePopulated(DataStructureMethods.GET);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#GET_ASYNC} is used
     * and that the {@link com.hazelcast.monitor.NearCacheStats} are calculated correctly.
     */
    @Test
    public void whenGetAsyncIsUsed_thenNearCacheShouldBePopulated() {
        whenGetIsUsed_thenNearCacheShouldBePopulated(DataStructureMethods.GET_ASYNC);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#GET_ALL} is used
     * and that the {@link com.hazelcast.monitor.NearCacheStats} are calculated correctly.
     */
    @Test
    public void whenGetAllIsUsed_thenNearCacheShouldBePopulated() {
        whenGetIsUsed_thenNearCacheShouldBePopulated(DataStructureMethods.GET_ALL);
    }

    protected void whenGetIsUsed_thenNearCacheShouldBePopulated(DataStructureMethods method) {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        assumeThatMethodIsAvailable(context.nearCacheAdapter, method);

        // populate the data structure
        populateDataAdapter(context);
        assertNearCacheSize(context, 0);
        assertNearCacheStats(context, 0, 0, 0);

        // populate the Near Cache
        populateNearCache(context, method);
        assertNearCacheSizeEventually(context, DEFAULT_RECORD_COUNT);
        assertNearCacheStats(context, DEFAULT_RECORD_COUNT, 0, DEFAULT_RECORD_COUNT);

        // generate Near Cache hits
        populateNearCache(context, method);
        assertNearCacheSize(context, DEFAULT_RECORD_COUNT);
        assertNearCacheStats(context, DEFAULT_RECORD_COUNT, DEFAULT_RECORD_COUNT, DEFAULT_RECORD_COUNT);

        assertNearCacheContent(context, DEFAULT_RECORD_COUNT);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#GET_ALL} is used on a half
     * filled Near Cache and that the {@link com.hazelcast.monitor.NearCacheStats} are calculated correctly.
     */
    @Test
    public void whenGetAllWithHalfFilledNearCacheIsUsed_thenNearCacheShouldBePopulated() {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        assumeThatMethodIsAvailable(context.nearCacheAdapter, DataStructureMethods.GET_ALL);

        // populate the data structure
        populateDataAdapter(context);
        assertNearCacheSize(context, 0);
        assertNearCacheStats(context, 0, 0, 0);

        // populate the Near Cache with half of the entries
        int size = DEFAULT_RECORD_COUNT / 2;
        populateNearCache(context, DataStructureMethods.GET_ALL, size);
        assertNearCacheSizeEventually(context, size);
        assertNearCacheStats(context, size, 0, size);

        // generate Near Cache hits and populate the missing entries
        int expectedHits = size;
        populateNearCache(context, DataStructureMethods.GET_ALL);
        assertNearCacheSize(context, DEFAULT_RECORD_COUNT);
        assertNearCacheStats(context, DEFAULT_RECORD_COUNT, expectedHits, DEFAULT_RECORD_COUNT);

        // generate Near Cache hits
        expectedHits += DEFAULT_RECORD_COUNT;
        populateNearCache(context, DataStructureMethods.GET_ALL);
        assertNearCacheSize(context, DEFAULT_RECORD_COUNT);
        assertNearCacheStats(context, DEFAULT_RECORD_COUNT, expectedHits, DEFAULT_RECORD_COUNT);

        assertNearCacheContent(context, DEFAULT_RECORD_COUNT);
    }

    /**
     * Checks that the Near Cache is not populated when {@link DataStructureMethods#GET_ALL} is used with an empty key set.
     */
    @Test
    public void whenGetAllWithEmptySetIsUsed_thenNearCacheShouldNotBePopulated() {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        assumeThatMethodIsAvailable(context.nearCacheAdapter, DataStructureMethods.GET_ALL);

        // populate the data structure
        populateDataAdapter(context);
        assertNearCacheSize(context, 0);
        assertNearCacheStats(context, 0, 0, 0);

        // use getAll() with an empty set, which should not populate the Near Cache
        context.nearCacheAdapter.getAll(Collections.<Integer>emptySet());
        assertNearCacheSize(context, 0);
        assertNearCacheStats(context, 0, 0, 0);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#SET} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenSetIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.SET);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#PUT} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenPutIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.PUT);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#PUT_IF_ABSENT} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenPutIfAbsentIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.PUT_IF_ABSENT);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#PUT_IF_ABSENT_ASYNC} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenPutIfAbsentAsyncIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.PUT_IF_ABSENT_ASYNC);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#REPLACE} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenReplaceIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.REPLACE);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#REPLACE_WITH_OLD_VALUE} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenReplaceWithOldValueIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.REPLACE_WITH_OLD_VALUE);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#PUT_ALL} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenPutAllIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.PUT_ALL);
    }

    private void whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods method) {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        DataStructureAdapter<Integer, String> adapter = context.nearCacheAdapter;
        assumeThatLocalUpdatePolicyIsCacheOnUpdate(context);
        assumeThatMethodIsAvailable(adapter, method);

        Map<Integer, String> putAllMap = new HashMap<Integer, String>(DEFAULT_RECORD_COUNT);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String value = "value-" + i;
            switch (method) {
                case SET:
                    adapter.set(i, value);
                    break;
                case PUT:
                    assertNull(adapter.put(i, value));
                    break;
                case PUT_IF_ABSENT:
                    assertTrue(adapter.putIfAbsent(i, value));
                    break;
                case PUT_IF_ABSENT_ASYNC:
                    assertTrue(getFuture(adapter.putIfAbsentAsync(i, value), "Could not put value via putIfAbsentAsync()"));
                    break;
                case REPLACE:
                    assertNull(adapter.replace(i, value));
                    break;
                case REPLACE_WITH_OLD_VALUE:
                    String oldValue = "oldValue-" + i;
                    context.dataAdapter.put(i, oldValue);
                    assertTrue(adapter.replace(i, oldValue, value));
                    break;
                case PUT_ALL:
                    putAllMap.put(i, value);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected method: " + method);
            }
        }
        if (method == DataStructureMethods.PUT_ALL) {
            adapter.putAll(putAllMap);
        }

        String message = format("Population is not working on %s()", method.getMethodName());
        assertNearCacheSizeEventually(context, DEFAULT_RECORD_COUNT, message);
        assertNearCacheContent(context, DEFAULT_RECORD_COUNT);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#SET} is used.
     */
    @Test
    public void whenSetIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.SET);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#SET} is used.
     */
    @Test
    public void whenSetIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.SET);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT} is used.
     */
    @Test
    public void whenPutIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.PUT);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT} is used.
     */
    @Test
    public void whenPutIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.PUT);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REPLACE} is used.
     */
    @Test
    public void whenReplaceIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REPLACE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REPLACE} is used.
     */
    @Test
    public void whenReplaceIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REPLACE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REPLACE_WITH_OLD_VALUE} is used.
     */
    @Test
    public void whenReplaceWithOldValueIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REPLACE_WITH_OLD_VALUE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REPLACE_WITH_OLD_VALUE} is used.
     */
    @Test
    public void whenReplaceWithOldValueIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REPLACE_WITH_OLD_VALUE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#INVOKE} is used.
     */
    @Test
    public void whenInvokeIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.INVOKE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#INVOKE} is used.
     */
    @Test
    public void whenInvokeIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.INVOKE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_KEY} is used.
     */
    @Test
    public void whenExecuteOnKeyIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.EXECUTE_ON_KEY);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_KEY} is used.
     */
    @Test
    public void whenExecuteOnKeyIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.EXECUTE_ON_KEY);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_KEYS} is used.
     */
    @Test
    public void whenExecuteOnKeysIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.EXECUTE_ON_KEYS);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_KEYS} is used.
     */
    @Test
    public void whenExecuteOnKeysIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.EXECUTE_ON_KEYS);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_ALL} is used.
     */
    @Test
    public void whenPutAllIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.PUT_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_ALL} is used.
     */
    @Test
    public void whenPutAllIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.PUT_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#INVOKE_ALL} is used.
     */
    @Test
    public void whenInvokeAllIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.INVOKE_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#INVOKE_ALL} is used.
     */
    @Test
    public void whenInvokeAllIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.INVOKE_ALL);
    }

    /**
     * With the {@link NearCacheTestContext#dataAdapter} we have to set {@link NearCacheConfig#setInvalidateOnChange(boolean)}.
     * With the {@link NearCacheTestContext#nearCacheAdapter} Near Cache invalidations are not needed.
     */
    private void whenEntryIsChanged_thenNearCacheShouldBeInvalidated(boolean useDataAdapter, DataStructureMethods method) {
        nearCacheConfig.setInvalidateOnChange(useDataAdapter);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        DataStructureAdapter<Integer, String> adapter = useDataAdapter ? context.dataAdapter : context.nearCacheAdapter;
        if (!ENTRY_PROCESSOR_METHODS.contains(method)) {
            // since EntryProcessors return a user-defined result we cannot directly put this into the Near Cache,
            // so we execute this test also for CACHE_ON_UPDATE configurations
            assumeThatLocalUpdatePolicyIsInvalidate(context);
        }
        assumeThatMethodIsAvailable(adapter, method);

        populateDataAdapter(context);
        populateNearCache(context);

        // this should invalidate the Near Cache
        Map<Integer, String> invalidationMap = new HashMap<Integer, String>(DEFAULT_RECORD_COUNT);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String value = "value-" + i;
            String newValue = "newValue-" + i;
            switch (method) {
                case SET:
                    adapter.set(i, newValue);
                    break;
                case PUT:
                    assertEquals(value, adapter.put(i, newValue));
                    break;
                case REPLACE:
                    assertEquals(value, adapter.replace(i, newValue));
                    break;
                case REPLACE_WITH_OLD_VALUE:
                    assertTrue(adapter.replace(i, value, newValue));
                    break;
                case INVOKE:
                    assertEquals(newValue, adapter.invoke(i, new ICacheReplaceEntryProcessor(), "value", "newValue"));
                    break;
                case EXECUTE_ON_KEY:
                    assertEquals(newValue, adapter.executeOnKey(i, new IMapReplaceEntryProcessor("value", "newValue")));
                    break;
                case EXECUTE_ON_KEYS:
                case PUT_ALL:
                case INVOKE_ALL:
                    invalidationMap.put(i, newValue);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected method: " + method);
            }
        }
        if (method == DataStructureMethods.EXECUTE_ON_KEYS) {
            Map<Integer, Object> resultMap = adapter.executeOnKeys(invalidationMap.keySet(),
                    new IMapReplaceEntryProcessor("value", "newValue"));
            assertEquals(DEFAULT_RECORD_COUNT, resultMap.size());
            for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
                assertEquals("newValue-" + i, resultMap.get(i));
            }
        } else if (method == DataStructureMethods.PUT_ALL) {
            adapter.putAll(invalidationMap);
        } else if (method == DataStructureMethods.INVOKE_ALL) {
            Map<Integer, EntryProcessorResult<String>> resultMap = adapter.invokeAll(invalidationMap.keySet(),
                    new ICacheReplaceEntryProcessor(), "value", "newValue");
            assertEquals(DEFAULT_RECORD_COUNT, resultMap.size());
            for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
                assertEquals("newValue-" + i, resultMap.get(i).get());
            }
        }

        String message = format("Invalidation is not working on %s()", method.getMethodName());
        assertNearCacheSizeEventually(context, 0, message);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertEquals("newValue-" + i, context.dataAdapter.get(i));
        }
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#LOAD_ALL} is used.
     */
    @Test
    public void whenLoadAllIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.LOAD_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#LOAD_ALL} is used.
     */
    @Test
    public void whenLoadAllIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.LOAD_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#LOAD_ALL_WITH_KEYS} is used.
     */
    @Test
    public void whenLoadAllWithKeysIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.LOAD_ALL_WITH_KEYS);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#LOAD_ALL_WITH_KEYS} is used.
     */
    @Test
    public void whenLoadAllWithKeysIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.LOAD_ALL_WITH_KEYS);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#LOAD_ALL_WITH_LISTENER} is used.
     */
    @Test
    public void whenLoadAllWithListenerIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.LOAD_ALL_WITH_LISTENER);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#LOAD_ALL_WITH_LISTENER} is used.
     */
    @Test
    public void whenLoadAllWithListenerIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.LOAD_ALL_WITH_LISTENER);
    }

    /**
     * With the {@link NearCacheTestContext#dataAdapter} we have to set {@link NearCacheConfig#setInvalidateOnChange(boolean)}.
     * With the {@link NearCacheTestContext#nearCacheAdapter} Near Cache invalidations are not needed.
     */
    private void whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(boolean useDataAdapter, DataStructureMethods method) {
        nearCacheConfig.setInvalidateOnChange(useDataAdapter);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext(true);
        DataStructureAdapter<Integer, String> adapter = useDataAdapter ? context.dataAdapter : context.nearCacheAdapter;
        assumeThatMethodIsAvailable(adapter, method);

        // wait until the initial load is done and the cluster is in a safe state
        waitUntilLoaded(context);
        warmupPartitionsAndWaitForAllSafeState(context);

        populateDataAdapter(context);
        populateNearCache(context);

        // this should invalidate the Near Cache
        Set<Integer> keys = new HashSet<Integer>(DEFAULT_RECORD_COUNT);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            keys.add(i);
        }
        switch (method) {
            case LOAD_ALL:
                context.loader.setKeys(keys);
                adapter.loadAll(true);
                break;
            case LOAD_ALL_WITH_KEYS:
                adapter.loadAll(keys, true);
                break;
            case LOAD_ALL_WITH_LISTENER:
                ICacheCompletionListener listener = new ICacheCompletionListener();
                adapter.loadAll(keys, true, listener);
                listener.await();
                break;
            default:
                throw new IllegalArgumentException("Unexpected method: " + method);
        }

        // wait until the loader is finished and validate the updated values
        waitUntilLoaded(context);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertEquals("newValue-" + i, context.dataAdapter.get(i));
        }

        String message = format("Invalidation is not working on %s()", method.getMethodName());
        assertNearCacheSizeEventually(context, 0, message);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE} is used.
     */
    @Test
    public void whenRemoveIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REMOVE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE} is used.
     */
    @Test
    public void whenRemoveIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REMOVE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ASYNC} is used.
     */
    @Test
    public void whenRemoveAsyncIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REMOVE_ASYNC);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ASYNC} is used.
     */
    @Test
    public void whenRemoveAsyncIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REMOVE_ASYNC);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_WITH_OLD_VALUE)} is used.
     */
    @Test
    public void whenRemoveWithOldValueIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REMOVE_WITH_OLD_VALUE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_WITH_OLD_VALUE)} is used.
     */
    @Test
    public void whenRemoveWithOldValueIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REMOVE_WITH_OLD_VALUE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ALL)} is used.
     */
    @Test
    public void whenRemoveAllIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REMOVE_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ALL)} is used.
     */
    @Test
    public void whenRemoveAllIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REMOVE_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ALL_WITH_KEYS)} is used.
     */
    @Test
    public void whenRemoveAllWithKeysIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REMOVE_ALL_WITH_KEYS);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ALL_WITH_KEYS)} is used.
     */
    @Test
    public void whenRemoveAllWithKeysIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REMOVE_ALL_WITH_KEYS);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#CLEAR)} is used.
     */
    @Test
    public void whenClearIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.CLEAR);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#CLEAR)} is used.
     */
    @Test
    public void whenClearIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.CLEAR);
    }

    /**
     * With the {@link NearCacheTestContext#dataAdapter} we have to set {@link NearCacheConfig#setInvalidateOnChange(boolean)}.
     * With the {@link NearCacheTestContext#nearCacheAdapter} Near Cache invalidations are not needed.
     */
    private void whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(boolean useDataAdapter, DataStructureMethods method) {
        nearCacheConfig.setInvalidateOnChange(useDataAdapter);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        DataStructureAdapter<Integer, String> adapter = useDataAdapter ? context.dataAdapter : context.nearCacheAdapter;
        assumeThatMethodIsAvailable(adapter, method);

        populateDataAdapter(context);
        populateNearCache(context);

        // this should invalidate the Near Cache
        Set<Integer> removeKeys = new HashSet<Integer>();
        if (method == DataStructureMethods.REMOVE_ALL) {
            adapter.removeAll();
        } else {
            for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
                String value = "value-" + i;
                switch (method) {
                    case REMOVE:
                        adapter.remove(i);
                        break;
                    case REMOVE_WITH_OLD_VALUE:
                        assertTrue(adapter.remove(i, value));
                        break;
                    case REMOVE_ASYNC:
                        assertEquals(value, getFuture(adapter.removeAsync(i), "Could not remove entry via removeAsync()"));
                        break;
                    case REMOVE_ALL_WITH_KEYS:
                        removeKeys.add(i);
                        break;
                    case CLEAR:
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected method: " + method);
                }
            }
        }
        if (method == DataStructureMethods.REMOVE_ALL_WITH_KEYS) {
            adapter.removeAll(removeKeys);
        } else if (method == DataStructureMethods.CLEAR) {
            adapter.clear();
        }

        String message = format("Invalidation is not working on %s()", method.getMethodName());
        assertNearCacheSizeEventually(context, 0, message);
    }

    /**
     * Checks that the Near Cache works correctly when {@link DataStructureMethods#CONTAINS_KEY} is used.
     */
    @Test
    public void testContainsKey_onNearCacheAdapter() {
        testContainsKey(false);
    }

    /**
     * Checks that the Near Cache works correctly when {@link DataStructureMethods#CONTAINS_KEY} is used.
     */
    @Test
    public void testContainsKey_onDataAdapter() {
        testContainsKey(true);
    }

    /**
     * With the {@link NearCacheTestContext#dataAdapter} we have to set {@link NearCacheConfig#setInvalidateOnChange(boolean)}.
     * With the {@link NearCacheTestContext#nearCacheAdapter} Near Cache invalidations are not needed.
     */
    private void testContainsKey(boolean useDataAdapter) {
        nearCacheConfig.setInvalidateOnChange(useDataAdapter);
        final NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        assumeThatMethodIsAvailable(context.nearCacheAdapter, DataStructureMethods.CONTAINS_KEY);

        // populate data structure
        context.dataAdapter.put(1, "value-1");
        context.dataAdapter.put(2, "value-2");
        context.dataAdapter.put(3, "value-3");

        // populate Near Cache
        context.nearCacheAdapter.get(1);
        context.nearCacheAdapter.get(2);
        context.nearCacheAdapter.get(3);

        assertTrue(context.nearCacheAdapter.containsKey(1));
        assertTrue(context.nearCacheAdapter.containsKey(2));
        assertTrue(context.nearCacheAdapter.containsKey(3));
        assertFalse(context.nearCacheAdapter.containsKey(4));
        assertFalse(context.nearCacheAdapter.containsKey(5));

        // remove a key which is in the Near Cache
        DataStructureAdapter<Integer, String> adapter = useDataAdapter ? context.dataAdapter : context.nearCacheAdapter;
        adapter.remove(1);
        adapter.remove(3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(context.nearCacheAdapter.containsKey(1));
                assertTrue(context.nearCacheAdapter.containsKey(2));
                assertFalse(context.nearCacheAdapter.containsKey(3));
                assertFalse(context.nearCacheAdapter.containsKey(4));
                assertFalse(context.nearCacheAdapter.containsKey(5));
            }
        });
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

        // populate data structure with an extra entry
        populateDataAdapter(context);
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

        populateDataAdapter(context);

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
        assertNearCacheSizeEventually(context, DEFAULT_RECORD_COUNT);
        assertThatMemoryCostsAreGreaterThanZero(context, nearCacheConfig.getInMemoryFormat());

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            context.nearCacheAdapter.remove(i);
        }

        // the Near Cache is empty, we shouldn't see memory costs anymore
        assertNearCacheSizeEventually(context, 0);
        assertThatMemoryCostsAreZero(context);
    }

    /**
     * Checks that the Near Cache never returns its internal {@link NearCache#CACHED_AS_NULL} to the public API.
     */
    @Test
    public void whenEmptyDataStructure_thenPopulatedNearCacheShouldReturnNull_neverCACHED_AS_NULL() {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            // populate Near Cache
            assertNull("Expected null from original data structure for key " + i, context.nearCacheAdapter.get(i));
            // fetch value from Near Cache
            assertNull("Expected null from near cached data structure for key " + i, context.nearCacheAdapter.get(i));

            // fetch internal value directly from Near Cache
            NV value = getFromNearCache(context, i);
            if (value != null) {
                // the internal value should either be `null` or `CACHED_AS_NULL`
                assertEquals("Expected CACHED_AS_NULL in Near Cache for key " + i, NearCache.CACHED_AS_NULL, value);
            }
        }
    }

    /**
     * Checks that the Near Cache updates value for keys which are already in the Near Cache,
     * even if the Near Cache is full an the eviction is disabled (via {@link com.hazelcast.config.EvictionPolicy#NONE}.
     */
    @Test
    public void whenNearCacheIsFull_thenPutOnSameKeyShouldUpdateValue_onNearCacheAdapter() {
        whenNearCacheIsFull_thenPutOnSameKeyShouldUpdateValue(false);
    }

    /**
     * Checks that the Near Cache updates value for keys which are already in the Near Cache,
     * even if the Near Cache is full an the eviction is disabled (via {@link com.hazelcast.config.EvictionPolicy#NONE}.
     */
    @Test
    public void whenNearCacheIsFull_thenPutOnSameKeyShouldUpdateValue_onDataAdapter() {
        whenNearCacheIsFull_thenPutOnSameKeyShouldUpdateValue(true);
    }

    /**
     * With the {@link NearCacheTestContext#dataAdapter} we have to set {@link NearCacheConfig#setInvalidateOnChange(boolean)}.
     * With the {@link NearCacheTestContext#nearCacheAdapter} Near Cache invalidations are not needed.
     */
    private void whenNearCacheIsFull_thenPutOnSameKeyShouldUpdateValue(boolean useDataAdapter) {
        int size = DEFAULT_RECORD_COUNT / 2;
        setEvictionConfig(nearCacheConfig, NONE, ENTRY_COUNT, size);
        nearCacheConfig.setInvalidateOnChange(useDataAdapter);

        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        DataStructureAdapter<Integer, String> adapter = useDataAdapter ? context.dataAdapter : context.nearCacheAdapter;
        boolean isNearCacheDirectlyUpdated = isCacheOnUpdate(context) && !useDataAdapter;

        populateDataAdapter(context);
        populateNearCache(context);

        // assert that the old value is present
        assertNearCacheSize(context, size);
        assertEquals("value-1", context.nearCacheAdapter.get(1));

        // update the entry
        adapter.put(1, "newValue");

        // wait for the invalidation to be processed
        int expectedSize = isNearCacheDirectlyUpdated ? size : size - 1;
        assertNearCacheSizeEventually(context, expectedSize);

        long expectedHits = context.stats.getHits() + (isNearCacheDirectlyUpdated ? 2 : 1);
        long expectedMisses = context.stats.getMisses() + (isNearCacheDirectlyUpdated ? 0 : 1);

        // assert that the new entry is updated
        assertEquals("newValue", context.nearCacheAdapter.get(1));
        assertEquals("newValue", context.nearCacheAdapter.get(1));

        assertNearCacheStats(context, size, expectedHits, expectedMisses);
    }
}
