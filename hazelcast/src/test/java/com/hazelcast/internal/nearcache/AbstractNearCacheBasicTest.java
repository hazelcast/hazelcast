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
import com.hazelcast.internal.adapter.ICacheReplaceEntryProcessor;
import com.hazelcast.internal.adapter.IMapReplaceEntryProcessor;
import com.hazelcast.monitor.NearCacheStats;
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
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheEvictionsEventually;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSize;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSizeEventually;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheStats;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertThatMemoryCostsAreGreaterThanZero;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertThatMemoryCostsAreZero;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assumeThatLocalUpdatePolicyIsCacheOnUpdate;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assumeThatLocalUpdatePolicyIsInvalidate;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assumeThatMethodIsAvailable;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getFuture;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.isCacheOnUpdate;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.setEvictionConfig;
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
            String value = context.nearCacheAdapter.get(i);
            assertEquals("value-" + i, value);
        }
    }

    protected void populateNearCache(NearCacheTestContext<Integer, String, NK, NV> context, DataStructureMethods method) {
        switch (method) {
            case GET:
                populateNearCache(context);
                break;
            case GET_ASYNC:
                List<Future<String>> futures = new ArrayList<Future<String>>(DEFAULT_RECORD_COUNT);
                for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
                    futures.add(context.nearCacheAdapter.getAsync(i));
                }
                for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
                    assertEquals("value-" + i, getFuture(futures.get(i), "Could not get value via getAsync()"));
                }
                break;
            case GET_ALL:
                Set<Integer> getAllSet = new HashSet<Integer>(DEFAULT_RECORD_COUNT);
                for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
                    getAllSet.add(i);
                }
                Map<Integer, String> resultMap = context.nearCacheAdapter.getAll(getAllSet);
                assertEquals(DEFAULT_RECORD_COUNT, resultMap.size());
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
        populateMap(context);
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
    }

    @Test
    public void whenGetAllWithEmptySetIsUsed_thenNearCacheShouldNotBePopulated() {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        assumeThatMethodIsAvailable(context.nearCacheAdapter, DataStructureMethods.GET_ALL);

        // populate the data structure
        populateMap(context);
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
        assumeThatLocalUpdatePolicyIsCacheOnUpdate(nearCacheConfig);

        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        DataStructureAdapter<Integer, String> adapter = context.nearCacheAdapter;
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
                    context.dataAdapter.put(i, value);
                    assertTrue(adapter.replace(i, value, "newValue-" + i));
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
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#SET} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenSetIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.SET);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#SET} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenSetIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.SET);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenPutIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.PUT);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenPutIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.PUT);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REPLACE} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenReplaceIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REPLACE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REPLACE} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenReplaceIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REPLACE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REPLACE_WITH_OLD_VALUE} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenReplaceWithOldValueIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REPLACE_WITH_OLD_VALUE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REPLACE_WITH_OLD_VALUE} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenReplaceWithOldValueIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REPLACE_WITH_OLD_VALUE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#INVOKE} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenInvokeIsUsed_thenNearCacheIsInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.INVOKE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#INVOKE} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenInvokeIsUsed_thenNearCacheIsInvalidated_withUpdateOnDataCacheAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.INVOKE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_KEY} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenExecuteOnKeyIsUsed_thenNearCacheIsInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.EXECUTE_ON_KEY);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_KEY} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenExecuteOnKeyIsUsed_thenNearCacheIsInvalidated_withUpdateOnDataCacheAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.EXECUTE_ON_KEY);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_KEYS} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenExecuteOnKeysIsUsed_thenNearCacheIsInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.EXECUTE_ON_KEYS);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_KEYS} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenExecuteOnKeysIsUsed_thenNearCacheIsInvalidated_withUpdateOnDataCacheAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.EXECUTE_ON_KEYS);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_ALL} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenPutAllIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.PUT_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_ALL} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenPutAllIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.PUT_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#INVOKE_ALL} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenInvokeAllIsUsed_thenNearCacheIsInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.INVOKE_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#INVOKE_ALL} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenInvokeAllIsUsed_thenNearCacheIsInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.INVOKE_ALL);
    }

    private void whenEntryIsChanged_thenNearCacheShouldBeInvalidated(boolean useNearCacheAdapter, DataStructureMethods method) {
        assumeThatLocalUpdatePolicyIsInvalidate(nearCacheConfig);

        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        DataStructureAdapter<Integer, String> adapter = useNearCacheAdapter ? context.nearCacheAdapter : context.dataAdapter;
        assumeThatMethodIsAvailable(adapter, method);

        populateMap(context);
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
                case PUT_ALL:
                case INVOKE_ALL:
                case EXECUTE_ON_KEYS:
                    invalidationMap.put(i, newValue);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected method: " + method);
            }
        }
        if (method == DataStructureMethods.PUT_ALL) {
            adapter.putAll(invalidationMap);
        } else if (method == DataStructureMethods.INVOKE_ALL) {
            Map<Integer, EntryProcessorResult<String>> resultMap = adapter.invokeAll(invalidationMap.keySet(),
                    new ICacheReplaceEntryProcessor(), "value", "newValue");
            assertEquals(DEFAULT_RECORD_COUNT, resultMap.size());
            for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
                assertEquals("newValue-" + i, resultMap.get(i).get());
            }
        } else if (method == DataStructureMethods.EXECUTE_ON_KEYS) {
            Map<Integer, Object> resultMap = adapter.executeOnKeys(invalidationMap.keySet(),
                    new IMapReplaceEntryProcessor("value", "newValue"));
            assertEquals(DEFAULT_RECORD_COUNT, resultMap.size());
            for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
                assertEquals("newValue-" + i, resultMap.get(i));
            }
        }

        String message = format("Invalidation is not working on %s()", method.getMethodName());
        assertNearCacheSizeEventually(context, 0, message);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenRemoveIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REMOVE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenRemoveIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REMOVE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ASYNC} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenRemoveAsyncIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REMOVE_ASYNC);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ASYNC} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenRemoveAsyncIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REMOVE_ASYNC);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_WITH_OLD_VALUE)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenRemoveWithOldValueIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REMOVE_WITH_OLD_VALUE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_WITH_OLD_VALUE)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenRemoveWithOldValueIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REMOVE_WITH_OLD_VALUE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ALL)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenRemoveAllIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REMOVE_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ALL)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenRemoveAllIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REMOVE_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ALL_WITH_KEYS)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#nearCacheAdapter}, so there is no Near Cache invalidation necessary.
     */
    @Test
    public void whenRemoveAllWithKeysIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.REMOVE_ALL_WITH_KEYS);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#REMOVE_ALL_WITH_KEYS)} is used.
     *
     * This variant uses the {@link NearCacheTestContext#dataAdapter}, so we need to configure Near Cache invalidation.
     */
    @Test
    public void whenRemoveAllWithKeysIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnDataAdapter() {
        nearCacheConfig.setInvalidateOnChange(true);
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.REMOVE_ALL_WITH_KEYS);
    }

    private void whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(boolean useNearCacheAdapter, DataStructureMethods method) {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        DataStructureAdapter<Integer, String> adapter = useNearCacheAdapter ? context.nearCacheAdapter : context.dataAdapter;
        assumeThatMethodIsAvailable(adapter, method);

        populateMap(context);
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
                    default:
                        throw new IllegalArgumentException("Unexpected method: " + method);
                }
            }
        }
        if (method == DataStructureMethods.REMOVE_ALL_WITH_KEYS) {
            adapter.removeAll(removeKeys);
        }

        String message = format("Invalidation is not working on %s()", method.getMethodName());
        assertNearCacheSizeEventually(context, 0, message);
    }

    /**
     * Checks that the Near Cache works correctly when {@link DataStructureMethods#CONTAINS_KEY} is used.
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
        assumeThatMethodIsAvailable(context.nearCacheAdapter, DataStructureMethods.CONTAINS_KEY);

        // populate map
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
        DataStructureAdapter<Integer, String> adapter = useNearCacheAdapter ? context.nearCacheAdapter : context.dataAdapter;
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
    public void whenEmptyMap_thenPopulatedNearCacheShouldReturnNull_neverCACHED_AS_NULL() {
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            // populate Near Cache
            assertNull("Expected null from original data structure for key " + i, context.nearCacheAdapter.get(i));
            // fetch value from Near Cache
            assertNull("Expected null from near cached data structure for key " + i, context.nearCacheAdapter.get(i));

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

    @SuppressWarnings("unchecked")
    private NK getNearCacheKey(NearCacheTestContext<Integer, String, NK, NV> context, int key) {
        return (NK) context.serializationService.toData(key);
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

        long expectedMisses = getExpectedMissesWithLocalUpdatePolicy(context.stats);
        long expectedHits = getExpectedHitsWithLocalUpdatePolicy(context.stats);

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
                long expectedMisses = getExpectedMissesWithLocalUpdatePolicy(context.stats);
                long expectedHits = getExpectedHitsWithLocalUpdatePolicy(context.stats);

                assertEquals("newValue", context.nearCacheAdapter.get(1));
                assertEquals("newValue", context.nearCacheAdapter.get(1));

                assertNearCacheStats(context, size, expectedHits, expectedMisses);
            }
        });
    }

    private long getExpectedMissesWithLocalUpdatePolicy(NearCacheStats stats) {
        if (isCacheOnUpdate(nearCacheConfig)) {
            // we expect the first and second get() to be hits, since the value should be already be cached
            return stats.getMisses();
        }
        // we expect the first get() to be a miss, due to the replaced / invalidated value
        return stats.getMisses() + 1;
    }

    private long getExpectedHitsWithLocalUpdatePolicy(NearCacheStats stats) {
        if (isCacheOnUpdate(nearCacheConfig)) {
            // we expect the first and second get() to be hits, since the value should be already be cached
            return stats.getHits() + 2;
        }
        // we expect the second get() to be a hit, since it should be served from the Near Cache
        return stats.getHits() + 1;
    }
}
