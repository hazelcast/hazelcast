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

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods;
import com.hazelcast.internal.adapter.DataStructureAdapterMethod;
import com.hazelcast.internal.adapter.ICacheCompletionListener;
import com.hazelcast.internal.adapter.ICacheReplaceEntryProcessor;
import com.hazelcast.internal.adapter.IMapReplaceEntryProcessor;
import com.hazelcast.internal.adapter.ReplicatedMapDataStructureAdapter;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import javax.cache.expiry.ExpiryPolicy;
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
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheInvalidations;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheInvalidationsBetween;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheReference;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSize;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSizeEventually;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheStats;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertThatMemoryCostsAreGreaterThanZero;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertThatMemoryCostsAreZero;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assumeThatLocalUpdatePolicyIsCacheOnUpdate;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assumeThatLocalUpdatePolicyIsInvalidate;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getFuture;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getNearCacheKey;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getValueFromNearCache;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.isCacheOnUpdate;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.setEvictionConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.waitUntilLoaded;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.HOURS;
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
     * Number of seconds to expire Near Cache entries via TTL.
     */
    protected static final int MAX_TTL_SECONDS = 2;

    /**
     * Number of seconds to expire idle Near Cache entries.
     */
    protected static final int MAX_IDLE_SECONDS = 1;

    /**
     * The default name used for the data structures which have a Near Cache.
     */
    protected static final String DEFAULT_NEAR_CACHE_NAME = "defaultNearCache";

    /**
     * The partition count to configure in the Hazelcast members.
     */
    protected static final String PARTITION_COUNT = "5";

    /**
     * Defines all {@link DataStructureMethods} which are using EntryProcessors.
     */
    private static final List<DataStructureMethods> ENTRY_PROCESSOR_METHODS = asList(
            DataStructureMethods.INVOKE,
            DataStructureMethods.EXECUTE_ON_KEY,
            DataStructureMethods.EXECUTE_ON_KEYS,
            DataStructureMethods.EXECUTE_ON_ENTRIES,
            DataStructureMethods.EXECUTE_ON_ENTRIES_WITH_PREDICATE,
            DataStructureMethods.INVOKE_ALL
    );

    /**
     * The {@link NearCacheConfig} used by the Near Cache tests.
     *
     * Needs to be set by the implementations of this class in their {@link org.junit.Before} methods.
     */
    protected NearCacheConfig nearCacheConfig;

    /**
     * Assumes that the {@link DataStructureAdapter} created by the Near Cache test supports a given
     * {@link DataStructureAdapterMethod}.
     *
     * @param method the {@link DataStructureAdapterMethod} to test for
     */
    protected abstract void assumeThatMethodIsAvailable(DataStructureAdapterMethod method);

    /**
     * Creates the {@link NearCacheTestContext} used by the Near Cache tests.
     *
     * The backing {@link DataStructureAdapter} will be populated with {@value DEFAULT_RECORD_COUNT} entries.
     *
     * @param <K> key type of the created {@link DataStructureAdapter}
     * @param <V> value type of the created {@link DataStructureAdapter}
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected final <K, V> NearCacheTestContext<K, V, NK, NV> createContext() {
        return createContext(DEFAULT_RECORD_COUNT, false);
    }

    /**
     * Creates the {@link NearCacheTestContext} used by the Near Cache tests.
     *
     * The backing {@link DataStructureAdapter} will be populated with {@value DEFAULT_RECORD_COUNT} entries.
     *
     * @param <K>  key type of the created {@link DataStructureAdapter}
     * @param <V>  value type of the created {@link DataStructureAdapter}
     * @param size determines the size the backing {@link DataStructureAdapter} should be populated with
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected final <K, V> NearCacheTestContext<K, V, NK, NV> createContext(int size) {
        return createContext(size, false);
    }

    /**
     * Creates the {@link NearCacheTestContext} used by the Near Cache tests.
     *
     * @param <K>           key type of the created {@link DataStructureAdapter}
     * @param <V>           value type of the created {@link DataStructureAdapter}
     * @param size          determines the size the backing {@link DataStructureAdapter} should be populated with
     * @param loaderEnabled determines if a loader should be configured
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createContext(int size, boolean loaderEnabled);

    /**
     * Creates the {@link NearCacheTestContext} with only a Near Cache instance used by the Near Cache tests.
     *
     * @param <K> key type of the created {@link com.hazelcast.internal.adapter.DataStructureAdapter}
     * @param <V> value type of the created {@link com.hazelcast.internal.adapter.DataStructureAdapter}
     * @return a {@link NearCacheTestContext} with only a client used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createNearCacheContext();

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
        assumeThatMethodIsAvailable(method);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        // assert that the Near Cache is empty
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
        assertNearCacheReferences(context, DEFAULT_RECORD_COUNT, method);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#GET_ALL} is used on a half
     * filled Near Cache and that the {@link com.hazelcast.monitor.NearCacheStats} are calculated correctly.
     */
    @Test
    public void whenGetAllWithHalfFilledNearCacheIsUsed_thenNearCacheShouldBePopulated() {
        assumeThatMethodIsAvailable(DataStructureMethods.GET_ALL);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        // assert that the Near Cache is empty
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
        assumeThatMethodIsAvailable(DataStructureMethods.GET_ALL);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        // assert that the Near Cache is empty
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
     * Checks that the Near Cache is populated when {@link DataStructureMethods#SET_ASYNC} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenSetAsyncIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.SET_ASYNC);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#SET_ASYNC_WITH_TTL} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenSetAsyncWithTtlIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.SET_ASYNC_WITH_TTL);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#SET_ASYNC_WITH_EXPIRY_POLICY} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenSetAsyncWithExpiryPolicyIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.SET_ASYNC_WITH_EXPIRY_POLICY);
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
     * Checks that the Near Cache is populated when {@link DataStructureMethods#PUT_ASYNC} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenPutAsyncIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.PUT_ASYNC);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#PUT_ASYNC_WITH_TTL} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenPutAsyncWithTtlIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.PUT_ASYNC_WITH_TTL);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#PUT_ASYNC_WITH_EXPIRY_POLICY} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenPutAsyncWithExpiryPolicyIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.PUT_ASYNC_WITH_EXPIRY_POLICY);
    }

    /**
     * Checks that the Near Cache is populated when {@link DataStructureMethods#PUT_TRANSIENT} with
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} is used.
     */
    @Test
    public void whenPutTransientIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureMethods.PUT_TRANSIENT);
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
        assumeThatMethodIsAvailable(method);
        assumeThatLocalUpdatePolicyIsCacheOnUpdate(nearCacheConfig);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext(0);
        DataStructureAdapter<Integer, String> adapter = context.nearCacheAdapter;

        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, HOURS);

        Map<Integer, String> putAllMap = new HashMap<Integer, String>(DEFAULT_RECORD_COUNT);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String value = "value-" + i;
            switch (method) {
                case SET:
                    adapter.set(i, value);
                    break;
                case SET_ASYNC:
                    getFuture(adapter.setAsync(i, value), "Could not set value via setAsync()");
                    break;
                case SET_ASYNC_WITH_TTL:
                    getFuture(adapter.setAsync(i, value, 1, HOURS), "Could not set value via setAsync() with TTL");
                    break;
                case SET_ASYNC_WITH_EXPIRY_POLICY:
                    getFuture(adapter.setAsync(i, value, expiryPolicy), "Could not set value via setAsync() with ExpiryPolicy");
                    break;
                case PUT:
                    assertNull(adapter.put(i, value));
                    break;
                case PUT_ASYNC:
                    assertNull(getFuture(adapter.putAsync(i, value), "Could not put value via putAsync()"));
                    break;
                case PUT_ASYNC_WITH_TTL:
                    assertNull(getFuture(adapter.putAsync(i, value, 1, HOURS), "Could not put value via putAsync() with TTL"));
                    break;
                case PUT_ASYNC_WITH_EXPIRY_POLICY:
                    ICompletableFuture<String> future = adapter.putAsync(i, value, expiryPolicy);
                    assertNull(getFuture(future, "Could not put value via putAsync() with ExpiryPolicy"));
                    break;
                case PUT_TRANSIENT:
                    adapter.putTransient(i, value, 1, HOURS);
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
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#SET_ASYNC} is used.
     */
    @Test
    public void whenSetAsyncIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.SET_ASYNC);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#SET_ASYNC} is used.
     */
    @Test
    public void whenSetAsyncIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.SET_ASYNC);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#SET_ASYNC_WITH_TTL} is used.
     */
    @Test
    public void whenSetAsyncWithTtlIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.SET_ASYNC_WITH_TTL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#SET_ASYNC_WITH_TTL} is used.
     */
    @Test
    public void whenSetAsyncWithTtlIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.SET_ASYNC_WITH_TTL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#SET_ASYNC_WITH_EXPIRY_POLICY}
     * is used.
     */
    @Test
    public void whenSetAsyncWithExpiryPolicyIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.SET_ASYNC_WITH_EXPIRY_POLICY);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#SET_ASYNC_WITH_EXPIRY_POLICY}
     * is used.
     */
    @Test
    public void whenSetAsyncWithExpiryPolicyIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.SET_ASYNC_WITH_EXPIRY_POLICY);
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
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_ASYNC} is used.
     */
    @Test
    public void whenPutAsyncIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.PUT_ASYNC);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_ASYNC} is used.
     */
    @Test
    public void whenPutAsyncIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.PUT_ASYNC);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_ASYNC_WITH_TTL} is used.
     */
    @Test
    public void whenPutAsyncWithTtlIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.PUT_ASYNC_WITH_TTL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_ASYNC_WITH_TTL} is used.
     */
    @Test
    public void whenPutAsyncWithTtlIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.PUT_ASYNC_WITH_TTL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_ASYNC_WITH_EXPIRY_POLICY}
     * is used.
     */
    @Test
    public void whenPutAsyncWithExpiryPolicyIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.PUT_ASYNC_WITH_EXPIRY_POLICY);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_ASYNC_WITH_EXPIRY_POLICY}
     * is used.
     */
    @Test
    public void whenPutAsyncWithExpiryPolicyIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.PUT_ASYNC_WITH_EXPIRY_POLICY);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_TRANSIENT}
     * is used.
     */
    @Test
    public void whenPutTransientIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.PUT_TRANSIENT);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#PUT_TRANSIENT}
     * is used.
     */
    @Test
    public void whenPutTransientIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.PUT_TRANSIENT);
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
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_ENTRIES} is used.
     */
    @Test
    public void whenExecuteOnEntriesIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.EXECUTE_ON_ENTRIES);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_ENTRIES} is used.
     */
    @Test
    public void whenExecuteOnEntriesIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.EXECUTE_ON_ENTRIES);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_ENTRIES_WITH_PREDICATE}
     * is used.
     */
    @Test
    public void whenExecuteOnEntriesWithPredicateIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.EXECUTE_ON_ENTRIES_WITH_PREDICATE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EXECUTE_ON_ENTRIES_WITH_PREDICATE}
     * is used.
     */
    @Test
    public void whenExecuteOnEntriesWithPredicateIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsChanged_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.EXECUTE_ON_ENTRIES_WITH_PREDICATE);
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
        assumeThatMethodIsAvailable(method);
        if (!ENTRY_PROCESSOR_METHODS.contains(method)) {
            // since EntryProcessors return a user-defined result we cannot directly put this into the Near Cache,
            // so we execute this test also for CACHE_ON_UPDATE configurations
            assumeThatLocalUpdatePolicyIsInvalidate(nearCacheConfig);
        }
        nearCacheConfig.setInvalidateOnChange(useDataAdapter);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        DataStructureAdapter<Integer, String> adapter = useDataAdapter ? context.dataAdapter : context.nearCacheAdapter;

        populateNearCache(context);

        // this should invalidate the Near Cache
        IMapReplaceEntryProcessor mapEntryProcessor = new IMapReplaceEntryProcessor("value", "newValue");
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, HOURS);
        Map<Integer, String> invalidationMap = new HashMap<Integer, String>(DEFAULT_RECORD_COUNT);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String value = "value-" + i;
            String newValue = "newValue-" + i;
            switch (method) {
                case SET:
                    adapter.set(i, newValue);
                    break;
                case SET_ASYNC:
                    getFuture(adapter.setAsync(i, newValue), "Could not set value via setAsync()");
                    break;
                case SET_ASYNC_WITH_TTL:
                    getFuture(adapter.setAsync(i, newValue, 1, HOURS), "Could not set value via setAsync() with TTL");
                    break;
                case SET_ASYNC_WITH_EXPIRY_POLICY:
                    ICompletableFuture<Void> voidFuture = adapter.setAsync(i, newValue, expiryPolicy);
                    getFuture(voidFuture, "Could not set value via setAsync() with ExpiryPolicy");
                    break;
                case PUT:
                    assertEquals(value, adapter.put(i, newValue));
                    break;
                case PUT_ASYNC:
                    assertEquals(value, getFuture(adapter.putAsync(i, newValue), "Could not put value via putAsync()"));
                    break;
                case PUT_ASYNC_WITH_TTL:
                    ICompletableFuture<String> ttlFuture = adapter.putAsync(i, newValue, 1, HOURS);
                    assertEquals(value, getFuture(ttlFuture, "Could not put value via putAsync() with TTL"));
                    break;
                case PUT_ASYNC_WITH_EXPIRY_POLICY:
                    ICompletableFuture<String> expiryFuture = adapter.putAsync(i, newValue, expiryPolicy);
                    assertEquals(value, getFuture(expiryFuture, "Could not put value via putAsync() with ExpiryPolicy"));
                    break;
                case PUT_TRANSIENT:
                    adapter.putTransient(i, newValue, 1, HOURS);
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
                    assertEquals(newValue, adapter.executeOnKey(i, mapEntryProcessor));
                    break;
                case EXECUTE_ON_KEYS:
                case PUT_ALL:
                case INVOKE_ALL:
                    invalidationMap.put(i, newValue);
                    break;
                case EXECUTE_ON_ENTRIES:
                case EXECUTE_ON_ENTRIES_WITH_PREDICATE:
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected method: " + method);
            }
        }
        if (method == DataStructureMethods.EXECUTE_ON_KEYS) {
            Map<Integer, Object> resultMap = adapter.executeOnKeys(invalidationMap.keySet(), mapEntryProcessor);
            assertResultMap(resultMap);
        } else if (method == DataStructureMethods.EXECUTE_ON_ENTRIES) {
            Map<Integer, Object> resultMap = adapter.executeOnEntries(mapEntryProcessor);
            assertResultMap(resultMap);
        } else if (method == DataStructureMethods.EXECUTE_ON_ENTRIES_WITH_PREDICATE) {
            Map<Integer, Object> resultMap = adapter.executeOnEntries(mapEntryProcessor, TruePredicate.INSTANCE);
            assertResultMap(resultMap);
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

        assertNearCacheInvalidations(context, DEFAULT_RECORD_COUNT);
        String message = format("Invalidation is not working on %s()", method.getMethodName());
        assertNearCacheSizeEventually(context, 0, message);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertEquals("newValue-" + i, context.dataAdapter.get(i));
        }
    }

    private void assertResultMap(Map<Integer, Object> resultMap) {
        assertEquals(DEFAULT_RECORD_COUNT, resultMap.size());
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertEquals("newValue-" + i, resultMap.get(i));
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
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EVICT} is used.
     */
    @Test
    public void whenEvictIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.EVICT);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EVICT} is used.
     */
    @Test
    public void whenEvictIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.EVICT);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EVICT_ALL} is used.
     */
    @Test
    public void whenEvictAllIsUsed_thenNearCacheIsInvalidated_onNearCacheAdapter() {
        whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.EVICT_ALL);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#EVICT_ALL} is used.
     */
    @Test
    public void whenEvictAllIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.EVICT_ALL);
    }

    /**
     * With the {@link NearCacheTestContext#dataAdapter} we have to set {@link NearCacheConfig#setInvalidateOnChange(boolean)}.
     * With the {@link NearCacheTestContext#nearCacheAdapter} Near Cache invalidations are not needed.
     */
    private void whenEntryIsLoaded_thenNearCacheShouldBeInvalidated(boolean useDataAdapter, DataStructureMethods method) {
        assumeThatMethodIsAvailable(method);
        nearCacheConfig.setInvalidateOnChange(useDataAdapter);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext(DEFAULT_RECORD_COUNT, true);
        DataStructureAdapter<Integer, String> adapter = useDataAdapter ? context.dataAdapter : context.nearCacheAdapter;

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
            case EVICT:
                for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
                    adapter.evict(i);
                }
                break;
            case EVICT_ALL:
                adapter.evictAll();
                break;
            default:
                throw new IllegalArgumentException("Unexpected method: " + method);
        }

        // wait until the loader is finished and validate the updated or evicted values
        waitUntilLoaded(context);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            if (method == DataStructureMethods.EVICT || method == DataStructureMethods.EVICT_ALL) {
                assertNull(context.dataAdapter.get(i));
            } else {
                assertEquals("newValue-" + i, context.dataAdapter.get(i));
            }
        }

        if (method == DataStructureMethods.EVICT_ALL) {
            assertNearCacheInvalidations(context, 1);
        } else {
            assertNearCacheInvalidationsBetween(context, DEFAULT_RECORD_COUNT, DEFAULT_RECORD_COUNT * 2);
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
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#DELETE} is used.
     */
    @Test
    public void whenDeleteIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.DELETE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#DELETE} is used.
     */
    @Test
    public void whenDeleteIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.DELETE);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#DELETE_ASYNC} is used.
     */
    @Test
    public void whenDeleteAsyncIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.DELETE_ASYNC);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#DELETE_ASYNC} is used.
     */
    @Test
    public void whenDeleteAsyncIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.DELETE_ASYNC);
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
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#DESTROY)} is used.
     */
    @Test
    public void whenDestroyIsUsed_thenNearCacheShouldBeInvalidated_onNearCacheAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(false, DataStructureMethods.DESTROY);
    }

    /**
     * Checks that the Near Cache is eventually invalidated when {@link DataStructureMethods#DESTROY)} is used.
     */
    @Test
    public void whenDestroyIsUsed_thenNearCacheShouldBeInvalidated_onDataAdapter() {
        whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(true, DataStructureMethods.DESTROY);
    }

    /**
     * With the {@link NearCacheTestContext#dataAdapter} we have to set {@link NearCacheConfig#setInvalidateOnChange(boolean)}.
     * With the {@link NearCacheTestContext#nearCacheAdapter} Near Cache invalidations are not needed.
     */
    private void whenEntryIsRemoved_thenNearCacheShouldBeInvalidated(boolean useDataAdapter, DataStructureMethods method) {
        assumeThatMethodIsAvailable(method);
        nearCacheConfig.setInvalidateOnChange(useDataAdapter);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext();
        DataStructureAdapter<Integer, String> adapter = useDataAdapter ? context.dataAdapter : context.nearCacheAdapter;

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
                        assertEquals(value, adapter.remove(i));
                        break;
                    case REMOVE_WITH_OLD_VALUE:
                        assertTrue(adapter.remove(i, value));
                        break;
                    case REMOVE_ASYNC:
                        assertEquals(value, getFuture(adapter.removeAsync(i), "Could not remove entry via removeAsync()"));
                        break;
                    case DELETE:
                        adapter.delete(i);
                        break;
                    case DELETE_ASYNC:
                        assertTrue(value, getFuture(adapter.deleteAsync(i), "Could not remove entry via deleteAsync()"));
                        break;
                    case REMOVE_ALL_WITH_KEYS:
                        removeKeys.add(i);
                        break;
                    case CLEAR:
                    case DESTROY:
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
        } else if (method == DataStructureMethods.DESTROY) {
            adapter.destroy();
        }

        if (method == DataStructureMethods.CLEAR) {
            // the ReplicatedMap fires its own map cleared event instead of a Near Cache invalidation
            if (!(adapter instanceof ReplicatedMapDataStructureAdapter)) {
                // there should be a single invalidation with a null key
                assertNearCacheInvalidations(context, 1);
            }
        } else if (method == DataStructureMethods.DESTROY) {
            // the ReplicatedMap fires its own map cleared event instead of a Near Cache invalidation
            if (!(adapter instanceof ReplicatedMapDataStructureAdapter)) {
                // there can be 1 or 2 invalidations, depending on when the invalidation listener is de-registered
                assertNearCacheInvalidationsBetween(context, 1, 2);
            }
        } else {
            // there should be invalidations for each key
            assertNearCacheInvalidations(context, DEFAULT_RECORD_COUNT);
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
        assumeThatMethodIsAvailable(DataStructureMethods.CONTAINS_KEY);
        nearCacheConfig.setInvalidateOnChange(useDataAdapter);
        final NearCacheTestContext<Integer, String, NK, NV> context = createContext(3);

        // populate Near Cache
        populateNearCache(context, DataStructureMethods.GET, 3);

        assertTrue(context.nearCacheAdapter.containsKey(0));
        assertTrue(context.nearCacheAdapter.containsKey(1));
        assertTrue(context.nearCacheAdapter.containsKey(2));
        assertFalse(context.nearCacheAdapter.containsKey(3));
        assertFalse(context.nearCacheAdapter.containsKey(4));

        // remove a key which is in the Near Cache
        DataStructureAdapter<Integer, String> adapter = useDataAdapter ? context.dataAdapter : context.nearCacheAdapter;
        adapter.remove(0);
        adapter.remove(2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(context.nearCacheAdapter.containsKey(0));
                assertTrue(context.nearCacheAdapter.containsKey(1));
                assertFalse(context.nearCacheAdapter.containsKey(2));
                assertFalse(context.nearCacheAdapter.containsKey(3));
                assertFalse(context.nearCacheAdapter.containsKey(4));
            }
        });
    }

    @Test
    public void whenNullKeyIsCached_thenContainsKeyShouldReturnFalse() {
        NearCacheTestContext<Integer, Integer, NK, NV> context = createContext(0);

        int absentKey = 1;
        assertNull("Returned value should be null", context.nearCacheAdapter.get(absentKey));
        assertFalse("containsKey() should return false", context.nearCacheAdapter.containsKey(absentKey));
    }

    /**
     * Checks that the Near Cache eviction works as expected if the Near Cache is full.
     */
    @Test
    public void testNearCacheEviction() {
        setEvictionConfig(nearCacheConfig, LRU, ENTRY_COUNT, DEFAULT_RECORD_COUNT);
        // create the NearCacheTestContext and populate the backing data structure with an extra entry
        NearCacheTestContext<Integer, String, NK, NV> context = createContext(DEFAULT_RECORD_COUNT + 1);

        // populate Near Caches
        populateNearCache(context);

        // all Near Cache implementations use the same eviction algorithm, which evicts a single entry
        int expectedEvictions = 1;

        // we expect (size + the extra entry - the expectedEvictions) entries in the Near Cache
        long expectedOwnedEntryCount = DEFAULT_RECORD_COUNT + 1 - expectedEvictions;
        long expectedHits = context.stats.getHits();
        long expectedMisses = context.stats.getMisses() + 1;

        // trigger eviction via fetching the extra entry
        context.nearCacheAdapter.get(DEFAULT_RECORD_COUNT);

        assertNearCacheEvictionsEventually(context, expectedEvictions);
        assertNearCacheStats(context, expectedOwnedEntryCount, expectedHits, expectedMisses, expectedEvictions, 0);
    }

    @Test
    public void testNearCacheExpiration_withTTL() {
        nearCacheConfig.setTimeToLiveSeconds(MAX_TTL_SECONDS);

        testNearCacheExpiration(MAX_TTL_SECONDS);
    }

    @Test
    public void testNearCacheExpiration_withMaxIdle() {
        nearCacheConfig.setMaxIdleSeconds(MAX_IDLE_SECONDS);

        testNearCacheExpiration(MAX_IDLE_SECONDS);
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    private void testNearCacheExpiration(int expireSeconds) {
        final NearCacheTestContext<Integer, String, NK, NV> context = createContext();

        populateNearCache(context);

        // we allow a difference of -1 here, since the NearCacheStats are not updated atomically,
        // so one entry could already be removed from the owned entry count, but not added to the
        // expirations yet (we also copy the stats, so there are no on-fly changes during the assert)
        NearCacheStats stats = new NearCacheStatsImpl(context.stats);
        assertTrue(format("All entries (beside one) should be in the Near Cache or expired from the Near Cache (%s)", stats),
                stats.getOwnedEntryCount() + stats.getExpirations() >= DEFAULT_RECORD_COUNT - 1
                        && stats.getOwnedEntryCount() + stats.getExpirations() <= DEFAULT_RECORD_COUNT);

        sleepSeconds(expireSeconds + 1);

        assertTrueEventually(new AssertTask() {
            public void run() {
                // the get() call triggers the Near Cache eviction/expiration process,
                // but we need to call this on every assert, since the Near Cache has a cooldown for TTL cleanups
                context.nearCacheAdapter.get(0);

                assertNearCacheSize(context, 1, "Expected the Near Cache to contain just the trigger entry");
                assertEquals("Expected no Near Cache evictions", 0, context.stats.getEvictions());
                assertTrue(format("Expected at least %d entries to be expired from the Near Cache", DEFAULT_RECORD_COUNT),
                        context.stats.getExpirations() >= DEFAULT_RECORD_COUNT);
            }
        });
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
        final NearCacheTestContext<Integer, String, NK, NV> context = createContext();

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
     * Checks that the Near Cache never returns its internal {@link NearCache#CACHED_AS_NULL} to the user
     * when {@link DataStructureMethods#GET} is used.
     */
    @Test
    public void whenGetIsUsedOnEmptyDataStructure_thenAlwaysReturnNullFromNearCache() {
        whenGetIsUsedOnEmptyDataStructure_thenAlwaysReturnNullFromNearCache(DataStructureMethods.GET);
    }

    /**
     * Checks that the Near Cache never returns its internal {@link NearCache#CACHED_AS_NULL} to the user
     * when {@link DataStructureMethods#GET_ASYNC} is used.
     */
    @Test
    public void whenGetAsyncIsUsedOnEmptyDataStructure_thenAlwaysReturnNullFromNearCache() {
        whenGetIsUsedOnEmptyDataStructure_thenAlwaysReturnNullFromNearCache(DataStructureMethods.GET_ASYNC);
    }

    /**
     * Checks that the Near Cache never returns its internal {@link NearCache#CACHED_AS_NULL} to the user
     * when {@link DataStructureMethods#GET_ALL} is used.
     */
    @Test
    public void whenGetAllIsUsedOnEmptyDataStructure_thenAlwaysReturnNullFromNearCache() {
        whenGetIsUsedOnEmptyDataStructure_thenAlwaysReturnNullFromNearCache(DataStructureMethods.GET_ALL);
    }

    /**
     * Checks that the Near Cache never returns its internal {@link NearCache#CACHED_AS_NULL} to the user.
     */
    private void whenGetIsUsedOnEmptyDataStructure_thenAlwaysReturnNullFromNearCache(DataStructureMethods method) {
        assumeThatMethodIsAvailable(method);
        NearCacheTestContext<Integer, String, NK, NV> context = createContext(0);

        // populate Near Cache and check for null values
        populateNearCache(context, method, DEFAULT_RECORD_COUNT, null);
        // fetch value from Near Cache and check for null values
        populateNearCache(context, method, DEFAULT_RECORD_COUNT, null);

        // fetch internal value directly from Near Cache
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            NV value = getValueFromNearCache(context, getNearCacheKey(context, i));
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
        boolean isNearCacheDirectlyUpdated = isCacheOnUpdate(nearCacheConfig) && !useDataAdapter;

        populateNearCache(context);

        // assert that the old value is present
        assertNearCacheSize(context, size);
        assertEquals("value-1", context.nearCacheAdapter.get(1));

        // update the entry
        adapter.put(1, "newValue");

        // wait for the invalidation to be processed
        int expectedSize = isNearCacheDirectlyUpdated ? size : size - 1;
        assertNearCacheSizeEventually(context, expectedSize);
        assertNearCacheInvalidations(context, 1);

        long expectedHits = context.stats.getHits() + (isNearCacheDirectlyUpdated ? 2 : 1);
        long expectedMisses = context.stats.getMisses() + (isNearCacheDirectlyUpdated ? 0 : 1);

        // assert that the new entry is updated
        assertEquals("newValue", context.nearCacheAdapter.get(1));
        assertEquals("newValue", context.nearCacheAdapter.get(1));

        assertNearCacheStats(context, size, expectedHits, expectedMisses);
    }

    @Test
    public void whenValueIsUpdated_thenAnotherNearCacheContextShouldBeInvalidated() {
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext<Integer, String, NK, NV> firstContext = createContext();
        NearCacheTestContext<Integer, String, NK, NV> secondContext = createNearCacheContext();

        // populate Near Cache in the second context
        populateNearCache(secondContext);

        // update values in the first context
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            firstContext.nearCacheAdapter.put(i, "newValue-" + i);
        }

        // wait until the second context is invalidated
        assertNearCacheSizeEventually(secondContext, 0);

        // populate the second context again with the updated values
        populateNearCache(secondContext, DataStructureMethods.GET, DEFAULT_RECORD_COUNT, "newValue-");
    }

    @Test
    public void whenValueIsRemoved_thenAnotherNearCacheContextCannotGetValueAgain() {
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext<Integer, String, NK, NV> firstContext = createContext();
        NearCacheTestContext<Integer, String, NK, NV> secondContext = createNearCacheContext();

        // populate Near Cache in the second context
        populateNearCache(secondContext);

        // remove values from the first context
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            firstContext.nearCacheAdapter.remove(i);
        }

        // wait until the second context is invalidated
        assertNearCacheSizeEventually(secondContext, 0);

        // populate the second context again with the updated values
        populateNearCache(secondContext, DataStructureMethods.GET, DEFAULT_RECORD_COUNT, null);
    }

    @Test
    public void whenDataStructureIsCleared_thenAnotherNearCacheContextCannotGetValuesAgain() {
        assumeThatMethodIsAvailable(DataStructureMethods.CLEAR);
        nearCacheConfig.setInvalidateOnChange(true);
        NearCacheTestContext<Integer, String, NK, NV> firstContext = createContext();
        NearCacheTestContext<Integer, String, NK, NV> secondContext = createNearCacheContext();

        // populate Near Cache in the second context
        populateNearCache(secondContext);

        // call clear() from the first context
        firstContext.nearCacheAdapter.clear();

        // wait until the second context is invalidated
        assertNearCacheSizeEventually(secondContext, 0);

        // populate the second context again with the updated values
        populateNearCache(secondContext, DataStructureMethods.GET, DEFAULT_RECORD_COUNT, null);
    }

    @SuppressWarnings("unchecked")
    protected static void populateDataAdapter(DataStructureAdapter<?, ?> dataAdapter, int size) {
        if (size < 1) {
            return;
        }
        DataStructureAdapter<Integer, String> adapter = (DataStructureAdapter<Integer, String>) dataAdapter;
        for (int i = 0; i < size; i++) {
            adapter.put(i, "value-" + i);
        }
    }

    protected static void populateNearCache(NearCacheTestContext<Integer, String, ?, ?> context) {
        populateNearCache(context, DataStructureMethods.GET);
    }

    protected static void populateNearCache(NearCacheTestContext<Integer, String, ?, ?> context, DataStructureMethods method) {
        populateNearCache(context, method, DEFAULT_RECORD_COUNT, "value-");
    }

    private static void populateNearCache(NearCacheTestContext<Integer, String, ?, ?> context, DataStructureMethods method,
                                          int size) {
        populateNearCache(context, method, size, "value-");
    }

    private static void populateNearCache(NearCacheTestContext<Integer, String, ?, ?> context, DataStructureMethods method,
                                          int size, String valuePrefix) {
        switch (method) {
            case GET:
                for (int i = 0; i < size; i++) {
                    Object value = context.nearCacheAdapter.get(i);
                    if (valuePrefix == null) {
                        assertNull(value);
                    } else {
                        assertEquals(valuePrefix + i, value);
                    }
                }
                break;
            case GET_ASYNC:
                List<Future<String>> futures = new ArrayList<Future<String>>(size);
                for (int i = 0; i < size; i++) {
                    futures.add(context.nearCacheAdapter.getAsync(i));
                }
                for (int i = 0; i < size; i++) {
                    Object value = getFuture(futures.get(i), "Could not get value for index " + i + " via getAsync()");
                    if (valuePrefix == null) {
                        assertNull(value);
                    } else {
                        assertEquals(valuePrefix + i, value);
                    }
                }
                break;
            case GET_ALL:
                Set<Integer> getAllSet = new HashSet<Integer>(size);
                for (int i = 0; i < size; i++) {
                    getAllSet.add(i);
                }
                Map<Integer, String> resultMap = context.nearCacheAdapter.getAll(getAllSet);
                int resultSize = resultMap.size();
                if (valuePrefix == null) {
                    if (context.nearCacheAdapter instanceof ReplicatedMapDataStructureAdapter) {
                        assertEquals(size, resultSize);
                        for (int i = 0; i < size; i++) {
                            assertNull(resultMap.get(i));
                        }
                    } else {
                        assertTrue("result map from getAll() should be empty, but was " + resultSize, resultMap.isEmpty());
                    }
                } else {
                    assertEquals(size, resultSize);
                    for (int i = 0; i < size; i++) {
                        Object value = resultMap.get(i);
                        assertEquals(valuePrefix + i, value);
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("Unexpected method: " + method);
        }
    }

    private static void assertNearCacheReferences(NearCacheTestContext<Integer, String, ?, ?> context, int size,
                                                  DataStructureMethods method) {
        if (context.nearCacheConfig.getInMemoryFormat() != InMemoryFormat.OBJECT) {
            return;
        }
        switch (method) {
            case GET:
                for (int i = 0; i < size; i++) {
                    Object value = context.nearCacheAdapter.get(i);
                    assertNearCacheReference(context, i, value);
                }
                break;
            case GET_ASYNC:
                for (int i = 0; i < size; i++) {
                    Object value = getFuture(context.nearCacheAdapter.getAsync(i), "");
                    assertNearCacheReference(context, i, value);
                }
                break;
            case GET_ALL:
                Set<Integer> getAllSet = new HashSet<Integer>(size);
                for (int i = 0; i < size; i++) {
                    getAllSet.add(i);
                }
                Map<Integer, String> resultMap = context.nearCacheAdapter.getAll(getAllSet);
                for (Map.Entry<Integer, String> entry : resultMap.entrySet()) {
                    assertNearCacheReference(context, entry.getKey(), entry.getValue());
                }
                break;
            default:
                throw new IllegalArgumentException("Unexpected method: " + method);
        }
    }
}
