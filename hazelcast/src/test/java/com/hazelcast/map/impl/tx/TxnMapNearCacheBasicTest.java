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

package com.hazelcast.map.impl.tx;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods;
import com.hazelcast.internal.adapter.DataStructureAdapterMethod;
import com.hazelcast.internal.adapter.TransactionalMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCacheBasicTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.internal.nearcache.NearCacheTestContextBuilder;
import com.hazelcast.internal.nearcache.NearCacheTestUtils;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSize;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSizeEventually;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheStats;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getMapNearCacheManager;
import static com.hazelcast.map.impl.nearcache.MapInvalidationListener.createInvalidationEventHandler;
import static java.util.Arrays.asList;

/**
 * Basic Near Cache tests for {@link com.hazelcast.core.TransactionalMap} on Hazelcast members.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class TxnMapNearCacheBasicTest extends AbstractNearCacheBasicTest<Data, String> {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public boolean serializeKeys;

    private final TestHazelcastInstanceFactory hazelcastFactory = createHazelcastInstanceFactory(2);

    @Parameters(name = "format:{0} serializeKeys:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false},
        });
    }

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(inMemoryFormat, serializeKeys)
                .setCacheLocalEntries(true)
                // we have to configure invalidation, otherwise the Near Cache in the TransactionalMap will not be used
                .setInvalidateOnChange(true);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Override
    protected void assumeThatMethodIsAvailable(DataStructureAdapterMethod method) {
        NearCacheTestUtils.assumeThatMethodIsAvailable(TransactionalMapDataStructureAdapter.class, method);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(int size, boolean loaderEnabled) {
        Config config = createConfig(false);

        HazelcastInstance dataInstance = hazelcastFactory.newHazelcastInstance(config);
        TransactionalMapDataStructureAdapter<K, V> dataAdapter
                = new TransactionalMapDataStructureAdapter<K, V>(dataInstance, DEFAULT_NEAR_CACHE_NAME);

        populateDataAdapter(dataAdapter, size);

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder
                .setDataInstance(dataInstance)
                .setDataAdapter(dataAdapter)
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder.build();
    }

    protected Config createConfig(boolean withNearCache) {
        Config config = getConfig()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), PARTITION_COUNT);

        if (withNearCache) {
            config.getMapConfig(DEFAULT_NEAR_CACHE_NAME).setNearCacheConfig(nearCacheConfig);
        }
        return config;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        Config configWithNearCache = createConfig(true);

        HazelcastInstance nearCacheInstance = hazelcastFactory.newHazelcastInstance(configWithNearCache);
        IMap<K, V> nearCacheMap = nearCacheInstance.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(nearCacheInstance);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(nearCacheInstance))
                .setNearCacheInstance(nearCacheInstance)
                .setNearCacheAdapter(new TransactionalMapDataStructureAdapter<K, V>(nearCacheInstance, DEFAULT_NEAR_CACHE_NAME))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setInvalidationListener(createInvalidationEventHandler(nearCacheMap));
    }

    /**
     * The {@link com.hazelcast.core.TransactionalMap} doesn't populate the Near Cache, so we override this test.
     */
    @Override
    protected void whenGetIsUsed_thenNearCacheShouldBePopulated(DataStructureMethods method) {
        assumeThatMethodIsAvailable(method);
        NearCacheTestContext<Integer, String, Data, String> context = createContext();

        // populate the data structure
        assertNearCacheSize(context, 0);
        assertNearCacheStats(context, 0, 0, 0);

        // use TransactionalMap.get() which reads from the Near Cache, but doesn't populate it (so we just create misses)
        populateNearCache(context, method);
        assertNearCacheSize(context, 0);
        assertNearCacheStats(context, 0, 0, DEFAULT_RECORD_COUNT);

        // use IMap.get() which populates the Near Cache (but also increases the misses again)
        IMap<Integer, String> map = context.nearCacheInstance.getMap(DEFAULT_NEAR_CACHE_NAME);
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            map.get(i);
        }
        assertNearCacheSizeEventually(context, DEFAULT_RECORD_COUNT);
        assertNearCacheStats(context, DEFAULT_RECORD_COUNT, 0, DEFAULT_RECORD_COUNT * 2);

        // use TransactionalMap.get() to make some hits
        populateNearCache(context, method);
        assertNearCacheSizeEventually(context, DEFAULT_RECORD_COUNT);
        assertNearCacheStats(context, DEFAULT_RECORD_COUNT, DEFAULT_RECORD_COUNT, DEFAULT_RECORD_COUNT * 2);
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void testNearCacheEviction() {
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void testNearCacheExpiration_withTTL() {
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void testNearCacheExpiration_withMaxIdle() {
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void testNearCacheMemoryCostCalculation() {
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void testNearCacheMemoryCostCalculation_withConcurrentCacheMisses() {
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void whenNearCacheIsFull_thenPutOnSameKeyShouldUpdateValue_onNearCacheAdapter() {
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void whenNearCacheIsFull_thenPutOnSameKeyShouldUpdateValue_onDataAdapter() {
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void whenValueIsUpdated_thenAnotherNearCacheContextShouldBeInvalidated() {
    }
}
