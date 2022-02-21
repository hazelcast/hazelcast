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

package com.hazelcast.map.impl.tx;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods;
import com.hazelcast.internal.adapter.DataStructureAdapterMethod;
import com.hazelcast.internal.adapter.TransactionalMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.AbstractNearCacheBasicTest;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContext;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContextBuilder;
import com.hazelcast.internal.nearcache.impl.NearCacheTestUtils;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionalMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getMapNearCacheManager;
import static com.hazelcast.test.Accessors.getSerializationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Basic Near Cache tests for {@link TransactionalMap} on Hazelcast members.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TxnMapNearCacheBasicTest extends AbstractNearCacheBasicTest<Data, String> {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public boolean serializeKeys;

    private final int nodeCount = 2;
    private final TestHazelcastInstanceFactory hazelcastFactory = createHazelcastInstanceFactory(nodeCount);

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
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean loaderEnabled) {
        Config config = getConfig(false);

        HazelcastInstance dataInstance = hazelcastFactory.newHazelcastInstance(config);
        TransactionalMapDataStructureAdapter<K, V> dataAdapter
                = new TransactionalMapDataStructureAdapter<K, V>(dataInstance, DEFAULT_NEAR_CACHE_NAME);

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

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected Config getConfig(boolean withNearCache) {
        Config config = getConfig();

        if (withNearCache) {
            config.getMapConfig(DEFAULT_NEAR_CACHE_NAME).setNearCacheConfig(nearCacheConfig);
        }
        return config;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        Config configWithNearCache = getConfig(true);

        HazelcastInstance nearCacheInstance = hazelcastFactory.newHazelcastInstance(configWithNearCache);
        nearCacheInstance.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(nearCacheInstance);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(nearCacheInstance))
                .setNearCacheInstance(nearCacheInstance)
                .setNearCacheAdapter(new TransactionalMapDataStructureAdapter<K, V>(nearCacheInstance, DEFAULT_NEAR_CACHE_NAME))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager);
    }

    /**
     * The {@link TransactionalMap} doesn't populate the Near Cache, so we override this method.
     */
    @Override
    protected void populateNearCache(NearCacheTestContext<Integer, String, ?, ?> context, DataStructureMethods method,
                                     int size, String valuePrefix) {
        switch (method) {
            case GET:
                IMap<Integer, String> map = context.nearCacheInstance.getMap(DEFAULT_NEAR_CACHE_NAME);
                for (int i = 0; i < size; i++) {
                    Object value = map.get(i);
                    if (valuePrefix == null) {
                        assertNull(value);
                    } else {
                        assertEquals(valuePrefix + i, value);
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("Unexpected method: " + method);
        }
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
}
