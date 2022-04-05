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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.AbstractNearCacheLeakTest;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContext;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContextBuilder;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
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

/**
 * Basic Near Cache tests for {@link IMap} on Hazelcast members.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class MapNearCacheLeakTest extends AbstractNearCacheLeakTest<Data, String> {

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
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(true);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config config = getConfig(false);

        HazelcastInstance dataInstance = hazelcastFactory.newHazelcastInstance(config);
        IMap<K, V> dataMap = dataInstance.getMap(DEFAULT_NEAR_CACHE_NAME);
        IMapDataStructureAdapter<K, V> dataAdapter = new IMapDataStructureAdapter<K, V>(dataMap);

        // wait until the initial load is done
        dataAdapter.waitUntilLoaded();

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder
                .setDataInstance(dataInstance)
                .setDataAdapter(dataAdapter)
                .build();
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected Config getConfig(boolean withNearCache) {
        Config config = getConfig();

        MapConfig mapConfig = config.getMapConfig(DEFAULT_NEAR_CACHE_NAME);
        if (withNearCache) {
            mapConfig.setNearCacheConfig(nearCacheConfig);
        }

        return config;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        Config configWithNearCache = getConfig(true);

        HazelcastInstance nearCacheInstance = hazelcastFactory.newHazelcastInstance(configWithNearCache);
        IMap<K, V> nearCacheMap = nearCacheInstance.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(nearCacheInstance);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        RepairingTask repairingTask = ((MapNearCacheManager) nearCacheManager).getRepairingTask();

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(nearCacheInstance))
                .setNearCacheInstance(nearCacheInstance)
                .setNearCacheAdapter(new IMapDataStructureAdapter<K, V>(nearCacheMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setHasLocalData(true)
                .setRepairingTask(repairingTask);
    }
}
