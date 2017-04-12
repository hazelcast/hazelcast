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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.adapter.IMapMapStore;
import com.hazelcast.internal.nearcache.AbstractNearCacheBasicTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.nio.serialization.Data;
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

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getMapNearCacheManager;
import static java.util.Arrays.asList;

/**
 * Basic Near Cache tests for {@link IMap} on Hazelcast members.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({ParallelTest.class, QuickTest.class})
public class MapNearCacheBasicTest extends AbstractNearCacheBasicTest<Data, String> {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    private final TestHazelcastInstanceFactory hazelcastFactory = createHazelcastInstanceFactory();

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
        });
    }

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(inMemoryFormat)
                .setCacheLocalEntries(true);
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean loaderEnabled) {
        IMapMapStore mapStore = loaderEnabled ? new IMapMapStore() : null;

        Config configWithNearCache = getConfig();
        MapConfig mapConfig = configWithNearCache.getMapConfig(DEFAULT_NEAR_CACHE_NAME)
                .setNearCacheConfig(nearCacheConfig);
        if (loaderEnabled) {
            addMapStoreConfig(mapStore, mapConfig);
        }

        Config config = getConfig();
        if (loaderEnabled) {
            addMapStoreConfig(mapStore, config.getMapConfig(DEFAULT_NEAR_CACHE_NAME));
        }

        HazelcastInstance nearCacheInstance = hazelcastFactory.newHazelcastInstance(configWithNearCache);
        HazelcastInstance dataInstance = hazelcastFactory.newHazelcastInstance(config);

        IMap<K, V> nearCacheMap = nearCacheInstance.getMap(DEFAULT_NEAR_CACHE_NAME);
        IMap<K, V> dataMap = dataInstance.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(nearCacheInstance);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContext<K, V, Data, String>(
                getSerializationService(nearCacheInstance),
                nearCacheInstance,
                dataInstance,
                new IMapDataStructureAdapter<K, V>(nearCacheMap),
                new IMapDataStructureAdapter<K, V>(dataMap),
                nearCacheConfig,
                true,
                nearCache,
                nearCacheManager,
                mapStore);
    }

    public static void addMapStoreConfig(IMapMapStore mapStore, MapConfig mapConfig) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setClassName(null)
                .setImplementation(mapStore);

        mapConfig.setMapStoreConfig(mapStoreConfig);
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the IMap due to invalidations not being applied correctly")
    public void whenLoadAllIsUsed_thenNearCacheIsInvalidated_onDataAdapter() {
        // FIXME: the PutFromLoadAllOperation has the sourceUuid from the local node, so invalidations are not applied
    }
}
