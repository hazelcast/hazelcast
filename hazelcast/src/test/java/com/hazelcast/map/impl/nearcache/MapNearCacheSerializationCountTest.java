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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCacheSerializationCountTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.internal.nearcache.NearCacheTestContextBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getMapNearCacheManager;
import static java.util.Arrays.asList;

/**
 * Near Cache serialization count tests for {@link IMap} on Hazelcast members.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({ParallelTest.class, QuickTest.class})
public class MapNearCacheSerializationCountTest extends AbstractNearCacheSerializationCountTest<Data, String> {

    @Parameter
    public int[] keySerializationCounts;

    @Parameter(value = 1)
    public int[] keyDeserializationCounts;

    @Parameter(value = 2)
    public int[] valueSerializationCounts;

    @Parameter(value = 3)
    public int[] valueDeserializationCounts;

    @Parameter(value = 4)
    public InMemoryFormat mapInMemoryFormat;

    @Parameter(value = 5)
    public InMemoryFormat nearCacheInMemoryFormat;

    private final TestHazelcastInstanceFactory hazelcastFactory = createHazelcastInstanceFactory();

    @Parameters(name = "mapFormat:{4} nearCacheFormat:{5}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {INT_ARRAY_1, INT_ARRAY_0, new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, null},
                {INT_ARRAY_1, INT_ARRAY_0, new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, BINARY},
                {INT_ARRAY_1, INT_ARRAY_0, new int[]{1, 0, 0}, new int[]{0, 1, 0}, BINARY, OBJECT},

                {INT_ARRAY_1, INT_ARRAY_0, new int[]{1, 1, 1}, new int[]{1, 1, 1}, OBJECT, null},
                {INT_ARRAY_1, INT_ARRAY_0, new int[]{1, 1, 0}, new int[]{1, 1, 1}, OBJECT, BINARY},
                {INT_ARRAY_1, INT_ARRAY_0, new int[]{1, 1, 0}, new int[]{1, 1, 0}, OBJECT, OBJECT},
        });
    }

    @Before
    public void setUp() {
        expectedKeySerializationCounts = keySerializationCounts;
        expectedKeyDeserializationCounts = keyDeserializationCounts;
        expectedValueSerializationCounts = valueSerializationCounts;
        expectedValueDeserializationCounts = valueDeserializationCounts;
        if (nearCacheInMemoryFormat != null) {
            nearCacheConfig = createNearCacheConfig(nearCacheInMemoryFormat)
                    .setCacheLocalEntries(true);
        }
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config configWithNearCache = getConfig(true);
        Config config = getConfig(false);

        HazelcastInstance nearCacheMember = hazelcastFactory.newHazelcastInstance(configWithNearCache);
        HazelcastInstance dataMember = hazelcastFactory.newHazelcastInstance(config);

        IMap<K, V> nearCacheMap = nearCacheMember.getMap(DEFAULT_NEAR_CACHE_NAME);
        IMap<K, V> dataMap = dataMember.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(dataMember);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(dataMember))
                .setNearCacheInstance(nearCacheMember)
                .setDataInstance(dataMember)
                .setNearCacheAdapter(new IMapDataStructureAdapter<K, V>(nearCacheMap))
                .setDataAdapter(new IMapDataStructureAdapter<K, V>(dataMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setHasLocalData(true)
                .build();
    }

    private Config getConfig(boolean withNearCache) {
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(mapInMemoryFormat)
                .setBackupCount(0)
                .setAsyncBackupCount(0);
        if (withNearCache && nearCacheConfig != null) {
            mapConfig.setNearCacheConfig(nearCacheConfig);
        }
        prepareSerializationConfig(config.getSerializationConfig());
        return config;
    }
}
