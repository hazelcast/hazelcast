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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.DataStructureAdapterMethod;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.adapter.IMapMapStore;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.AbstractNearCacheBasicTest;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContext;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContextBuilder;
import com.hazelcast.internal.nearcache.impl.NearCacheTestUtils;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getMapNearCacheManager;
import static com.hazelcast.map.impl.nearcache.MapNearCacheBasicTest.addMapStoreConfig;
import static com.hazelcast.test.Accessors.getSerializationService;

/**
 * Basic Near Cache tests for {@link IMap} on Hazelcast Lite members
 * with a Hazelcast client as {@link NearCacheTestContext#dataInstance}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LiteMemberClientMapNearCacheBasicTest extends AbstractNearCacheBasicTest<Data, String> {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(DEFAULT_MEMORY_FORMAT, DEFAULT_SERIALIZE_KEYS);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Override
    protected void assumeThatMethodIsAvailable(DataStructureAdapterMethod method) {
        NearCacheTestUtils.assumeThatMethodIsAvailable(IMapDataStructureAdapter.class, method);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean loaderEnabled) {
        IMapMapStore mapStore = loaderEnabled ? new IMapMapStore() : null;
        Config config = getConfig(mapStore, false);
        ClientConfig clientConfig = getClientConfig();

        // create a Hazelcast member to hold the data
        hazelcastFactory.newHazelcastInstance(config);

        // create a Hazelcast client to be used in the tests
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        IMap<K, V> clientMap = client.getMap(DEFAULT_NEAR_CACHE_NAME);
        IMapDataStructureAdapter<K, V> dataAdapter = new IMapDataStructureAdapter<K, V>(clientMap);

        // wait until the initial load is done
        dataAdapter.waitUntilLoaded();

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder(mapStore);
        return builder
                .setDataInstance(client)
                .setDataAdapter(dataAdapter)
                .setLoader(mapStore)
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder(null);
        return builder.build();
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected Config getConfig(IMapMapStore mapStore, boolean liteMember) {
        Config config = getConfig()
                .setLiteMember(liteMember);

        MapConfig mapConfig = config.getMapConfig(DEFAULT_NEAR_CACHE_NAME);
        addMapStoreConfig(mapStore, mapConfig);
        if (liteMember) {
            mapConfig.setNearCacheConfig(nearCacheConfig);
        }

        return config;
    }

    protected ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(NearCache.PROP_EXPIRATION_TASK_INITIAL_DELAY_SECONDS, "0");
        clientConfig.setProperty(NearCache.PROP_EXPIRATION_TASK_PERIOD_SECONDS, "1");
        return clientConfig;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder(IMapMapStore mapStore) {
        Config configWithNearCache = getConfig(mapStore, true);

        HazelcastInstance liteMember = hazelcastFactory.newHazelcastInstance(configWithNearCache);
        IMap<K, V> liteMemberMap = liteMember.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(liteMember);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(liteMember))
                .setNearCacheInstance(liteMember)
                .setNearCacheAdapter(new IMapDataStructureAdapter<K, V>(liteMemberMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setLoader(mapStore);
    }
}
