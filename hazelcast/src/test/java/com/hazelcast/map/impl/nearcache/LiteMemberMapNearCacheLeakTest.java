/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nearcache.AbstractNearCacheLeakTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.internal.nearcache.NearCacheTestContextBuilder;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.properties.GroupProperty;
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

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getMapNearCacheManager;
import static com.hazelcast.map.impl.nearcache.MapInvalidationListener.createInvalidationEventHandler;
import static java.util.Arrays.asList;

/**
 * Basic Near Cache tests for {@link IMap} on Hazelcast Lite members.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class LiteMemberMapNearCacheLeakTest extends AbstractNearCacheLeakTest<Data, String> {

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
                .setInvalidateOnChange(true);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(int size) {
        Config config = createConfig(false);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        IMap<K, V> memberMap = member.getMap(DEFAULT_NEAR_CACHE_NAME);
        IMapDataStructureAdapter<K, V> dataAdapter = new IMapDataStructureAdapter<K, V>(memberMap);

        // wait until the initial load is done
        dataAdapter.waitUntilLoaded();
        populateDataAdapter(dataAdapter, size);

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder
                .setDataInstance(member)
                .setDataAdapter(dataAdapter)
                .build();
    }

    protected Config createConfig(boolean liteMember) {
        Config config = getConfig()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), PARTITION_COUNT)
                .setLiteMember(liteMember);

        MapConfig mapConfig = config.getMapConfig(DEFAULT_NEAR_CACHE_NAME);
        if (liteMember) {
            mapConfig.setNearCacheConfig(nearCacheConfig);
        }

        return config;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        Config configWithNearCache = createConfig(true);

        HazelcastInstance liteMember = hazelcastFactory.newHazelcastInstance(configWithNearCache);
        IMap<K, V> liteMemberMap = liteMember.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(liteMember);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        RepairingTask repairingTask = ((MapNearCacheManager) nearCacheManager).getRepairingTask();

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(liteMember))
                .setNearCacheInstance(liteMember)
                .setNearCacheAdapter(new IMapDataStructureAdapter<K, V>(liteMemberMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setInvalidationListener(createInvalidationEventHandler(liteMemberMap))
                .setRepairingTask(repairingTask);
    }
}
