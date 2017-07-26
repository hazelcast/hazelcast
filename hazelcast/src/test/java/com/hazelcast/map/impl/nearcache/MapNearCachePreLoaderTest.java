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
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCachePreloaderTest;
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

import java.io.File;
import java.util.Collection;

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getMapNearCacheManager;
import static com.hazelcast.map.impl.nearcache.MapInvalidationListener.createInvalidationEventHandler;
import static java.util.Arrays.asList;

/**
 * Basic Near Cache tests for {@link IMap} on Hazelcast members.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({ParallelTest.class, QuickTest.class})
public class MapNearCachePreLoaderTest extends AbstractNearCachePreloaderTest<Data, String> {

    private final File storeFile = new File("nearCache-" + defaultNearCache + ".store").getAbsoluteFile();
    private final File storeLockFile = new File(storeFile.getName() + ".lock").getAbsoluteFile();

    @Parameters(name = "format:{0} invalidationOnChange:{1} serializeKeys:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, false, true},
                {InMemoryFormat.BINARY, false, false},
                {InMemoryFormat.BINARY, true, true},
                {InMemoryFormat.BINARY, true, false},

                {InMemoryFormat.OBJECT, false, true},
                {InMemoryFormat.OBJECT, false, false},
                {InMemoryFormat.OBJECT, true, true},
                {InMemoryFormat.OBJECT, true, false},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public boolean invalidationOnChange;

    @Parameter(value = 2)
    public boolean serializeKeys;

    private final TestHazelcastInstanceFactory hazelcastFactory = createHazelcastInstanceFactory();

    @Before
    public void setUp() {
        nearCacheConfig = getNearCacheConfig(inMemoryFormat, serializeKeys, invalidationOnChange, KEY_COUNT,
                storeFile.getParent())
                .setCacheLocalEntries(true);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Override
    protected File getStoreFile() {
        return storeFile;
    }

    @Override
    protected File getStoreLockFile() {
        return storeLockFile;
    }

    @Override
    protected <K, V> DataStructureAdapter<K, V> getDataStructure(NearCacheTestContext<K, V, Data, String> context, String name) {
        IMap<K, V> map = context.nearCacheInstance.getMap(name);
        return new IMapDataStructureAdapter<K, V>(map);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean createClient) {
        Config config = createConfig(false);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        IMap<K, V> memberMap = member.getMap(nearCacheConfig.getName());

        if (createClient) {
            NearCacheTestContextBuilder<K, V, Data, String> contextBuilder = createNearCacheContextBuilder();
            return contextBuilder
                    .setDataInstance(member)
                    .setDataAdapter(new IMapDataStructureAdapter<K, V>(memberMap))
                    .build();

        }
        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(member))
                .setDataInstance(member)
                .setDataAdapter(new IMapDataStructureAdapter<K, V>(memberMap))
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createClientContext() {
        NearCacheTestContextBuilder<K, V, Data, String> contextBuilder = createNearCacheContextBuilder();
        return contextBuilder.build();
    }

    protected Config createConfig(boolean withNearCache) {
        Config config = getConfig();

        MapConfig mapConfig = config.getMapConfig(nearCacheConfig.getName());
        if (withNearCache) {
            mapConfig.setNearCacheConfig(nearCacheConfig);
        }

        return config;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        Config config = createConfig(true);

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);
        IMap<K, V> map = instance.getMap(nearCacheConfig.getName());

        NearCacheManager nearCacheManager = getMapNearCacheManager(instance);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(nearCacheConfig.getName());

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(instance))
                .setNearCacheInstance(instance)
                .setNearCacheAdapter(new IMapDataStructureAdapter<K, V>(map))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setInvalidationListener(createInvalidationEventHandler(map));
    }
}
