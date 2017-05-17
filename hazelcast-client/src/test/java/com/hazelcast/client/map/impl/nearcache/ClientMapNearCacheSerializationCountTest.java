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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
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
import static java.util.Arrays.asList;

/**
 * Near Cache serialization count tests for {@link IMap} on Hazelcast clients.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapNearCacheSerializationCountTest extends AbstractNearCacheSerializationCountTest<Data, String> {

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

    @Parameter(value = 6)
    public Boolean invalidateOnChange;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameters(name = "mapFormat:{4} nearCacheFormat:{5} invalidateOnChange:{6}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, null, null},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, false},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, true},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, false},

                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 1), newInt(1, 1, 1), OBJECT, null, null},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, true},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, false},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, true},
                {newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false},
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
                    .setInvalidateOnChange(invalidateOnChange);
        }
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config config = getConfig();
        config.getMapConfig(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(mapInMemoryFormat)
                .setBackupCount(0)
                .setAsyncBackupCount(0);
        prepareSerializationConfig(config.getSerializationConfig());

        ClientConfig clientConfig = getClientConfig();
        if (nearCacheConfig != null) {
            clientConfig.addNearCacheConfig(nearCacheConfig);
        }
        prepareSerializationConfig(clientConfig.getSerializationConfig());

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<K, V> memberMap = member.getMap(DEFAULT_NEAR_CACHE_NAME);
        IMap<K, V> clientMap = client.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setDataInstance(member)
                .setNearCacheAdapter(new IMapDataStructureAdapter<K, V>(clientMap))
                .setDataAdapter(new IMapDataStructureAdapter<K, V>(memberMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .build();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }
}
