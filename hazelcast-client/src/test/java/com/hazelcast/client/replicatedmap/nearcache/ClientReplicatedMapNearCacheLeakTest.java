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

package com.hazelcast.client.replicatedmap.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.internal.adapter.ReplicatedMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCacheLeakTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.internal.nearcache.NearCacheTestContextBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
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

import static com.hazelcast.client.replicatedmap.nearcache.ClientReplicatedMapInvalidationListener.createInvalidationEventHandler;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ClientReplicatedMapNearCacheLeakTest extends AbstractNearCacheLeakTest<Data, String> {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
        });
    }

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(inMemoryFormat, true)
                .setInvalidateOnChange(true);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(int size) {
        Config config = createConfig();

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        ReplicatedMap<K, V> memberMap = member.getReplicatedMap(DEFAULT_NEAR_CACHE_NAME);
        ReplicatedMapDataStructureAdapter<K, V> dataAdapter = new ReplicatedMapDataStructureAdapter<K, V>(memberMap);

        populateDataAdapter(dataAdapter, size);

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder
                .setDataInstance(member)
                .setDataAdapter(dataAdapter)
                .build();
    }

    protected Config createConfig() {
        Config config = getConfig()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), PARTITION_COUNT);

        config.getReplicatedMapConfig(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(nearCacheConfig.getInMemoryFormat());

        return config;
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        ClientConfig clientConfig = createClientConfig();

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        ReplicatedMap<K, V> clientMap = client.getReplicatedMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new ReplicatedMapDataStructureAdapter<K, V>(clientMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setInvalidationListener(createInvalidationEventHandler(clientMap));
    }
}
