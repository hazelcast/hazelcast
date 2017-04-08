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

package com.hazelcast.client.replicatedmap.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.internal.adapter.ReplicatedMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCacheSerializationCountTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.nio.serialization.Data;
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

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static java.util.Arrays.asList;

/**
 * Near Cache serialization count tests for {@link ReplicatedMap} on Hazelcast clients.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ClientReplicatedMapNearCacheSerializationCountTest extends AbstractNearCacheSerializationCountTest<Data, String> {

    @Parameter
    public int[] expectedSerializationCounts;

    @Parameter(value = 1)
    public int[] expectedDeserializationCounts;

    @Parameter(value = 2)
    public InMemoryFormat replicatedMapInMemoryFormat;

    @Parameter(value = 3)
    public InMemoryFormat nearCacheInMemoryFormat;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameters(name = "replicatedMapFormat:{2} nearCacheFormat:{3}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, null,},
                {new int[]{1, 1, 0}, new int[]{0, 1, 1}, BINARY, BINARY,},
                {new int[]{1, 0, 0}, new int[]{0, 1, 0}, BINARY, OBJECT,},

                {new int[]{1, 1, 1}, new int[]{1, 1, 1}, OBJECT, null,},
                {new int[]{1, 2, 0}, new int[]{1, 1, 1}, OBJECT, BINARY,},
                {new int[]{1, 1, 0}, new int[]{1, 1, 0}, OBJECT, OBJECT,},
        });
    }

    @Before
    public void setUp() {
        if (nearCacheInMemoryFormat != null) {
            nearCacheConfig = createNearCacheConfig(nearCacheInMemoryFormat);
        }
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected int[] getExpectedSerializationCounts() {
        return expectedSerializationCounts;
    }

    @Override
    protected int[] getExpectedDeserializationCounts() {
        return expectedDeserializationCounts;
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config config = getConfig();
        config.getReplicatedMapConfig(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(replicatedMapInMemoryFormat);
        prepareSerializationConfig(config.getSerializationConfig());

        ClientConfig clientConfig = getClientConfig();
        if (nearCacheConfig != null) {
            clientConfig.addNearCacheConfig(nearCacheConfig);
        }
        prepareSerializationConfig(clientConfig.getSerializationConfig());

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);

        ReplicatedMap<K, V> memberMap = member.getReplicatedMap(DEFAULT_NEAR_CACHE_NAME);
        ReplicatedMap<K, V> clientMap = client.getReplicatedMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();

        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContext<K, V, Data, String>(
                client.getSerializationService(),
                client,
                member,
                new ReplicatedMapDataStructureAdapter<K, V>(clientMap),
                new ReplicatedMapDataStructureAdapter<K, V>(memberMap),
                nearCacheConfig,
                false,
                nearCache,
                nearCacheManager);
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }
}
