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
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods;
import com.hazelcast.internal.adapter.DataStructureAdapterMethod;
import com.hazelcast.internal.adapter.ReplicatedMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCacheSerializationCountTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheSerializationCountConfigBuilder;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.internal.nearcache.NearCacheTestContextBuilder;
import com.hazelcast.internal.nearcache.NearCacheTestUtils;
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
import static com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods.GET;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_OPERATION_THREAD_COUNT;
import static java.util.Arrays.asList;

/**
 * Near Cache serialization count tests for {@link ReplicatedMap} on Hazelcast clients.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ClientReplicatedMapNearCacheSerializationCountTest extends AbstractNearCacheSerializationCountTest<Data, String> {

    @Parameter
    public DataStructureMethods method;

    @Parameter(value = 1)
    public int[] keySerializationCounts;

    @Parameter(value = 2)
    public int[] keyDeserializationCounts;

    @Parameter(value = 3)
    public int[] valueSerializationCounts;

    @Parameter(value = 4)
    public int[] valueDeserializationCounts;

    @Parameter(value = 5)
    public InMemoryFormat replicatedMapInMemoryFormat;

    @Parameter(value = 6)
    public InMemoryFormat nearCacheInMemoryFormat;

    @Parameter(value = 7)
    public Boolean serializeKeys;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameters(name = "method:{0} replicatedMapFormat:{5} nearCacheFormat:{6} serializeKeys:{7}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, null, null},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(0, 1, 1), BINARY, BINARY, true},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(0, 1, 1), BINARY, BINARY, false},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, true},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, false},

                // FIXME: we should not serialize the value twice
                {GET, newInt(1, 1, 1), newInt(1, 1, 1), newInt(1, 1, 1), newInt(1, 1, 1), OBJECT, null, null},
                {GET, newInt(1, 1, 0), newInt(1, 1, 0), newInt(1, 2, 0), newInt(1, 1, 1), OBJECT, BINARY, true},
                {GET, newInt(1, 1, 0), newInt(1, 1, 0), newInt(1, 2, 0), newInt(1, 1, 1), OBJECT, BINARY, false},
                {GET, newInt(1, 1, 0), newInt(1, 1, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, true},
                {GET, newInt(1, 1, 0), newInt(1, 1, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false},
        });
    }

    @Before
    public void setUp() {
        testMethod = method;
        expectedKeySerializationCounts = keySerializationCounts;
        expectedKeyDeserializationCounts = keyDeserializationCounts;
        expectedValueSerializationCounts = valueSerializationCounts;
        expectedValueDeserializationCounts = valueDeserializationCounts;
        if (nearCacheInMemoryFormat != null) {
            nearCacheConfig = createNearCacheConfig(nearCacheInMemoryFormat, serializeKeys)
                    // we have to disable invalidations, otherwise there will be an non-deterministic serialization in a listener
                    .setInvalidateOnChange(false);
        }
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected void addConfiguration(NearCacheSerializationCountConfigBuilder configBuilder) {
        configBuilder.append(method);
        configBuilder.append(replicatedMapInMemoryFormat);
        configBuilder.append(nearCacheInMemoryFormat);
        configBuilder.append(serializeKeys);
    }

    @Override
    protected void assumeThatMethodIsAvailable(DataStructureAdapterMethod method) {
        NearCacheTestUtils.assumeThatMethodIsAvailable(ReplicatedMapDataStructureAdapter.class, method);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config config = getConfig()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "1");
        config.getReplicatedMapConfig(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(replicatedMapInMemoryFormat);
        prepareSerializationConfig(config.getSerializationConfig());

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        ReplicatedMap<K, V> memberMap = member.getReplicatedMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder
                .setDataInstance(member)
                .setDataAdapter(new ReplicatedMapDataStructureAdapter<K, V>(memberMap))
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder.build();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        ClientConfig clientConfig = getClientConfig();
        if (nearCacheConfig != null) {
            clientConfig.addNearCacheConfig(nearCacheConfig);
        }
        prepareSerializationConfig(clientConfig.getSerializationConfig());

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        ReplicatedMap<K, V> clientMap = client.getReplicatedMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new ReplicatedMapDataStructureAdapter<K, V>(clientMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager);
    }
}
