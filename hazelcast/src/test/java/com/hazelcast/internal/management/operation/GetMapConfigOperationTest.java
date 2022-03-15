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

package com.hazelcast.internal.management.operation;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GetMapConfigOperationTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private TestHazelcastFactory factory;
    private HazelcastClientInstanceImpl client;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();

        Config config = smallInstanceConfig();
        MapConfig withIndex = new MapConfig("map-with-index")
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "first"));
        config.addMapConfig(withIndex);

        CompactSerializationConfig compactSerializationConfig =
                config.getSerializationConfig().getCompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);

        hz = factory.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        client = ((HazelcastClientProxy) factory.newHazelcastClient(clientConfig)).client;
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void mapWithPreconfiguredIndex_empty() throws Exception {
        MCGetMapConfigCodec.ResponseParameters actual = runCommand(client, hz, "map-with-index").get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        // index is read from config
        assertThat(actual.globalIndexes)
                .usingElementComparatorIgnoringFields("name")
                .containsExactly(new IndexConfig(IndexType.SORTED, "first"));
    }

    @Test
    public void testMapWithPreconfiguredIndex_addedIndex() throws Exception {
        client.getMap("map-with-index").addIndex(new IndexConfig(IndexType.HASH, "second"));
        MCGetMapConfigCodec.ResponseParameters actual = runCommand(client, hz, "map-with-index").get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertThat(actual.globalIndexes)
                .usingElementComparatorIgnoringFields("name")
                .containsExactlyInAnyOrder(
                        new IndexConfig(IndexType.SORTED, "first"),
                        new IndexConfig(IndexType.HASH, "second")
                );
    }

    @Test
    public void testMapWithoutPreconfiguredIndexes_addedIndex() throws Exception {
        client.getMap("map-without-indexes").addIndex(new IndexConfig(IndexType.HASH, "second"));
        MCGetMapConfigCodec.ResponseParameters actual = runCommand(client, hz, "map-without-indexes").get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertThat(actual.globalIndexes)
                .usingElementComparatorIgnoringFields("name")
                .containsExactlyInAnyOrder(
                        new IndexConfig(IndexType.HASH, "second")
                );
    }

    private ClientDelegatingFuture<MCGetMapConfigCodec.ResponseParameters> runCommand(
            HazelcastClientInstanceImpl client, HazelcastInstance member, String mapName) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetMapConfigCodec.encodeRequest(mapName),
                null,
                member.getCluster().getLocalMember().getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                client.getSerializationService(),
                MCGetMapConfigCodec::decodeResponse
        );
    }
}
