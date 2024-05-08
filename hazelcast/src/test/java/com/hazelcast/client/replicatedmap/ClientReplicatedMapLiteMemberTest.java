/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.replicatedmap;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.ConfigRoutingUtil;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReplicatedMapLiteMemberTest {

    @Rule
    // needed for SUBSET routing mode
    public OverridePropertyRule setProp = OverridePropertyRule.set(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE, "true");

    @Parameterized.Parameter
    public RoutingMode routingMode;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(RoutingMode.UNISOCKET, RoutingMode.SMART, RoutingMode.SUBSET);
    }

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    private ClientConfig newClientConfig() {
        return ConfigRoutingUtil.newClientConfig(routingMode);
    }

    @After
    public void destroy() {
        factory.terminateAll();
    }

    @Test
    public void testReplicatedMapIsCreated() {
        testReplicatedMapCreated(2, 1, newClientConfig());
    }

    @Test
    public void testReplicatedMapNotCreatedOnOnlyLiteMembers() {
        Assert.assertThrows(ReplicatedMapCantBeCreatedOnLiteMemberException.class, () -> {
            testReplicatedMapCreated(2, 0, newClientConfig());
        });
    }

    @Test
    public void testReplicatedMapNotCreatedOnSingleLiteMember() {
        Assert.assertThrows(ReplicatedMapCantBeCreatedOnLiteMemberException.class, () -> {
            testReplicatedMapCreated(1, 0, newClientConfig());
        });
    }

    private void testReplicatedMapCreated(int numberOfLiteNodes,
                                          int numberOfDataNodes,
                                          ClientConfig clientConfig) {

        createNodes(numberOfLiteNodes, numberOfDataNodes);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        assertNotNull(client.getReplicatedMap(randomMapName()));
    }

    @Test
    public void testReplicatedMapPut() {
        List<HazelcastInstance> instances = createNodes(3, 1);

        ClientConfig clientConfig = newClientConfig();
        if (routingMode == RoutingMode.UNISOCKET) {
            configureDummyClientConnection(instances.get(0), clientConfig);
        }
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        ReplicatedMap<Object, Object> map = client.getReplicatedMap(randomMapName());
        assertNull(map.put(1, 2));
    }

    private void configureDummyClientConnection(HazelcastInstance instance, ClientConfig clientConfig) {
        Address memberAddress = getAddress(instance);
        clientConfig.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress(memberAddress.getHost() + ":" + memberAddress.getPort());
    }

    private List<HazelcastInstance> createNodes(int numberOfLiteNodes, int numberOfDataNodes) {
        List<HazelcastInstance> instances = new ArrayList<>();

        Config liteConfig = new Config().setLiteMember(true);
        for (int i = 0; i < numberOfLiteNodes; i++) {
            instances.add(factory.newHazelcastInstance(liteConfig));
        }

        for (int i = 0; i < numberOfDataNodes; i++) {
            instances.add(factory.newHazelcastInstance());
        }

        int clusterSize = numberOfLiteNodes + numberOfDataNodes;
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(clusterSize, instance);
        }

        return instances;
    }
}
