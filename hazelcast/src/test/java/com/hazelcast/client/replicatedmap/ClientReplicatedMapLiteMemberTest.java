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

package com.hazelcast.client.replicatedmap;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.cluster.Address;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReplicatedMapLiteMemberTest {

    private TestHazelcastFactory factory;

    private ClientConfig smartClientConfig;
    private ClientConfig dummyClientConfig;

    @Before
    public void init() {
        factory = new TestHazelcastFactory();
        smartClientConfig = new ClientConfig();
        dummyClientConfig = new ClientConfig();
        dummyClientConfig.getNetworkConfig().setSmartRouting(false);
    }

    @After
    public void destroy() {
        factory.terminateAll();
    }

    @Test
    public void testReplicatedMapIsCreatedBySmartClient() {
        testReplicatedMapCreated(2, 1, smartClientConfig);
    }

    @Test
    public void testReplicatedMapIsCreatedByDummyClient() {
        testReplicatedMapCreated(2, 1, dummyClientConfig);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testReplicatedMapNotCreatedOnOnlyLiteMembersBySmartClient() {
        testReplicatedMapCreated(2, 0, smartClientConfig);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testReplicatedMapNotCreatedOnOnlyLiteMembersByDummyClient() {
        testReplicatedMapCreated(2, 0, dummyClientConfig);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testReplicatedMapNotCreatedOnSingleLiteMemberBySmartClient() {
        testReplicatedMapCreated(1, 0, smartClientConfig);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testReplicatedMapNotCreatedOnSingleLiteMemberByDummyClient() {
        testReplicatedMapCreated(1, 0, dummyClientConfig);
    }

    private void testReplicatedMapCreated(int numberOfLiteNodes, int numberOfDataNodes, ClientConfig clientConfig) {
        createNodes(numberOfLiteNodes, numberOfDataNodes);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        assertNotNull(client.getReplicatedMap(randomMapName()));
    }

    @Test
    public void testReplicatedMapPutBySmartClient() {
        createNodes(3, 1);

        HazelcastInstance client = factory.newHazelcastClient();
        ReplicatedMap<Object, Object> map = client.getReplicatedMap(randomMapName());
        assertNull(map.put(1, 2));
    }

    @Test
    public void testReplicatedMapPutByDummyClient() throws UnknownHostException {
        List<HazelcastInstance> instances = createNodes(3, 1);
        configureDummyClientConnection(instances.get(0));

        HazelcastInstance client = factory.newHazelcastClient(dummyClientConfig);

        ReplicatedMap<Object, Object> map = client.getReplicatedMap(randomMapName());
        map.put(1, 2);
    }

    private void configureDummyClientConnection(HazelcastInstance instance) throws UnknownHostException {
        Address memberAddress = getAddress(instance);
        dummyClientConfig.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        ClientNetworkConfig networkConfig = dummyClientConfig.getNetworkConfig();
        networkConfig.addAddress(memberAddress.getHost() + ":" + memberAddress.getPort());
    }

    private List<HazelcastInstance> createNodes(int numberOfLiteNodes, int numberOfDataNodes) {
        List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();

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
