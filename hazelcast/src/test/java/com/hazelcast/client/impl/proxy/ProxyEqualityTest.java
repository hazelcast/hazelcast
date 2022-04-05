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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProxyEqualityTest {

    private final TestHazelcastFactory hazelcastFactoryClusterA = new TestHazelcastFactory();
    private final TestHazelcastFactory hazelcastFactoryClusterB = new TestHazelcastFactory();

    private final String name = "foo";

    private HazelcastInstance client1ClusterA;
    private HazelcastInstance client2ClusterA;

    private HazelcastInstance client1ClusterB;

    @After
    public void tearDown() {
        hazelcastFactoryClusterA.terminateAll();
        hazelcastFactoryClusterB.terminateAll();
    }


    @Before
    public void setup() throws Exception {
        Config config = new Config();
        String clusterAName = "ClusterA";
        config.setClusterName(clusterAName);
        hazelcastFactoryClusterA.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterAName);
        client1ClusterA = hazelcastFactoryClusterA.newHazelcastClient(clientConfig);
        client2ClusterA = hazelcastFactoryClusterA.newHazelcastClient(clientConfig);

        //setup Group B
        config = new Config();
        String clusterBName = "ClusterB";
        config.setClusterName(clusterBName);
        hazelcastFactoryClusterB.newHazelcastInstance(config);

        clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterBName);
        client1ClusterB = hazelcastFactoryClusterB.newHazelcastClient(clientConfig);
    }

    @Test
    public void testTwoClientProxiesFromTheSameInstanceAreEquals() {

        ClientProxy ref1 = (ClientProxy) client1ClusterA.getSet(name);
        ClientProxy ref2 = (ClientProxy) client1ClusterA.getSet(name);

        assertEquals(ref1, ref2);
    }

    @Test
    public void testProxiesAreCached() {

        ClientProxy ref1 = (ClientProxy) client1ClusterA.getSet(name);
        ClientProxy ref2 = (ClientProxy) client1ClusterA.getSet(name);

        assertSame(ref1, ref2);
    }

    @Test
    public void testTwoClientProxiesFromDifferentInstancesAreNotEquals() {

        ClientProxy ref1 = (ClientProxy) client1ClusterA.getSet(name);
        ClientProxy ref2 = (ClientProxy) client1ClusterB.getSet(name);

        assertNotEquals(ref1, ref2);
    }

    @Test
    public void testTwoClientProxiesFromTwoDifferentClientsConnectedToTheSameInstanceAreNotEquals() {

        ClientProxy ref1 = (ClientProxy) client1ClusterA.getSet(name);
        ClientProxy ref2 = (ClientProxy) client2ClusterA.getSet(name);

        assertNotEquals(ref1, ref2);
    }
}
