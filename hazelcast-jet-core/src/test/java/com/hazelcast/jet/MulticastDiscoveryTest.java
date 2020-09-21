/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SerialTest.class, SlowTest.class})
public class MulticastDiscoveryTest extends JetTestSupport {

    private static final String UNABLE_TO_CONNECT_MESSAGE = "Unable to connect";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before() {
        TestProcessors.reset(1);
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
        Jet.shutdownAll();
    }

    @Test
    public void when_twoJetInstancesCreated_then_clusterOfTwoShouldBeFormed() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().setClusterName(randomName());

        JetInstance instance = Jet.newJetInstance(config);
        Jet.newJetInstance(config);

        assertEquals(2, instance.getCluster().getMembers().size());
    }

    @Test
    public void when_twoJetAndTwoHzInstancesCreated_then_twoClustersOfTwoShouldBeFormed() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().setClusterName(randomName());

        JetInstance jetInstance = Jet.newJetInstance(config);
        Jet.newJetInstance(config);

        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        assertEquals(2, jetInstance.getCluster().getMembers().size());
        assertEquals(2, hazelcastInstance.getCluster().getMembers().size());
    }

    @Test
    public void when_jetClientCreated_then_connectsToJetCluster() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().setClusterName(randomName());

        JetInstance jetInstance1 = Jet.newJetInstance(config);
        JetInstance jetInstance2 = Jet.newJetInstance(config);

        // Configure client with the address of the created instance
        // Sometimes the instances are created with a different port number
        // like 5704, 5705
        JetInstance client = Jet.newJetClient(clientConfig(jetInstance1, jetInstance2));

        assertEquals(2, client.getHazelcastInstance().getCluster().getMembers().size());
    }

    @Test
    public void when_jetClientCreated_then_doesNotConnectToHazelcastCluster() {
        Config config = new Config();
        config.setClusterName(randomName());

        Hazelcast.newHazelcastInstance(config);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(UNABLE_TO_CONNECT_MESSAGE);
        Jet.newJetClient();
    }

    @Test
    public void when_hazelcastClientCreated_then_doesNotConnectToJetCluster() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().setClusterName(randomName());

        Jet.newJetInstance(config);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(UNABLE_TO_CONNECT_MESSAGE);
        HazelcastClient.newHazelcastClient();
    }

    /**
     * Creates a client config which configured with the address of the jet
     * instances provided.
     */
    private static JetClientConfig clientConfig(JetInstance... jetInstances) {
        JetClientConfig jetClientConfig = new JetClientConfig();
        ClientNetworkConfig networkConfig = jetClientConfig.getNetworkConfig();
        for (JetInstance jet : jetInstances) {
            jetClientConfig.setClusterName(jet.getConfig().getHazelcastConfig().getClusterName());
            Address address = getAddress(jet);
            networkConfig.addAddress(address.getHost() + ":" + address.getPort());
        }
        return jetClientConfig;
    }
}
