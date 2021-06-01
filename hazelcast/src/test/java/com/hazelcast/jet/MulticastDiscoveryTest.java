/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
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
    private static final int CLUSTER_CONNECTION_TIMEOUT = 20_000;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before() {
        TestProcessors.reset(1);
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void when_twoJetInstancesCreated_then_clusterOfTwoShouldBeFormed() {
        Config config = new Config().setClusterName(randomName());

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        assertEquals(2, instance.getCluster().getMembers().size());
    }

    @Test
    public void when_jetClientCreated_then_connectsToJetCluster() {
        Config config = new Config().setClusterName(randomName());

        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);

        // Configure client with the address of the created instance
        // Sometimes the instances are created with a different port number
        // like 5704, 5705
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig(instance1, instance2));

        assertEquals(2, client.getCluster().getMembers().size());
    }

    @Test
    public void when_hazelcastClientCreated_then_doesNotConnectToJetCluster() {
        Config config = new Config();
        config.setClusterName(randomName());

        Hazelcast.newHazelcastInstance(config);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(UNABLE_TO_CONNECT_MESSAGE);

        ClientConfig clientConfig = ClientConfig.load();
        configureTimeout(clientConfig);
        HazelcastClient.newHazelcastClient(clientConfig);
    }

    /**
     * Creates a client config which configured with the address of the jet
     * instances provided.
     */
    private static ClientConfig clientConfig(HazelcastInstance... instances) {
        ClientConfig jetClientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = jetClientConfig.getNetworkConfig();
        for (HazelcastInstance instance : instances) {
            jetClientConfig.setClusterName(instance.getConfig().getClusterName());
            Address address = getAddress((JetInstance) instance.getJet());
            networkConfig.addAddress(address.getHost() + ":" + address.getPort());
        }
        return jetClientConfig;
    }

    private void configureTimeout(ClientConfig clientConfig) {
        // override default indefinite cluster connection timeout
        clientConfig.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(CLUSTER_CONNECTION_TIMEOUT);
    }
}
