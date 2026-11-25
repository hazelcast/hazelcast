/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.util;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CLUSTER_ID_CHANGED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static com.hazelcast.test.TimeConstants.MINUTE;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Covers the failover side of the rule described in {@link ClientClusterIdChangedLifecycleEventTest}: a client
 * configured with a {@link ClientFailoverConfig} is switched through the failover path on any cluster id change,
 * and receives {@link LifecycleState#CLIENT_CHANGED_CLUSTER}, never {@link LifecycleState#CLIENT_CLUSTER_ID_CHANGED},
 * even when the cluster it lands on is a restarted instance of the one it was connected to. The members here are
 * made to report failover support so this can run against a Community build, which otherwise rejects failover
 * clients.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientClusterIdChangedWithFailoverConfigTest extends ClientTestSupport {
    private static final String CLUSTER_NAME = "dev";
    private static final int PORT = 5701;

    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = MINUTE * 10)
    public void testFailoverConfig_clusterRestarted_firesChangedCluster_notClusterIdChanged() {
        HazelcastInstance memberA = HazelcastInstanceFactory.newHazelcastInstance(newMemberConfig(), "memberA",
                failoverSupportingNodeContext());
        Member localMemberA = (Member) memberA.getLocalEndpoint();
        Address addressA = localMemberA.getAddress();
        String addressString = addressA.getHost() + ":" + addressA.getPort();

        List<LifecycleEvent> events = new CopyOnWriteArrayList<>();
        // the failover config validates that both alternatives carry the same listener configs, so the same
        // ListenerConfig instance is added to both
        ListenerConfig listenerConfig = new ListenerConfig((LifecycleListener) events::add);

        ClientConfig clientConfig1 = newClientConfig(addressString);
        clientConfig1.addListenerConfig(listenerConfig);
        ClientConfig clientConfig2 = newClientConfig(addressString);
        clientConfig2.addListenerConfig(listenerConfig);

        ClientFailoverConfig failoverConfig = new ClientFailoverConfig();
        failoverConfig.addClientConfig(clientConfig1).addClientConfig(clientConfig2).setTryCount(3);

        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(failoverConfig);

        assertTrueEventually(() -> assertEventCount(events, CLIENT_CONNECTED, 1));

        memberA.shutdown();
        HazelcastInstance memberB = HazelcastInstanceFactory.newHazelcastInstance(newMemberConfig(), "memberB",
                failoverSupportingNodeContext());
        UUID secondClusterId = memberB.getCluster().getClusterId();

        assertTrueEventually(() -> {
            assertEventCount(events, CLIENT_CHANGED_CLUSTER, 1);
            assertEquals(secondClusterId, client.getCluster().getClusterId());
        });

        assertTrueAllTheTime(() -> assertEventCount(events, CLIENT_CLUSTER_ID_CHANGED, 0), 3);
    }

    private Config newMemberConfig() {
        Config config = new Config();
        config.setClusterName(CLUSTER_NAME);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        // fixed, non-incrementing port so the restarted member listens where the shut down one did
        config.getNetworkConfig().setPort(PORT).setPortAutoIncrement(false);
        return config;
    }

    private ClientConfig newClientConfig(String addressString) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(CLUSTER_NAME);
        clientConfig.getNetworkConfig().setAddresses(Collections.singletonList(addressString));
        return clientConfig;
    }

    private static TestHazelcastInstanceFactory.DelegatingNodeContext failoverSupportingNodeContext() {
        return new TestHazelcastInstanceFactory.DelegatingNodeContext(new DefaultNodeContext()) {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                return new DefaultNodeExtension(node) {
                    @Override
                    public boolean isClientFailoverSupported() {
                        return true;
                    }
                };
            }
        };
    }

    private void assertEventCount(List<LifecycleEvent> events, LifecycleState state, int expectedCount) {
        long actualCount = events.stream().filter(event -> event.getState() == state).count();
        assertEquals("Expected " + expectedCount + " " + state + " events but found " + actualCount + " in " + events,
                expectedCount, actualCount);
    }
}
