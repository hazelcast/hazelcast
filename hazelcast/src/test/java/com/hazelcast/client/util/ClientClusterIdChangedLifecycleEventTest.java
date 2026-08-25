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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CLUSTER_ID_CHANGED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED;
import static com.hazelcast.test.TimeConstants.MINUTE;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Verifies when a client fires {@link LifecycleState#CLIENT_CLUSTER_ID_CHANGED} and when it does not, and
 * that it is never fired together with {@link LifecycleState#CLIENT_CHANGED_CLUSTER}. A client without
 * failover configuration receives the former when the cluster it reconnects to has a different id; a
 * client with failover configuration is switched through the failover path on any id change and receives
 * the latter, even when the cluster it lands on is a restarted instance of the one it was connected to.
 * The failover side of this rule is covered by {@link ClientClusterIdChangedWithFailoverConfigTest}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientClusterIdChangedLifecycleEventTest extends ClientTestSupport {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test(timeout = MINUTE * 10)
    public void testNoFailoverConfig_clusterRestarted_firesClusterIdChanged_notChangedCluster() {
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance();

        List<LifecycleEvent> events = new CopyOnWriteArrayList<>();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(newLifecycleListenerConfig(events));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        member.shutdown();
        HazelcastInstance restartedMember = hazelcastFactory.newHazelcastInstance();
        UUID newClusterId = restartedMember.getCluster().getClusterId();

        assertTrueEventually(() -> assertEventCount(events, CLIENT_CLUSTER_ID_CHANGED, 1));
        assertEquals(newClusterId, client.getCluster().getClusterId());

        // a late CLIENT_CHANGED_CLUSTER would mean the two states fired together
        assertTrueAllTheTime(() -> assertEventCount(events, CLIENT_CHANGED_CLUSTER, 0), 3);
    }

    @Test(timeout = MINUTE * 10)
    public void testNoFailoverConfig_reconnectToSameCluster_firesNeither() {
        HazelcastInstance member1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance member2 = hazelcastFactory.newHazelcastInstance();
        UUID clusterId = member1.getCluster().getClusterId();

        List<LifecycleEvent> events = new CopyOnWriteArrayList<>();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(newLifecycleListenerConfig(events));
        // single connection, so the client only ever talks to one member at a time
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        findConnectedMember(client, member1, member2).shutdown();

        assertTrueEventually(() -> assertEventCount(events, CLIENT_DISCONNECTED, 1));
        assertTrueEventually(() -> assertEventCount(events, CLIENT_CONNECTED, 2));
        assertEquals(clusterId, client.getCluster().getClusterId());

        assertTrueAllTheTime(() -> {
            assertEventCount(events, CLIENT_CLUSTER_ID_CHANGED, 0);
            assertEventCount(events, CLIENT_CHANGED_CLUSTER, 0);
        }, 3);
    }

    @Test(timeout = MINUTE * 10)
    public void testNoFailoverConfig_initialConnect_firesNeither() {
        hazelcastFactory.newHazelcastInstance();

        List<LifecycleEvent> events = new CopyOnWriteArrayList<>();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(newLifecycleListenerConfig(events));
        hazelcastFactory.newHazelcastClient(clientConfig);

        // lifecycle listeners are notified on the lifecycle executor, so the event can arrive after construction
        assertTrueEventually(() -> assertEventCount(events, CLIENT_CONNECTED, 1));
        assertTrueAllTheTime(() -> {
            assertEventCount(events, CLIENT_CLUSTER_ID_CHANGED, 0);
            assertEventCount(events, CLIENT_CHANGED_CLUSTER, 0);
        }, 3);
    }

    @Test(timeout = MINUTE * 10)
    public void testNoFailoverConfig_clusterRestartedTwice_firesClusterIdChangedTwice() {
        HazelcastInstance currentMember = hazelcastFactory.newHazelcastInstance();

        List<LifecycleEvent> events = new CopyOnWriteArrayList<>();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(newLifecycleListenerConfig(events));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        for (int restart = 1; restart <= 2; restart++) {
            currentMember.shutdown();
            currentMember = hazelcastFactory.newHazelcastInstance();
            UUID clusterId = currentMember.getCluster().getClusterId();
            int expectedClusterIdChangedCount = restart;
            assertTrueEventually(() -> {
                assertEventCount(events, CLIENT_CLUSTER_ID_CHANGED, expectedClusterIdChangedCount);
                assertEquals(clusterId, client.getCluster().getClusterId());
            });
        }

        assertTrueAllTheTime(() -> assertEventCount(events, CLIENT_CHANGED_CLUSTER, 0), 3);
    }

    private ListenerConfig newLifecycleListenerConfig(List<LifecycleEvent> events) {
        LifecycleListener listener = events::add;
        return new ListenerConfig(listener);
    }

    private HazelcastInstance findConnectedMember(HazelcastInstance client, HazelcastInstance... members) {
        Address remoteAddress = getHazelcastClientInstanceImpl(client).getConnectionManager()
                .getActiveConnections().iterator().next().getRemoteAddress();
        for (HazelcastInstance member : members) {
            if (member.getCluster().getLocalMember().getAddress().equals(remoteAddress)) {
                return member;
            }
        }
        throw new IllegalStateException("No member found for connection to " + remoteAddress);
    }

    private void assertEventCount(List<LifecycleEvent> events, LifecycleState state, int expectedCount) {
        long actualCount = events.stream().filter(event -> event.getState() == state).count();
        assertEquals("Expected " + expectedCount + " " + state + " events but found " + actualCount + " in " + events,
                expectedCount, actualCount);
    }
}
