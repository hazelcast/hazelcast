/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.MembershipAdapter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientClusterProxyTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String clusterName = "HZ:CLUSTER";
    private HazelcastInstance instance;

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    private HazelcastInstance createInstance() {
        Config config = new Config();
        config.setClusterName(clusterName);
        return factory.newHazelcastInstance(config);
    }
    private HazelcastInstance client() {
        instance = createInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        return factory.newHazelcastClient(clientConfig);
    }

    private HazelcastInstance notConnectedClient() {
        factory = new TestHazelcastFactory();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        return factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void addMembershipListener() throws Exception {
        UUID regId = client().getCluster().addMembershipListener(new MembershipAdapter());
        assertNotNull(regId);
    }

    @Test
    public void removeMembershipListener() throws Exception {
        Cluster cluster = client().getCluster();
        UUID regId = cluster.addMembershipListener(new MembershipAdapter());
        assertTrue(cluster.removeMembershipListener(regId));
    }

    @Test
    public void getMembers() throws Exception {
        assertEquals(1, client().getCluster().getMembers().size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getLocalMember() throws Exception {
        client().getCluster().getLocalMember();
    }

    @Test
    public void getClusterTime() throws Exception {
        assertTrue(client().getCluster().getClusterTime() > 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getClusterState() throws Exception {
        client().getCluster().getClusterState();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void changeClusterState() throws Exception {
        client().getCluster().changeClusterState(ClusterState.FROZEN);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getClusterVersion() throws Exception {
        client().getCluster().getClusterVersion();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void changeClusterStateWithOptions() throws Exception {
        client().getCluster().changeClusterState(ClusterState.FROZEN, new TransactionOptions());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shutdown() throws Exception {
        client().getCluster().shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shutdownWithOptions() throws Exception {
        client().getCluster().shutdown(new TransactionOptions());
    }

    @Test
    public void isEnterprise() {
        assertFalse(client().getCluster().isEnterprise());
    }

    @Test
    public void isEnterpriseReconnectionToCluster() {
        HazelcastInstance client = client();
        assertFalse(client.getCluster().isEnterprise());
        instance.shutdown();
        // isEnterprise should throw IllegalStateException when disconnected
        assertThatThrownBy(client.getCluster()::isEnterprise)
                .isInstanceOf(IllegalStateException.class)
                .hasStackTraceContaining("The client is not connected");
        // Reconnect and isEnterprise should be true eventually
        createInstance();
        assertTrueEventually(() -> {
            try {
                assertFalse(client.getCluster().isEnterprise());
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("The client is not connected")) {
                    throw new AssertionError("Not connected yet");
                } else {
                    throw e;
                }
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void isEnterpriseThrowsIfNotConnected() {
        notConnectedClient().getCluster().isEnterprise();
    }
}
