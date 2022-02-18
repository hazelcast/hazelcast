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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientClusterProxyTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory;

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    private HazelcastInstance client() {
        factory = new TestHazelcastFactory();
        Config config = new Config();
        String clusterAName = "HZ:CLUSTER";
        config.setClusterName(clusterAName);
        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(config.getClusterName());
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
}
