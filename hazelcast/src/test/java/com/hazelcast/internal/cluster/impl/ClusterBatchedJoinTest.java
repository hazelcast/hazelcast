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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ClusterBatchedJoinTest extends HazelcastTestSupport {

    private final HazelcastInstance[] batchedMembers = new HazelcastInstance[10];
    private final List<String> membersList = new ArrayList<>(10);

    @Before
    public void prepare() throws InterruptedException {
        // Prepare members list and config
        List<String> membersList = new ArrayList<>(10);
        for (int k = 0; k < 10; k++) {
            membersList.add("127.0.0.1:" + (5701 + k));
        }
        Config config = smallInstanceConfigWithoutJetAndMetrics().setClusterName("myCluster");
        config.getNetworkConfig().setPort(5701);
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getTcpIpConfig().setEnabled(true).setMembers(membersList);

        // Create a cluster that goes through a batched join, starting with a master instance (always index 0)
        batchedMembers[0] = Hazelcast.newHazelcastInstance(config);
        // Start 9 members simultaneously (so their join request is batched)
        ExecutorService pool = Executors.newFixedThreadPool(9);
        for (int k = 1; k < 10; k++) {
            int finalK = k;
            pool.execute(() -> batchedMembers[finalK] = Hazelcast.newHazelcastInstance(config));
        }

        // Wait for all member instances to be created
        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        // Ensure cluster size is 10
        assertClusterSizeEventually(10, batchedMembers);
    }

    @After
    public void cleanUp() {
        // Terminate cluster members
        for (HazelcastInstance member : batchedMembers) {
            member.getLifecycleService().terminate();
        }
    }

    @Test
    public void testDistributedObjectsAccurateAfterBatchedJoinHandling() {
        // Create a client and connect to the cluster
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("myCluster").getNetworkConfig().setAddresses(membersList);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            // Create 5 maps and populate them with some data
            for (int i = 0; i < 5; i++) {
                createAndPopulateMap(client, "map-" + i);
            }

            // Fetch distributed objects list and assert its size
            Collection<DistributedObject> distributedObjects = client.getDistributedObjects();
            assertEquals(5, distributedObjects.size());

            // Delete 3 maps
            for (int i = 0; i < 3; i++) {
                client.getMap("map-" + i).destroy();
            }

            // Assert distributed objects size
            distributedObjects = client.getDistributedObjects();
            assertEquals(2, distributedObjects.size());

            // Re-create 3 maps
            for (int i = 0; i < 3; i++) {
                createAndPopulateMap(client, "map-" + i);
            }

            // Assert distributed objects size
            distributedObjects = client.getDistributedObjects();
            assertEquals(5, distributedObjects.size());
        } finally {
            // Terminate client
            client.getLifecycleService().terminate();
        }
    }

    private void createAndPopulateMap(HazelcastInstance client, String mapName) {
        IMap<Integer, String> map = client.getMap(mapName);
        for (int j = 0; j < 10; j++) {
            map.put(j, "test" + j);
        }
    }
}
