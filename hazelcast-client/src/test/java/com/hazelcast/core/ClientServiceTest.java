/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ClientServiceTest {

    @Test
    public void getConnectedClients() {
        Config config = new Config();
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addInetSocketAddress(h.getCluster().getLocalMember().getInetSocketAddress());
        Map<Integer, HazelcastClient> map = new HashMap<Integer, HazelcastClient>();
        for (int i = 0; i < 5; i++) {
            HazelcastClient client = HazelcastClient.newHazelcastClient(clientConfig);
            map.put(i, client);
        }
        assertEquals(map.size(), h.getClientService().getConnectedClients().size());
        for (Client client : h.getClientService().getConnectedClients()) {
            assertEquals(ClientType.Native, client.getClientType());
        }
        for (HazelcastClient client : map.values()) {
            client.getLifecycleService().shutdown();
        }
    }

    @Test
    public void addListener() throws InterruptedException {
        int numClients = 5;
        Config config = new Config();
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addInetSocketAddress(h.getCluster().getLocalMember().getInetSocketAddress());
        final CountDownLatch connected = new CountDownLatch(numClients);
        final CountDownLatch disconnected = new CountDownLatch(numClients);
        h.getClientService().addClientListener(new ClientListener() {
            public void clientConnected(Client client) {
                connected.countDown();
            }

            public void clientDisconnected(Client client) {
                disconnected.countDown();
            }
        });
        Map<Integer, HazelcastClient> map = new HashMap<Integer, HazelcastClient>();
        for (int i = 0; i < numClients; i++) {
            HazelcastClient client = HazelcastClient.newHazelcastClient(clientConfig);
            map.put(i, client);
        }
        assertTrue(connected.await(5000, TimeUnit.MILLISECONDS));
        assertEquals(numClients, disconnected.getCount());
        for (HazelcastClient client : map.values()) {
            client.getLifecycleService().shutdown();
        }
        assertTrue(disconnected.await(5000, TimeUnit.MILLISECONDS));
    }
}
