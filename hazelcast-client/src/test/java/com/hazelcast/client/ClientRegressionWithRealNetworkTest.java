/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.util.ClientStateListener;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientRegressionWithRealNetworkTest extends ClientTestSupport {

    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientPortConnection() {
        final Config config1 = new Config();
        config1.getGroupConfig().setName("foo");
        config1.getNetworkConfig().setPort(5701);
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);
        instance1.getMap("map").put("key", "value");

        final Config config2 = new Config();
        config2.getGroupConfig().setName("bar");
        config2.getNetworkConfig().setPort(5702);
        Hazelcast.newHazelcastInstance(config2);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("bar");
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Object, Object> map = client.getMap("map");
        assertNull(map.put("key", "value"));
        assertEquals(1, map.size());
    }

    @Test
    public void testClientConnectionBeforeServerReady() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                Hazelcast.newHazelcastInstance();
            }
        });

        final CountDownLatch clientLatch = new CountDownLatch(1);
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                ClientConfig config = new ClientConfig();
                config.getNetworkConfig().setConnectionAttemptLimit(10);
                HazelcastClient.newHazelcastClient(config);
                clientLatch.countDown();
            }
        });

        assertOpenEventually(clientLatch);
    }

    @Test
    public void testConnectWithDNSHostnames() throws InterruptedException {
        Config config = new Config();
        config.getNetworkConfig().setPublicAddress("localhost");
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("localhost").setConnectionAttemptLimit(Integer.MAX_VALUE);
        ClientStateListener clientStateListener = new ClientStateListener(clientConfig);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        final ClientConnectionManager connectionManager = clientInstanceImpl.getConnectionManager();

        Hazelcast.newHazelcastInstance(config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(2, connectionManager.getActiveConnections().size());
            }
        });

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                LifecycleEvent.LifecycleState state = event.getState();
                if (state.equals(LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED)) {
                    disconnectedLatch.countDown();
                } else if (state.equals(LifecycleEvent.LifecycleState.CLIENT_CONNECTED)) {
                    connectedLatch.countDown();
                }
            }
        });

        hazelcastInstance.shutdown();

        assertOpenEventually(disconnectedLatch);

        assertOpenEventually(connectedLatch);

        assertEquals(1, connectionManager.getActiveConnections().size());
    }

    @Test
    public void testListenersWhenDNSHostnamesAreUsed() {
        Config config = new Config();
        int heartBeatSeconds = 5;
        config.getNetworkConfig().setPublicAddress("localhost");
        config.setProperty(GroupProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getName(), Integer.toString(heartBeatSeconds));
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress("localhost");
        networkConfig.setConnectionAttemptLimit(Integer.MAX_VALUE);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        final IMap<Integer, Integer> map = client.getMap("test");

        final AtomicInteger eventCount = new AtomicInteger(0);

        map.addEntryListener(new EntryAddedListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                eventCount.incrementAndGet();
            }
        }, false);

        Hazelcast.newHazelcastInstance(config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
                int size = clientInstanceImpl.getConnectionManager().getActiveConnections().size();
                assertEquals(2, size);

            }
        });

        hazelcastInstance.shutdown();

        sleepAtLeastSeconds(2 * heartBeatSeconds);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                map.put(1, 2);
                assertNotEquals(0, eventCount.get());
            }
        });
    }
}
