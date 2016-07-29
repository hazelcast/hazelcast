/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.listeners;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientTestUtil;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.listener.ClientEventRegistration;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Test;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractListenersOnReconnectTest extends HazelcastTestSupport {

    protected HazelcastInstance client;
    private int EVENT_COUNT = 10;
    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    private void testListenersInternal() {
        int clusterSize = factory.getAllHazelcastInstances().size();
        assertClusterSizeEventually(clusterSize, client);

        final AtomicInteger eventCount = new AtomicInteger();
        final String registrationId = addListener(eventCount);

        terminateRandomNode();
        factory.newHazelcastInstance();
        assertClusterSizeEventually(clusterSize, client);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotEquals(0, getClientEventRegistrations(client, registrationId).size());
            }
        });

        for (int i = 0; i < EVENT_COUNT; i++) {
            produceEvent();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(EVENT_COUNT, eventCount.get());
            }
        });

        assertTrue(removeListener(registrationId));
    }

    private void terminateRandomNode() {
        int clusterSize = factory.getAllHazelcastInstances().size();
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        factory.getAllHazelcastInstances().toArray(instances);
        int randNode = new Random().nextInt(clusterSize);
        instances[randNode].getLifecycleService().terminate();
    }

    @Test
    public void testListenersNonSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = createClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        client = factory.newHazelcastClient(clientConfig);

        testListenersInternal();
    }

    @Test
    public void testListenersSmartRouting() {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = createClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersInternal();
    }

    @Test
    public void testListenersSmartRoutingMultipleServer() {
        factory.newHazelcastInstance();
        factory.newHazelcastInstance();
        factory.newHazelcastInstance();

        ClientConfig clientConfig = createClientConfig();
        client = factory.newHazelcastClient(clientConfig);
        testListenersInternal();
    }

    @Test
    public void testListenersNonSmartRoutingMultipleServer() {
        factory.newHazelcastInstance();
        factory.newHazelcastInstance();
        factory.newHazelcastInstance();

        ClientConfig clientConfig = createClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        client = factory.newHazelcastClient(clientConfig);
        testListenersInternal();
    }

    private ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        return clientConfig;
    }

    private Collection<ClientEventRegistration> getClientEventRegistrations(HazelcastInstance client, String id) {
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) clientImpl.getListenerService();
        return listenerService.getActiveRegistrations(id);
    }

    protected abstract String addListener(final AtomicInteger eventCount);

    protected abstract void produceEvent();

    protected abstract boolean removeListener(String registrationId);
}
