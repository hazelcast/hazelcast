/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.heartbeat;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.spi.impl.ConnectionHeartbeatListener;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.TestClientRegistry;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientHeartbeatTest extends HazelcastTestSupport {
    TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testHeartbeatStoppedEvent() throws InterruptedException {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), "3000");
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "500");
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        connectionManager.addConnectionHeartbeatListener(new ConnectionHeartbeatListener() {
            @Override
            public void heartBeatStarted(Connection connection) {
            }

            @Override
            public void heartBeatStopped(Connection connection) {
                countDownLatch.countDown();
            }
        });

        blockMessagesFromInstance(instance, client);
        assertOpenEventually(countDownLatch);
    }

    @Test(expected = TargetDisconnectedException.class)
    public void testInvocation_whenHeartbeatStopped() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), "3000");
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "500");
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        String keyOwnedByInstance2 = generateKeyOwnedBy(instance2);
        IMap map = client.getMap(randomString());
        map.put(keyOwnedByInstance2, randomString());
        blockMessagesFromInstance(instance2, client);
        map.put(keyOwnedByInstance2, randomString());
    }

    @Test
    public void testAsyncInvocation_whenHeartbeatStopped() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), "3000");
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "500");
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        IMap map = client.getMap(randomString());
        String keyOwnedByInstance2 = generateKeyOwnedBy(instance2);
        map.put(keyOwnedByInstance2, randomString());
        blockMessagesFromInstance(instance2, client);
        ClientDelegatingFuture future = (ClientDelegatingFuture) map.putAsync(keyOwnedByInstance2, randomString());
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        future.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {

            }

            @Override
            public void onFailure(Throwable t) {
                if (t.getCause() instanceof TargetDisconnectedException) {
                    countDownLatch.countDown();
                }
            }
        });
        assertOpenEventually(countDownLatch);
    }

    private void blockMessagesFromInstance(HazelcastInstance instance, HazelcastInstance client) {
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        Address address = instance.getCluster().getLocalMember().getAddress();
        ((TestClientRegistry.MockClientConnectionManager) connectionManager).block(address);
    }

    private void unblockMessagesFromInstance(HazelcastInstance instance, HazelcastInstance client) {
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        Address address = instance.getCluster().getLocalMember().getAddress();
        ((TestClientRegistry.MockClientConnectionManager) connectionManager).unblock(address);
    }

    private HazelcastClientInstanceImpl getHazelcastClientInstanceImpl(HazelcastInstance client) {
        HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
        return clientProxy.client;
    }

}
