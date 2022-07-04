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

package com.hazelcast.client;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationparker.impl.OperationParkerImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationRegistry;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.ReliableMessageListener;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDisconnectTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testClientOperationCancelled_whenDisconnected() throws Exception {
        Config config = new Config();
        config.setProperty(ClusterProperty.CLIENT_CLEANUP_TIMEOUT.getName(), String.valueOf(Integer.MAX_VALUE));
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance(config);
        final String queueName = "q";

        final HazelcastInstance clientInstance = hazelcastFactory.newHazelcastClient();
        final UUID uuid = clientInstance.getLocalEndpoint().getUuid();
        final CountDownLatch clientDisconnectedFromNode = new CountDownLatch(1);
        hazelcastInstance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {

            }

            @Override
            public void clientDisconnected(Client client) {
                if (client.getUuid().equals(uuid)) {
                    clientDisconnectedFromNode.countDown();
                }
            }
        });
        new Thread(new Runnable() {
            @Override
            public void run() {
                IQueue<Integer> queue = clientInstance.getQueue(queueName);
                try {
                    queue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (HazelcastInstanceNotActiveException e) {
                    ignore(e);
                }
            }
        }).start();

        SECONDS.sleep(2);

        clientInstance.shutdown();
        assertOpenEventually(clientDisconnectedFromNode);

        final IQueue<Integer> queue = hazelcastInstance.getQueue(queueName);
        queue.add(1);
        //dead client should not be able to consume item from queue
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(queue.size(), 1);
            }
        }, 3);
    }

    @Test
    public void testClientOperationCancelled_whenDisconnected_lock() throws Exception {
        Config config = new Config();
        config.setProperty(ClusterProperty.CLIENT_CLEANUP_TIMEOUT.getName(), String.valueOf(Integer.MAX_VALUE));
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance(config);
        final String name = "m";

        final IMap<Object, Object> map = hazelcastInstance.getMap(name);
        final String key = "key";
        map.lock(key);

        final HazelcastInstance clientInstance = hazelcastFactory.newHazelcastClient();
        final CountDownLatch clientDisconnectedFromNode = new CountDownLatch(1);
        final UUID uuid = clientInstance.getLocalEndpoint().getUuid();
        hazelcastInstance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {

            }

            @Override
            public void clientDisconnected(Client client) {
                if (client.getUuid().equals(uuid)) {
                    clientDisconnectedFromNode.countDown();
                }
            }
        });
        new Thread(new Runnable() {
            @Override
            public void run() {
                IMap<Object, Object> clientMap = clientInstance.getMap(name);
                try {
                    clientMap.lock(key);
                } catch (Exception e) {
                    ignore(e);
                }

            }
        }).start();

        SECONDS.sleep(2);

        clientInstance.shutdown();
        assertOpenEventually(clientDisconnectedFromNode);

        map.unlock(key);
        //dead client should not be able to acquire the lock.
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(map.isLocked(key));
            }
        }, 3);
    }

    @Test
    public void testPendingInvocationAndWaitEntryCancelled_whenDisconnected_withQueue() {
        Config config = new Config();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(config);
        final String name = randomName();

        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    client.getQueue(name).take();
                } catch (Throwable ignored) {
                }
            }
        });

        assertNonEmptyPendingInvocationAndWaitSet(server);

        client.shutdown();

        assertEmptyPendingInvocationAndWaitSet(server);
    }

    @Test
    public void testPendingInvocationAndWaitEntryCancelled_whenDisconnected_withReliableTopic() {
        Config config = new Config();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(config);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        // ReliableTopic listener registers a blocking invocation
        client.getReliableTopic(randomName()).addMessageListener(new NopReliableMessageListener());

        assertNonEmptyPendingInvocationAndWaitSet(server);

        client.shutdown();

        assertEmptyPendingInvocationAndWaitSet(server);
    }

    private void assertNonEmptyPendingInvocationAndWaitSet(HazelcastInstance server) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(server);
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        final InvocationRegistry invocationRegistry = operationService.getInvocationRegistry();
        final OperationParkerImpl operationParker = (OperationParkerImpl) nodeEngine.getOperationParker();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(invocationRegistry.entrySet().isEmpty());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(operationParker.getTotalParkedOperationCount() > 0);
            }
        });
    }

    private void assertEmptyPendingInvocationAndWaitSet(HazelcastInstance server) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(server);
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        final InvocationRegistry invocationRegistry = operationService.getInvocationRegistry();
        final OperationParkerImpl operationParker = (OperationParkerImpl) nodeEngine.getOperationParker();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(invocationRegistry.entrySet().isEmpty());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, operationParker.getTotalParkedOperationCount());
            }
        });
    }

    private static class NopReliableMessageListener implements ReliableMessageListener<Object> {
        @Override
        public long retrieveInitialSequence() {
            return 0;
        }

        @Override
        public void storeSequence(long sequence) {
        }

        @Override
        public boolean isLossTolerant() {
            return false;
        }

        @Override
        public boolean isTerminal(Throwable failure) {
            return false;
        }

        @Override
        public void onMessage(Message<Object> message) {
        }
    }

    @Test
    public void testConnectionEventsFiredForClientsOnServerConnectionManager() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();

        CountDownLatch clientConnected = new CountDownLatch(1);
        CountDownLatch clientDisconnected = new CountDownLatch(1);
        getNodeEngineImpl(server).getNode().getServer().addConnectionListener(new ConnectionListener<ServerConnection>() {
            @Override
            public void connectionAdded(ServerConnection connection) {
                if (connection.isClient()) {
                    clientConnected.countDown();
                }
            }

            @Override
            public void connectionRemoved(ServerConnection connection) {
                if (connection.isClient()) {
                    clientDisconnected.countDown();
                }
            }
        });

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        assertOpenEventually(clientConnected);

        client.shutdown();
        assertOpenEventually(clientDisconnected);
    }
}
