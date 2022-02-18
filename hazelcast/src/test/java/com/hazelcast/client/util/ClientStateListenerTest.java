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

package com.hazelcast.client.util;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.FutureUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;
import static com.hazelcast.test.TimeConstants.MINUTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientStateListenerTest extends ClientTestSupport {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test(timeout = MINUTE * 10)
    public void testWithUnusedClientConfig()
            throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);

        assertFalse(listener.awaitConnected(1, MILLISECONDS));

        assertFalse(listener.awaitDisconnected(1, MILLISECONDS));

        assertFalse(listener.isConnected());

        assertFalse(listener.isShutdown());

        assertFalse(listener.isStarted());

        assertEquals(STARTING, listener.getCurrentState());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientASYNCStartConnected() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);

        hazelcastFactory.newHazelcastInstance();

        hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(listener.awaitConnected());

        assertFalse(listener.awaitDisconnected(1, MILLISECONDS));

        assertTrue(listener.isConnected());

        assertFalse(listener.isShutdown());

        assertTrue(listener.isStarted());

        assertEquals(CLIENT_CONNECTED, listener.getCurrentState());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientReconnectModeAsyncConnected() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);

        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        hazelcastFactory.newHazelcastClient(clientConfig);

        hazelcastInstance.shutdown();

        assertTrue(listener.awaitDisconnected());

        assertFalse(listener.isConnected());

        assertFalse(listener.isShutdown());

        assertTrue(listener.isStarted());

        assertEquals(CLIENT_DISCONNECTED, listener.getCurrentState());

        hazelcastFactory.newHazelcastInstance();

        assertTrue(listener.awaitConnected());

        assertTrue(listener.isConnected());

        assertFalse(listener.isShutdown());

        assertTrue(listener.isStarted());

        assertEquals(CLIENT_CONNECTED, listener.getCurrentState());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientReconnectModeAsyncConnectedMultipleThreads() throws InterruptedException {
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        ClientConfig clientConfig = new ClientConfig();
        final ClientStateListener listener = new ClientStateListener(clientConfig);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);

        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        hazelcastFactory.newHazelcastClient(clientConfig);

        hazelcastInstance.shutdown();

        List<Future> futures = new ArrayList<Future>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        assertTrue(listener.awaitDisconnected());
                    } catch (InterruptedException e) {
                        fail("Should not be interrupted");
                    }

                    assertFalse(listener.isConnected());

                    assertFalse(listener.isShutdown());

                    assertTrue(listener.isStarted());

                    assertEquals(CLIENT_DISCONNECTED, listener.getCurrentState());
                }
            }));
        }

        FutureUtil.waitForever(futures);
        assertTrue(FutureUtil.allDone(futures));

        hazelcastFactory.newHazelcastInstance();

        futures.clear();
        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        listener.awaitConnected();
                    } catch (InterruptedException e) {
                        fail("Should not be interrupted");
                    }

                    assertTrue(listener.isConnected());

                    assertFalse(listener.isShutdown());

                    assertTrue(listener.isStarted());

                    assertEquals(CLIENT_CONNECTED, listener.getCurrentState());
                }
            }));
        }

        FutureUtil.waitForever(futures);
        assertTrue(FutureUtil.allDone(futures));
    }

    @Test(timeout = MINUTE * 10)
    public void testClientReconnectModeOffDisconnected() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        final ClientStateListener listener = new ClientStateListener(clientConfig);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);

        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        hazelcastFactory.newHazelcastClient(clientConfig);

        hazelcastInstance.shutdown();

        hazelcastFactory.newHazelcastInstance();

        assertTrue(listener.awaitDisconnected());

        assertFalse(listener.isConnected());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(SHUTDOWN, listener.getCurrentState());
            }
        });

        assertFalse(listener.isStarted());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientConnectedWithTimeout() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);

        hazelcastFactory.newHazelcastInstance();

        hazelcastFactory.newHazelcastClient(clientConfig);
        assertTrue(listener.awaitConnected());

        assertFalse(listener.awaitDisconnected(1, MILLISECONDS));

        assertTrue(listener.isConnected());

        assertFalse(listener.isShutdown());

        assertTrue(listener.isStarted());

        assertEquals(CLIENT_CONNECTED, listener.getCurrentState());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientConnectedWithoutTimeout() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);

        hazelcastFactory.newHazelcastInstance();

        hazelcastFactory.newHazelcastClient(clientConfig);

        listener.awaitConnected();

        assertEquals(CLIENT_CONNECTED, listener.getCurrentState());

        assertTrue(listener.isConnected());

        assertTrue(listener.isStarted());

        assertFalse(listener.awaitDisconnected(1, MILLISECONDS));

        assertFalse(listener.isShutdown());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientDisconnectedWithTimeout() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);

        hazelcastFactory.newHazelcastInstance();

        hazelcastFactory.newHazelcastClient(clientConfig).shutdown();

        assertFalse(listener.awaitConnected(1, MILLISECONDS));

        assertTrue(listener.awaitDisconnected(1, MILLISECONDS));

        assertFalse(listener.isConnected());

        assertTrue(listener.isShutdown());

        assertFalse(listener.isStarted());

        assertEquals(SHUTDOWN, listener.getCurrentState());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientDisconnectedWithoutTimeout() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);

        hazelcastFactory.newHazelcastInstance();

        hazelcastFactory.newHazelcastClient(clientConfig).shutdown();

        listener.awaitDisconnected();

        assertFalse(listener.awaitConnected(1, MILLISECONDS));

        assertFalse(listener.isConnected());

        assertTrue(listener.isShutdown());

        assertFalse(listener.isStarted());

        assertEquals(SHUTDOWN, listener.getCurrentState());
    }

}
