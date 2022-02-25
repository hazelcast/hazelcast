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

package com.hazelcast.client.connectionstrategy;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfiguredBehaviourTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = HazelcastClientOfflineException.class)
    public void testAsyncStartTrueNoCluster() {

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        client.getMap(randomMapName());
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testAsyncStartTrueNoCluster_thenShutdown() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        client.shutdown();

        client.getMap(randomMapName());
    }


    @Test
    public void testAsyncStartTrue() {
        CountDownLatch connectedLatch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();

        // reserve a member address so that the client can be configured to look for it
        Address memberAddress = hazelcastFactory.nextAddress();

        // trying 8.8.8.8 address will delay the initial connection since no such server exist
        clientConfig.getNetworkConfig().addAddress("8.8.8.8", memberAddress.getHost() + ":" + memberAddress.getPort());
        clientConfig.addListenerConfig(new ListenerConfig((LifecycleListener) event -> {
            if (event.getState().equals(CLIENT_CONNECTED)) {
                connectedLatch.countDown();
            }
        }));
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        hazelcastFactory.newHazelcastInstance(memberAddress, null);

        assertOpenEventually(connectedLatch);

        assertTrueEventually(() -> {
            try {
                client.getMap(randomMapName());
            } catch (Exception e) {
                fail();
            }
        });
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testReconnectModeOFFSingleMember() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (LifecycleEvent.LifecycleState.SHUTDOWN.equals(event.getState())) {
                shutdownLatch.countDown();
            }
        });

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hazelcastInstance.shutdown();
        assertOpenEventually(shutdownLatch);

        map.put(1, 5);
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testReconnectModeOFFTwoMembers() {
        hazelcastFactory.newInstances(getConfig(), 2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (LifecycleEvent.LifecycleState.SHUTDOWN.equals(event.getState())) {
                shutdownLatch.countDown();
            }
        });

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hazelcastFactory.shutdownAllMembers();
        assertOpenEventually(shutdownLatch);

        map.put(1, 5);
    }

    @Test(expected = HazelcastClientOfflineException.class)
    public void testReconnectModeASYNCSingleMemberInitiallyOffline() {
        ClientConfig clientConfig = new ClientConfig();

        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED.equals(event.getState())) {
                disconnectedLatch.countDown();
            }
        });

        hazelcastInstance.shutdown();
        assertOpenEventually(disconnectedLatch);
        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNCSingleMember() {
        hazelcastFactory.newHazelcastInstance();

        CountDownLatch connectedLatch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig((LifecycleListener) event -> {
            if (event.getState().equals(CLIENT_CONNECTED)) {
                connectedLatch.countDown();
            }
        }));
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        assertOpenEventually(connectedLatch);

        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNC_clusterDown_clientGetsOfflineExcption() {
        HazelcastInstance member1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance member2 = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        IMap<Object, Object> map = client.getMap(randomMapName());

        member1.shutdown();
        member2.shutdown();

        assertTrue(client.getLifecycleService().isRunning());
        for (int i = 0; i < 100; i++) {
            try {
                map.get(randomString());
                fail("map.get should throw HazelcastClientOfflineException");
            } catch (HazelcastClientOfflineException ignored) {
            }
        }
    }

    @Test
    public void testReconnectModeASYNCSingleMemberStartLate() {
        CountDownLatch initialConnectionLatch = new CountDownLatch(1);
        CountDownLatch reconnectedLatch = new CountDownLatch(1);

        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.addListenerConfig(new ListenerConfig((LifecycleListener) event -> {
            if (event.getState().equals(CLIENT_CONNECTED)) {
                initialConnectionLatch.countDown();
            }
        }));
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertOpenEventually(initialConnectionLatch);

        hazelcastInstance.shutdown();

        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState().equals(CLIENT_CONNECTED)) {
                reconnectedLatch.countDown();
            }
        });

        hazelcastFactory.newHazelcastInstance();

        assertTrue(client.getLifecycleService().isRunning());
        assertOpenEventually(reconnectedLatch);

        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNCTwoMembers() {
        hazelcastFactory.newInstances(getConfig(), 2);

        CountDownLatch connectedLatch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig((LifecycleListener) event -> {
            if (event.getState().equals(CLIENT_CONNECTED)) {
                connectedLatch.countDown();
            }
        }));
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        assertOpenEventually(connectedLatch);

        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hazelcastFactory.shutdownAllMembers();

        HazelcastInstance[] instances = hazelcastFactory.newInstances(getConfig(), 2);

        assertTrueEventually(() -> {
            Set<Member> actualMembers = client.getCluster().getMembers();
            Set<Member> expectedMembers = instances[0].getCluster().getMembers();
            assertEquals(expectedMembers, actualMembers);
        });

        assertTrueEventually(() -> {
            try {
                map.get(1);
            } catch (HazelcastClientOfflineException e) {
                fail();
            }
        });
    }
}
