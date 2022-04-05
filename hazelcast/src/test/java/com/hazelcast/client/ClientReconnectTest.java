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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipAdapter;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReconnectTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testClientReconnectOnClusterDown() {
        final HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        ClientTestSupport.ReconnectListener reconnectListener = new ClientTestSupport.ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);
        IMap<String, String> m = client.getMap("default");
        h1.shutdown();
        hazelcastFactory.newHazelcastInstance();
        assertOpenEventually(reconnectListener.reconnectedLatch);
        assertNull(m.put("test", "test"));
        assertEquals("test", m.get("test"));
    }

    @Test
    public void testReconnectToNewInstanceAtSameAddress() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        Address localAddress = instance.getCluster().getLocalMember().getAddress();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final CountDownLatch memberRemovedLatch = new CountDownLatch(1);
        client.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemovedLatch.countDown();
            }
        });

        instance.shutdown();
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance(localAddress);

        assertOpenEventually(memberRemovedLatch);

        assertTrueEventually(() -> {
            assertClusterSize(1, client);
            Iterator<Member> iterator = client.getCluster().getMembers().iterator();
            Member member = iterator.next();
            assertEquals(instance2.getCluster().getLocalMember(), member);
        });
    }

    @Test
    public void testClientShutdownIfReconnectionNotPossible() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
                shutdownLatch.countDown();
            }
        });
        server.shutdown();

        assertOpenEventually(shutdownLatch);
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testRequestShouldFailOnShutdown() {
        final HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> test = client.getMap("test");
        test.put("key", "value");
        server.shutdown();
        test.get("key");
    }

    @Test
    public void testCallbackAfterClientShutdown() {
        final HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> test = client.getMap("test");
        server.shutdown();
        CompletionStage<Object> future = test.putAsync("key", "value");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> reference = new AtomicReference<>();
        future.exceptionally(t -> {
            reference.set(t);
            latch.countDown();
            return null;
        });
        assertOpenEventually(latch);
        assertInstanceOf(HazelcastClientNotActiveException.class, reference.get());
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testExceptionAfterClientShutdown() {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> test = client.getMap("test");
        test.put("key", "value");
        client.shutdown();
        //to force weak references to be cleaned and get not active exception from serialization service
        System.gc();
        test.get("key");
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testExceptionAfterClientShutdown_fromClientConnectionManager() {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> test = client.getMap("test");
        test.put("key", "value");
        client.shutdown();
        test.size();
    }

    @Test
    public void testShutdownClient_whenThereIsNoCluster() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        client.shutdown();
    }
}
