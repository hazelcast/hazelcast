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

package com.hazelcast.client;

import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ConnectionManagerTest {
    final Credentials credentials = new UsernamePasswordCredentials();

    private LifecycleServiceClientImpl createLifecycleServiceClientImpl(HazelcastClient hazelcastClient, final List<LifecycleState> lifecycleEvents) {
        final LifecycleServiceClientImpl lifecycleService = new LifecycleServiceClientImpl(hazelcastClient);
        lifecycleService.addLifecycleListener(new LifecycleListener() {

            public void stateChanged(LifecycleEvent event) {
                lifecycleEvents.add(event.getState());
            }
        });
        return lifecycleService;
    }

    @Test
    public void testGetConnection() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        final CountDownLatch latch = new CountDownLatch(2);
        final List<LifecycleState> lifecycleEvents = new ArrayList<LifecycleState>();
        final LifecycleServiceClientImpl lifecycleService = createLifecycleServiceClientImpl(client, lifecycleEvents);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setCredentials(credentials).addInetSocketAddress(inetSocketAddress).setConnectionTimeout(60000);
        ConnectionManager connectionManager = new ConnectionManager(client, clientConfig, lifecycleService) {
            protected Connection getNextConnection() {
                latch.countDown();
                return connection;
            }
        };
        ClientBinder binder = mock(ClientBinder.class);
        connectionManager.setBinder(binder);
        connectionManager.getConnection();
        assertEquals(connection, connectionManager.getConnection());
        verify(binder).bind(connection, credentials);
        assertEquals(connection, connectionManager.getConnection());
        assertEquals(1, latch.getCount());
        Thread.sleep(100); // wait a little events to be fired
        assertArrayEquals(new Object[]{LifecycleState.CLIENT_CONNECTION_OPENING}, lifecycleEvents.toArray());
    }

    @Test
    public void testGetConnectionWhenThereIsNoConnection() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final List<LifecycleState> lifecycleEvents = new ArrayList<LifecycleState>();
        final LifecycleServiceClientImpl lifecycleService = createLifecycleServiceClientImpl(client, lifecycleEvents);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setCredentials(credentials).addInetSocketAddress(inetSocketAddress).setConnectionTimeout(60000);
        ConnectionManager connectionManager = new ConnectionManager(client, clientConfig, lifecycleService) {
            protected Connection getNextConnection() {
                return null;
            }
        };
        ClientBinder binder = mock(ClientBinder.class);
        connectionManager.setBinder(binder);
        connectionManager.getConnection();
        assertEquals(null, connectionManager.getConnection());
        assertEquals(null, connectionManager.getConnection());
        assertArrayEquals(new Object[0], lifecycleEvents.toArray());
    }

    @Test
    public void testDestroyConnection() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        final CountDownLatch latch = new CountDownLatch(2);
        final List<LifecycleState> lifecycleEvents = new ArrayList<LifecycleState>();
        final LifecycleServiceClientImpl lifecycleService = createLifecycleServiceClientImpl(client, lifecycleEvents);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setCredentials(credentials).addInetSocketAddress(inetSocketAddress).setConnectionTimeout(60000);
        ConnectionManager connectionManager = new ConnectionManager(client, clientConfig, lifecycleService) {
            protected Connection getNextConnection() {
                latch.countDown();
                return connection;
            }
        };
        ClientBinder binder = mock(ClientBinder.class);
        connectionManager.setBinder(binder);
        assertEquals(connection, connectionManager.getConnection());
        connectionManager.destroyConnection(connection);
        connectionManager.getConnection();
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // wait a little events to be fired
        assertArrayEquals(new Object[]{LifecycleState.CLIENT_CONNECTION_OPENING,
                LifecycleState.CLIENT_CONNECTION_LOST,
                LifecycleState.CLIENT_CONNECTION_OPENING},
                lifecycleEvents.toArray());
    }

    @Test
    @Ignore
    public void testSameMemberAdded() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        final CountDownLatch latch = new CountDownLatch(2);
        final List<LifecycleState> lifecycleEvents = new ArrayList<LifecycleState>();
        final LifecycleServiceClientImpl lifecycleService = createLifecycleServiceClientImpl(client, lifecycleEvents);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setCredentials(credentials).addInetSocketAddress(inetSocketAddress).setConnectionTimeout(60000);
        ConnectionManager connectionManager = new ConnectionManager(client, clientConfig, lifecycleService) {
            protected Connection getNextConnection() {
                latch.countDown();
                return connection;
            }
        };
        ClientBinder binder = mock(ClientBinder.class);
        connectionManager.setBinder(binder);
        Cluster cluster = mock(Cluster.class);
        Member member = mock(Member.class);
        when(member.getInetSocketAddress()).thenReturn(inetSocketAddress);
        MembershipEvent membershipEvent = new MembershipEvent(cluster, member, MembershipEvent.MEMBER_ADDED);
        connectionManager.memberAdded(membershipEvent);
        connectionManager.getClusterMembers().contains(inetSocketAddress);
        assertEquals(1, connectionManager.getClusterMembers().size());
        assertArrayEquals(new Object[0], lifecycleEvents.toArray());
    }

    @Test
    @Ignore
    public void testDifferentMemberAdded() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        final List<LifecycleState> lifecycleEvents = new ArrayList<LifecycleState>();
        final LifecycleServiceClientImpl lifecycleService = createLifecycleServiceClientImpl(client, lifecycleEvents);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setCredentials(credentials).addInetSocketAddress(inetSocketAddress).setConnectionTimeout(60000);
        ConnectionManager connectionManager = new ConnectionManager(client, clientConfig, lifecycleService) {
            protected Connection getNextConnection() {
                return connection;
            }
        };
        ClientBinder binder = mock(ClientBinder.class);
        connectionManager.setBinder(binder);
        Cluster cluster = mock(Cluster.class);
        InetSocketAddress inetSocketAddress2 = new InetSocketAddress("hostname", 5702);
        Member member = mock(Member.class);
        when(member.getInetSocketAddress()).thenReturn(inetSocketAddress2);
        MembershipEvent membershipEvent = new MembershipEvent(cluster, member, MembershipEvent.MEMBER_ADDED);
        connectionManager.memberAdded(membershipEvent);
        connectionManager.getClusterMembers().contains(inetSocketAddress2);
        assertEquals(2, connectionManager.getClusterMembers().size());
        assertArrayEquals(new Object[0], lifecycleEvents.toArray());
    }

    @Test
    public void testMemberRemoved() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        final List<LifecycleState> lifecycleEvents = new ArrayList<LifecycleState>();
        final LifecycleServiceClientImpl lifecycleService = createLifecycleServiceClientImpl(client, lifecycleEvents);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setCredentials(credentials).addInetSocketAddress(inetSocketAddress).setConnectionTimeout(60000);
        ConnectionManager connectionManager = new ConnectionManager(client, clientConfig, lifecycleService) {
            protected Connection getNextConnection() {
                return connection;
            }
        };
        ClientBinder binder = mock(ClientBinder.class);
        connectionManager.setBinder(binder);
        Cluster cluster = mock(Cluster.class);
        Member member = mock(Member.class);
        when(member.getInetSocketAddress()).thenReturn(inetSocketAddress);
        MembershipEvent membershipEvent = new MembershipEvent(cluster, member, MembershipEvent.MEMBER_REMOVED);
        connectionManager.memberRemoved(membershipEvent);
        assertEquals(0, connectionManager.getClusterMembers().size());
        assertArrayEquals(new Object[0], lifecycleEvents.toArray());
    }

    @Test
    public void testUpdateMembers() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        Cluster cluster = mock(Cluster.class);
        when(client.getCluster()).thenReturn(cluster);
        Set<Member> members = new HashSet<Member>();
        Member member1 = mock(Member.class);
        Member member2 = mock(Member.class);
        Member member3 = mock(Member.class);
        InetSocketAddress inetSocketAddress1 = new InetSocketAddress("localhost", 9701);
        InetSocketAddress inetSocketAddress2 = new InetSocketAddress("localhost", 9702);
        InetSocketAddress inetSocketAddress3 = new InetSocketAddress("localhost", 9703);
        when(member1.getInetSocketAddress()).thenReturn(inetSocketAddress1);
        when(member2.getInetSocketAddress()).thenReturn(inetSocketAddress2);
        when(member3.getInetSocketAddress()).thenReturn(inetSocketAddress3);
        members.add(member1);
        members.add(member2);
        members.add(member3);
        when(cluster.getMembers()).thenReturn(members);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        final List<LifecycleState> lifecycleEvents = new ArrayList<LifecycleState>();
        final LifecycleServiceClientImpl lifecycleService = createLifecycleServiceClientImpl(client, lifecycleEvents);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setCredentials(credentials).addInetSocketAddress(inetSocketAddress).setConnectionTimeout(60000);
        ConnectionManager connectionManager = new ConnectionManager(client, clientConfig, lifecycleService) {
            protected Connection getNextConnection() {
                return connection;
            }
        };
        ClientBinder binder = mock(ClientBinder.class);
        connectionManager.setBinder(binder);
        connectionManager.updateMembers();
        assertTrue(connectionManager.getClusterMembers().contains(inetSocketAddress1));
        assertTrue(connectionManager.getClusterMembers().contains(inetSocketAddress2));
        assertTrue(connectionManager.getClusterMembers().contains(inetSocketAddress3));
        assertFalse(connectionManager.getClusterMembers().contains(inetSocketAddress));
        assertEquals(3, connectionManager.getClusterMembers().size());
        assertArrayEquals(new Object[0], lifecycleEvents.toArray());
    }

    @Test
    public void testShouldExecuteOnDisconnect() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        final List<LifecycleState> lifecycleEvents = new ArrayList<LifecycleState>();
        final LifecycleServiceClientImpl lifecycleService = createLifecycleServiceClientImpl(client, lifecycleEvents);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setCredentials(credentials).addInetSocketAddress(inetSocketAddress).setConnectionTimeout(60000);
        ConnectionManager connectionManager = new ConnectionManager(client, clientConfig, lifecycleService) {
            protected Connection getNextConnection() {
                return connection;
            }
        };
        assertTrue(connectionManager.shouldExecuteOnDisconnect(connection));
        assertFalse(connectionManager.shouldExecuteOnDisconnect(connection));
        assertArrayEquals(new Object[0], lifecycleEvents.toArray());
    }
}
