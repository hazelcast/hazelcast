/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ConnectionManagerTest {
    @Test
    public void testGetConnection() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        final CountDownLatch latch = new CountDownLatch(2);
        ConnectionManager connectionManager = new ConnectionManager(client, inetSocketAddress) {
            protected Connection getNextConnection() {
                latch.countDown();
                return connection;
            }
        };
        ClientBinder binder = mock(ClientBinder.class);
        connectionManager.setBinder(binder);
        connectionManager.getConnection();
        assertEquals(connection, connectionManager.getConnection());
        verify(binder).bind(connection);
        assertEquals(connection, connectionManager.getConnection());
        assertEquals(1, latch.getCount());
    }

    @Test
    public void testGetConnectionWhenThereIsNoConnection() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        ConnectionManager connectionManager = new ConnectionManager(client, inetSocketAddress) {
            protected Connection getNextConnection() {
                return null;
            }
        };
        ClientBinder binder = mock(ClientBinder.class);
        connectionManager.setBinder(binder);
        connectionManager.getConnection();
        assertEquals(null, connectionManager.getConnection());
        assertEquals(null, connectionManager.getConnection());
    }

    @Test
    public void testDestroyConnection() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        final CountDownLatch latch = new CountDownLatch(2);
        ConnectionManager connectionManager = new ConnectionManager(client, inetSocketAddress) {
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
    }

    @Test
    public void testSameMemberAdded() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        final CountDownLatch latch = new CountDownLatch(2);
        ConnectionManager connectionManager = new ConnectionManager(client, inetSocketAddress) {
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
    }

    @Test
    public void testDifferentMemberAdded() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        ConnectionManager connectionManager = new ConnectionManager(client, inetSocketAddress) {
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
    }

    @Test
    public void testMemberRemoved() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        ConnectionManager connectionManager = new ConnectionManager(client, inetSocketAddress) {
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
        ConnectionManager connectionManager = new ConnectionManager(client, inetSocketAddress) {
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
    }

    @Test
    public void testShouldExecuteOnDisconnect() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 5701);
        final Connection connection = mock(Connection.class);
        ConnectionManager connectionManager = new ConnectionManager(client, inetSocketAddress) {
            protected Connection getNextConnection() {
                return connection;
            }
        };
        assertTrue(connectionManager.shouldExecuteOnDisconnect(connection));
        assertFalse(connectionManager.shouldExecuteOnDisconnect(connection));
    }
}
