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
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.EventObject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MembershipListenerTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    private class MemberShipEventLogger implements MembershipListener {

        public LinkedBlockingDeque<EventObject> events = new LinkedBlockingDeque<EventObject>();

        public void memberAdded(MembershipEvent event) {
            events.addLast(event);
        }

        public void memberRemoved(MembershipEvent event) {
            events.addLast(event);
        }

    }

    private class InitialMemberShipEventLogger implements InitialMembershipListener {

        public LinkedBlockingDeque<EventObject> events = new LinkedBlockingDeque<EventObject>();

        public void memberAdded(MembershipEvent event) {
            events.addLast(event);
        }

        public void memberRemoved(MembershipEvent event) {
            events.addLast(event);
        }

        @Override
        public void init(InitialMembershipEvent event) {
            events.addLast(event);
        }
    }

    @Test
    public void whenMemberAdded_thenMemberAddedEvent() throws Exception {
        final HazelcastInstance server1 = hazelcastFactory.newHazelcastInstance();
        final MemberShipEventLogger listener = new MemberShipEventLogger();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        client.getCluster().addMembershipListener(listener);

        //start a second server and verify that the listener receives it.
        final HazelcastInstance server2 = hazelcastFactory.newHazelcastInstance();

        //verify that the listener receives member added event.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotEquals("Expecting one or more events", 0, listener.events.size());
                MembershipEvent event = (MembershipEvent) listener.events.getLast();
                assertEquals("Last event should be member added", MembershipEvent.MEMBER_ADDED, event.getEventType());
                assertEquals(server2.getCluster().getLocalMember(), event.getMember());
                assertEquals(getMembers(server1, server2), event.getMembers());
            }
        });
    }

    @Test
    public void givenMixOfListenerExists_whenConnect_thenCallInitialMembershipListener() throws Exception {
        hazelcastFactory.newHazelcastInstance();

        final ClientConfig config = new ClientConfig();

        // first add bunch of *regular* MembershipListener. They do not implement InitialMembershipListener
        config.addListenerConfig(new ListenerConfig().setImplementation(new MemberShipEventLogger()));
        config.addListenerConfig(new ListenerConfig().setImplementation(new MemberShipEventLogger()));
        config.addListenerConfig(new ListenerConfig().setImplementation(new MemberShipEventLogger()));

        // now add an InitialMembershipListener
        // if there is an exception thrown during event delivery to regular listeners
        // then no event will likely be delivered to InitialMemberShipEventLogger
        final InitialMemberShipEventLogger initialListener = new InitialMemberShipEventLogger();
        config.addListenerConfig(new ListenerConfig().setImplementation(initialListener));

        //connect to a grid
        hazelcastFactory.newHazelcastClient(config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("Expecting one event", 1, initialListener.events.size());
                InitialMembershipEvent event = (InitialMembershipEvent) initialListener.events.getLast();
                assertEquals(1, event.getMembers().size());
            }
        });
    }

    @Test
    public void whenMemberRemoved_thenMemberRemovedEvent() throws Exception {
        final HazelcastInstance server1 = hazelcastFactory.newHazelcastInstance();
        final MemberShipEventLogger listener = new MemberShipEventLogger();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        //start a second server and verify that hazelcastFactory listener receives it.
        final HazelcastInstance server2 = hazelcastFactory.newHazelcastInstance();

        client.getCluster().addMembershipListener(listener);

        final Member server2Member = server2.getCluster().getLocalMember();
        server2.shutdown();

        //verify that the correct member removed event was received.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotEquals("Expecting one or more events", 0, listener.events.size());
                MembershipEvent event = (MembershipEvent) listener.events.getLast();
                assertEquals("Last event should be member removed", MembershipEvent.MEMBER_REMOVED, event.getEventType());
                assertEquals(server2Member, event.getMember());
                assertEquals(getMembers(server1), event.getMembers());
            }
        });
    }

    @Test
    public void removedPhantomListener_thenFalse() throws Exception {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        assertFalse(client.getCluster().removeMembershipListener(UuidUtil.newUnsecureUUID()));
    }

    @Test(expected = NullPointerException.class)
    public void removedNullListener_thenException() throws Exception {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        assertFalse(client.getCluster().removeMembershipListener(null));
    }

    @Test(expected = java.lang.NullPointerException.class)
    public void addNullListener_thenException() throws Exception {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        client.getCluster().addMembershipListener(null);
    }

    private Set<Member> getMembers(HazelcastInstance... instances) {
        Set<Member> result = new HashSet<Member>();
        for (HazelcastInstance hz : instances) {
            result.add(hz.getCluster().getLocalMember());
        }
        return result;
    }

    /**
     * related to issue #1181
     */
    @Test
    public void testAddInitialMembership_whenListenerAddedViaClientConfig() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig().setImplementation(mock(InitialMembershipListener.class)));
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void initialMemberEvents_whenAddedViaConfig() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        final InitialMemberShipEventLogger listener = new InitialMemberShipEventLogger();
        clientConfig.addListenerConfig(new ListenerConfig().setImplementation(listener));
        hazelcastFactory.newHazelcastClient(clientConfig);

        EventObject eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(InitialMembershipEvent.class, eventObject);
        InitialMembershipEvent event = (InitialMembershipEvent) eventObject;
        assertEquals(2, event.getMembers().size());
        assertEquals(0, listener.events.size());
    }

    @Test
    public void initialMemberEvents_whenAddedAfterClientStarted() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final InitialMemberShipEventLogger listener = new InitialMemberShipEventLogger();
        client.getCluster().addMembershipListener(listener);

        EventObject eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(InitialMembershipEvent.class, eventObject);
        InitialMembershipEvent event = (InitialMembershipEvent) eventObject;
        assertEquals(2, event.getMembers().size());
        assertEquals(0, listener.events.size());
    }

    @Test
    public void initialMemberEvents_whenAddedAfterClientStartedAsync() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final InitialMemberShipEventLogger listener = new InitialMemberShipEventLogger();
        client.getCluster().addMembershipListener(listener);

        EventObject eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(InitialMembershipEvent.class, eventObject);
        InitialMembershipEvent event = (InitialMembershipEvent) eventObject;
        assertEquals(2, event.getMembers().size());
        assertEquals(0, listener.events.size());
    }

    @Test
    public void initialMemberEvents_whenClusterRestarted() throws InterruptedException {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        final InitialMemberShipEventLogger listener = new InitialMemberShipEventLogger();
        clientConfig.addListenerConfig(new ListenerConfig().setImplementation(listener));
        hazelcastFactory.newHazelcastClient(clientConfig);

        EventObject eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(InitialMembershipEvent.class, eventObject);

        instance1.getLifecycleService().terminate();

        eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(MembershipEvent.class, eventObject);
        assertEquals(MembershipEvent.MEMBER_REMOVED, ((MembershipEvent) eventObject).getEventType());

        instance2.getLifecycleService().terminate();
        hazelcastFactory.newHazelcastInstance();

        eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(MembershipEvent.class, eventObject);
        assertEquals(MembershipEvent.MEMBER_REMOVED, ((MembershipEvent) eventObject).getEventType());

        eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(MembershipEvent.class, eventObject);
        assertEquals(MembershipEvent.MEMBER_ADDED, ((MembershipEvent) eventObject).getEventType());
    }
}
