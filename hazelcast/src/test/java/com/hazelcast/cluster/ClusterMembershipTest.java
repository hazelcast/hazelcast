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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.EventObject;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClusterMembershipTest extends HazelcastTestSupport {

    private static final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @AfterClass
    public static void tearDown() throws Exception {
        executorService.shutdown();
    }

    @Test(expected = NullPointerException.class)
    public void testAddMembershipListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        Cluster cluster = hz.getCluster();

        cluster.addMembershipListener(null);
    }

    @Test
    public void testAddMembershipListener_whenListenerRegisteredTwice() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        Cluster cluster = hz1.getCluster();

        final MembershipListener membershipListener = mock(MembershipListener.class);

        String id1 = cluster.addMembershipListener(membershipListener);
        String id2 = cluster.addMembershipListener(membershipListener);

        // first we check if the registration id's are different
        assertNotEquals(id1, id2);

        // an now we make sure that if a member joins the cluster, the same interface gets invoked twice.
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                //now we verify that the memberAdded method is called twice.
                verify(membershipListener, times(2)).memberAdded(any(MembershipEvent.class));
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveMembershipListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        Cluster cluster = hz.getCluster();

        cluster.removeMembershipListener(null);
    }

    @Test
    public void testRemoveMembershipListener_whenNonExistingRegistrationId() {
        HazelcastInstance hz = createHazelcastInstance();
        Cluster cluster = hz.getCluster();

        boolean result = cluster.removeMembershipListener("notexist");

        assertFalse(result);
    }

    @Test
    public void testRemoveMembershipListener() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        Cluster cluster = hz1.getCluster();

        MembershipListener membershipListener = mock(MembershipListener.class);

        String id = cluster.addMembershipListener(membershipListener);
        boolean removed = cluster.removeMembershipListener(id);

        assertTrue(removed);

        // now we add a member
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        // and verify that the listener isn't called.
        verify(membershipListener, never()).memberAdded(any(MembershipEvent.class));
    }

    @Test
    public void testMembershipListener() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        MembershipListenerImpl listener = new MembershipListenerImpl();
        hz1.getCluster().addMembershipListener(listener);

        //start a second instance
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        assertEventuallySizeAtLeast(listener.events, 1);
        assertMembershipAddedEvent(listener.events.get(0), hz2.getCluster().getLocalMember(),
                hz1.getCluster().getLocalMember(), hz2.getCluster().getLocalMember());

        //terminate the second instance
        Member member2 = hz2.getCluster().getLocalMember();
        hz2.shutdown();

        assertEventuallySizeAtLeast(listener.events, 2);
        assertMembershipRemovedEvent(listener.events.get(1), member2, hz1.getCluster().getLocalMember());
    }

    @Test
    public void testNodesAbleToJoinFromMultipleThreads() throws InterruptedException {
        final int instanceCount = 6;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(instanceCount);
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        final String mapName = randomMapName();
        // index config is added since it was blocking post join operations.
        config.getMapConfig(mapName).addMapIndexConfig(new MapIndexConfig("name", false));

        final CyclicBarrier barrier = new CyclicBarrier(instanceCount + 1);
        for (int i = 0; i < instanceCount; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    final HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
                    hz.getMap(mapName);
                    assertClusterSizeEventually(instanceCount, hz, 20);

                    assertExecutionsDone(barrier);
                }
            });
        }
        assertExecutionsDone(barrier);
    }

    public static void assertExecutionsDone(CyclicBarrier barrier) {
        try {
            barrier.await(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMembershipListenerSequentialInvocation() throws Exception {

        final int nodeCount = 10;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        final CountDownLatch eventLatch = new CountDownLatch(nodeCount - 1);
        final CountDownLatch nodeLatch = new CountDownLatch(nodeCount - 1);

        Config config = new Config()
                .addListenerConfig(new ListenerConfig().setImplementation(newAddMemberListener(eventLatch)));


        // first node has listener
        factory.newHazelcastInstance(config);

        for (int i = 1; i < nodeCount; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    factory.newHazelcastInstance(new Config());
                    nodeLatch.countDown();
                }
            });
        }

        assertOpenEventually(nodeLatch);
        assertOpenEventually(eventLatch);
    }

    private static MembershipAdapter newAddMemberListener(final CountDownLatch eventLatch) {
        return new MembershipAdapter() {

            // flag to check listener is not called concurrently
            final AtomicBoolean flag = new AtomicBoolean(false);

            public void memberAdded(MembershipEvent membershipEvent) {
                if (flag.compareAndSet(false, true)) {
                    sleepMillis((int) (Math.random() * 500) + 50);
                    eventLatch.countDown();
                    flag.set(false);
                }
            }
        };
    }

    @Test
    public void testInitialMembershipListener() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();

        InitialMembershipListenerImpl listener = new InitialMembershipListenerImpl();
        hz1.getCluster().addMembershipListener(listener);

        assertEventuallySizeAtLeast(listener.events, 1);
        assertInitialMembershipEvent(listener.events.get(0), hz1.getCluster().getLocalMember());

        HazelcastInstance hz2 = factory.newHazelcastInstance();

        assertEventuallySizeAtLeast(listener.events, 2);
        assertMembershipAddedEvent(listener.events.get(1), hz2.getCluster().getLocalMember(),
                hz1.getCluster().getLocalMember(), hz2.getCluster().getLocalMember());

        Member member2 = hz2.getCluster().getLocalMember();
        hz2.shutdown();

        assertEventuallySizeAtLeast(listener.events, 3);
        assertMembershipRemovedEvent(listener.events.get(2), member2, hz1.getCluster().getLocalMember());
    }

    @Test
    public void testInitialMembershipListenerRegistrationWithMultipleInitialMembers() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        InitialMembershipListenerImpl listener = new InitialMembershipListenerImpl();
        hz1.getCluster().addMembershipListener(listener);

        assertEventuallySizeAtLeast(listener.events, 1);
        assertInitialMembershipEvent(listener.events.get(0), hz1.getCluster().getLocalMember(), hz2.getCluster().getLocalMember());
    }

    public void assertInitialMembershipEvent(EventObject e, Member... expectedMembers) {
        assertTrue(e instanceof InitialMembershipEvent);

        InitialMembershipEvent initialMembershipEvent = (InitialMembershipEvent) e;
        Set<Member> foundMembers = initialMembershipEvent.getMembers();
        assertEquals(new HashSet<Member>(Arrays.asList(expectedMembers)), foundMembers);
    }

    public void assertMembershipAddedEvent(EventObject e, Member addedMember, Member... expectedMembers) {
        assertMembershipEvent(e, MembershipEvent.MEMBER_ADDED, addedMember, expectedMembers);
    }

    public void assertMembershipRemovedEvent(EventObject e, Member addedMember, Member... expectedMembers) {
        assertMembershipEvent(e, MembershipEvent.MEMBER_REMOVED, addedMember, expectedMembers);
    }

    public void assertMembershipEvent(EventObject e, int type, Member changedMember, Member... expectedMembers) {
        assertTrue(e instanceof MembershipEvent);

        MembershipEvent membershipEvent = (MembershipEvent) e;
        Set<Member> foundMembers = membershipEvent.getMembers();
        assertEquals(type, membershipEvent.getEventType());
        assertEquals(changedMember, membershipEvent.getMember());
        assertEquals(new HashSet<Member>(Arrays.asList(expectedMembers)), foundMembers);
    }

    public void assertEventuallySizeAtLeast(List list, int expectedSize) {
        long startTimeMs = System.currentTimeMillis();
        for (; ; ) {
            if (list.size() >= expectedSize) {
                return;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (System.currentTimeMillis() - startTimeMs > TimeUnit.SECONDS.toMillis(10)) {
                fail("Timeout, size of the list didn't reach size: " + expectedSize + " in time");
            }
        }
    }

    private static class MembershipListenerImpl implements MembershipListener {
        private List<EventObject> events = Collections.synchronizedList(new LinkedList<EventObject>());

        public void memberAdded(MembershipEvent e) {
            events.add(e);
        }

        public void memberRemoved(MembershipEvent e) {
            events.add(e);
        }

        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        }
    }

    private static class InitialMembershipListenerImpl implements InitialMembershipListener {

        private List<EventObject> events = Collections.synchronizedList(new LinkedList<EventObject>());

        public void init(InitialMembershipEvent e) {
            events.add(e);
        }

        public void memberAdded(MembershipEvent e) {
            events.add(e);
        }

        public void memberRemoved(MembershipEvent e) {
            events.add(e);
        }

        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        }

        public void assertEventCount(int expected) {
            assertEquals(expected, events.size());
        }
    }
}
