package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class ClusterMembershipTest extends HazelcastTestSupport {

    @Test
    public void testMembershipListener() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        MembershipListenerImpl listener = new MembershipListenerImpl();
        hz1.getCluster().addMembershipListener(listener);

        //start a second instance
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());

        assertEventuallySizeAtLeast(listener.events, 1);
        assertMembershipAddedEvent(listener.events.get(0), hz2.getCluster().getLocalMember(), hz1.getCluster().getLocalMember(), hz2.getCluster().getLocalMember());

        //terminate the second instance
        Member member2 = hz2.getCluster().getLocalMember();
        hz2.getLifecycleService().shutdown();

        assertEventuallySizeAtLeast(listener.events, 2);
        assertMembershipRemovedEvent(listener.events.get(1), member2, hz1.getCluster().getLocalMember());
    }

    @Test
    public void testMembershipListenerSequentialInvocation() throws InterruptedException {
        final Config config = new Config();
        final int nodeCount = 10;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        final CountDownLatch eventLatch = new CountDownLatch(nodeCount - 1);
        final CountDownLatch nodeLatch = new CountDownLatch(nodeCount);
        config.addListenerConfig(new ListenerConfig().setImplementation(new MembershipListener() {

            final AtomicBoolean flag = new AtomicBoolean(false);

            public void memberAdded(MembershipEvent membershipEvent) {
                if (flag.compareAndSet(false, true)) {
                    try {
                        Thread.sleep((long) (Math.random() * 500) + 50);
                        eventLatch.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        flag.set(false);
                    }
                }
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
            }
        }));

        final ExecutorService ex = Executors.newFixedThreadPool(nodeCount / 2);
        for (int i = 0; i < nodeCount; i++) {
            ex.execute(new Runnable() {
                public void run() {
                    factory.newHazelcastInstance(config);
                    nodeLatch.countDown();
                }
            });
        }

        try {
            assertTrue(nodeLatch.await(30, TimeUnit.SECONDS));
            assertTrue(eventLatch.await(30, TimeUnit.SECONDS));
        } finally {
            ex.shutdownNow();
        }
    }

    @Test
    public void testInitialMembershipListener() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());

        InitialMembershipListenerImpl listener = new InitialMembershipListenerImpl();
        hz1.getCluster().addMembershipListener(listener);

        assertEventuallySizeAtLeast(listener.events, 1);
        assertInitialMembershipEvent(listener.events.get(0), hz1.getCluster().getLocalMember());

        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());

        assertEventuallySizeAtLeast(listener.events, 2);
        assertMembershipAddedEvent(listener.events.get(1), hz2.getCluster().getLocalMember(), hz1.getCluster().getLocalMember(), hz2.getCluster().getLocalMember());

        Member member2 = hz2.getCluster().getLocalMember();
        hz2.getLifecycleService().shutdown();

        assertEventuallySizeAtLeast(listener.events, 3);
        assertMembershipRemovedEvent(listener.events.get(2), member2, hz1.getCluster().getLocalMember());
    }

    @Test
    public void testInitialMembershipListenerRegistrationWithMultipleInitialMembers() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());

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

        public void assertEventCount(int expected) {
            assertEquals(expected, events.size());
        }
    }
}
