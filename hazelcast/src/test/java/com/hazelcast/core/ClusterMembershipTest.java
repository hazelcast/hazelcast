package com.hazelcast.core;

import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ClusterMembershipTest {

    @Before
    public void before() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMembershipListener()  {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        MembershipListenerImpl listener = new MembershipListenerImpl();
        hz1.getCluster().addMembershipListener(listener);

        //start a second instance
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        assertEventuallySizeAtLeast(listener.events, 1);
        assertMembershipAddedEvent(listener.events.get(0), hz2.getCluster().getLocalMember(), hz1.getCluster().getLocalMember(), hz2.getCluster().getLocalMember());

        //terminate the second instance
        Member member2 = hz2.getCluster().getLocalMember();
        hz2.getLifecycleService().shutdown();

        assertEventuallySizeAtLeast(listener.events, 2);
        assertMembershipRemovedEvent(listener.events.get(1), member2, hz1.getCluster().getLocalMember());
    }

    @Test
    public void testInitialMembershipListener() {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();

        InitialMembershipListenerImpl listener = new InitialMembershipListenerImpl();
        hz1.getCluster().addMembershipListener(listener);

        assertEventuallySizeAtLeast(listener.events, 1);
        assertInitialMembershopEvent(listener.events.get(0), hz1.getCluster().getLocalMember());

        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        assertEventuallySizeAtLeast(listener.events, 2);
        assertMembershipAddedEvent(listener.events.get(1), hz2.getCluster().getLocalMember(), hz1.getCluster().getLocalMember(), hz2.getCluster().getLocalMember());

        Member member2 = hz2.getCluster().getLocalMember();
        hz2.getLifecycleService().shutdown();

        assertEventuallySizeAtLeast(listener.events, 3);
        assertMembershipRemovedEvent(listener.events.get(2), member2, hz1.getCluster().getLocalMember());
    }

    @Test
    public void testInitialMembershipListenerRegistrationWithMultipleInitialMembers(){
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        InitialMembershipListenerImpl listener = new InitialMembershipListenerImpl();
        hz1.getCluster().addMembershipListener(listener);

        assertEventuallySizeAtLeast(listener.events, 1);
        assertInitialMembershopEvent(listener.events.get(0), hz1.getCluster().getLocalMember(), hz2.getCluster().getLocalMember());
    }

    //if the listener is already registered, is should not receive an additional InitialMembershipEvent
    @Test
    public void testDuplicateRegistrationOfInitialMembershipListener() throws InterruptedException {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();

        InitialMembershipListenerImpl listener = new InitialMembershipListenerImpl();
        hz1.getCluster().addMembershipListener(listener);
        hz1.getCluster().addMembershipListener(listener);

        //we need to verify that there are no additional InitialMembershipEvents send.
        Thread.sleep(1000);
        assertEquals(1, listener.events.size());
        assertInitialMembershopEvent(listener.events.get(0), hz1.getCluster().getLocalMember());
    }

    public void assertInitialMembershopEvent(EventObject e, Member... expectedMembers) {
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
