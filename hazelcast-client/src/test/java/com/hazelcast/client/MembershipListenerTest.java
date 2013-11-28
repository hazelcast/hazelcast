package com.hazelcast.client;

/**
 * User: danny Date: 11/28/13
 */


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MembershipListenerTest extends HazelcastTestSupport {

    @After
    public void tearDown(){
        Hazelcast.shutdownAll();
    }

    @Test
    public void whenMemberAdded_thenMemberAddedEvent() throws Exception {

        final HazelcastInstance server1 =Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final MemberShipEventLoger listener = new MemberShipEventLoger();
        client.getCluster().addMembershipListener(listener);

        //start a second server and verify that the listener receives it.
        final HazelcastInstance server2 = Hazelcast.newHazelcastInstance();

        //verify that the listener receives member added event.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.events.size());
                MembershipEvent event = listener.events.get(0);
                assertEquals(MembershipEvent.MEMBER_ADDED, event.getEventType());
                assertEquals(server2.getCluster().getLocalMember(), event.getMember());
                assertEquals(getMembers(server1, server2), event.getMembers());
            }
        });
    }

    @Test
    public void whenMemberRemoved_thenMemberRemovedEvent() throws Exception {

        final HazelcastInstance server1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        //start a second server and verify that the listener receives it.
        final HazelcastInstance server2 = Hazelcast.newHazelcastInstance();


        final MemberShipEventLoger listener = new MemberShipEventLoger();
        client.getCluster().addMembershipListener(listener);

        final Member server2Member = server2.getCluster().getLocalMember();
        server2.shutdown();

        //verify that the correct member removed event was received.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.events.size());
                MembershipEvent event = listener.events.get(0);
                assertEquals(MembershipEvent.MEMBER_REMOVED, event.getEventType());
                assertEquals(server2Member, event.getMember());
                assertEquals(getMembers(server1), event.getMembers());
            }
        });
    }

    @Test
    public void whenListenerRemoved_thenNoEventsRecived() throws Exception {

        final HazelcastInstance server1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final MemberShipEventLoger listener = new MemberShipEventLoger();

        final String regID = client.getCluster().addMembershipListener(listener);

        //start a second server and verify that the listener receives it.
        final HazelcastInstance server2 = Hazelcast.newHazelcastInstance();

        //verify that the correct member removed event was received.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.events.size());
                MembershipEvent event = listener.events.get(0);
                assertEquals(MembershipEvent.MEMBER_ADDED, event.getEventType());
                assertEquals(server2.getCluster().getLocalMember(), event.getMember());
                assertEquals(getMembers(server1,server2), event.getMembers());
            }
        });

        client.getCluster().removeMembershipListener(regID);

        final Member server2Member = server2.getCluster().getLocalMember();
        server2.shutdown();

        //sleep for 5  and assert No new messages
        //This negative test cannot prove that no event was recived after delay
        assertTrueLater(5, new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.events.size());
                MembershipEvent event = listener.events.get(0);
                assertEquals(MembershipEvent.MEMBER_ADDED, event.getEventType());
            }
        });
    }


    private Set<Member> getMembers(HazelcastInstance... instances) {
        Set<Member> result = new HashSet<Member>();
        for (HazelcastInstance hz : instances) {
            result.add(hz.getCluster().getLocalMember());
        }
        return result;
    }

    public abstract class AssertTask {

        public abstract void run();
    }

    public static void assertTrueEventually(AssertTask task) {
        AssertionError error = null;
        for (int k = 0; k < 120; k++) {
            try {
                task.run();
                return;
            } catch (AssertionError e) {
                error = e;
            }
            sleepSeconds(1);
        }

        throw error;
    }

    public static void assertTrueLater(int delaySeconds, AssertTask task) {
        sleepSeconds(delaySeconds);
        task.run();
    }

    private static void sleepSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }


    public class MemberShipEventLoger implements MembershipListener {

        private List<MembershipEvent> events = new Vector<MembershipEvent>();

        public void memberAdded(MembershipEvent event) {
            events.add(event);
        }

        public void memberRemoved(MembershipEvent event) {
            events.add(event);
        }
    }
}