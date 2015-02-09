package com.hazelcast.client;

/**
 * User: danny Date: 11/28/13
 */

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MembershipListenerTest extends HazelcastTestSupport {

    private HazelcastInstance server1 = null;
    private HazelcastInstance client = null;

    @Before
    public void setup() {
        server1 = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();

    }

    @After
    public void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void whenMemberAdded_thenMemberAddedEvent() throws Exception {
        final MemberShipEventLogger listener = new MemberShipEventLogger();

        client.getCluster().addMembershipListener(listener);

        //start a second server and verify that the listener receives it.
        final HazelcastInstance server2 = Hazelcast.newHazelcastInstance();

        //verify that the listener receives member added event.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotEquals("Expecting one or more events", 0, listener.events.size());
                MembershipEvent event = listener.events.getLast();
                assertEquals("Last event should be member added", MembershipEvent.MEMBER_ADDED, event.getEventType());
                assertEquals(server2.getCluster().getLocalMember(), event.getMember());
                assertEquals(getMembers(server1, server2), event.getMembers());
            }
        });
    }

    @Test
    public void whenMemberRemoved_thenMemberRemovedEvent() throws Exception {
        final MemberShipEventLogger listener = new MemberShipEventLogger();

        //start a second server and verify that the listener receives it.
        final HazelcastInstance server2 = Hazelcast.newHazelcastInstance();

        client.getCluster().addMembershipListener(listener);

        final Member server2Member = server2.getCluster().getLocalMember();
        server2.shutdown();

        //verify that the correct member removed event was received.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotEquals("Expecting one or more events", 0, listener.events.size());
                MembershipEvent event = listener.events.getLast();
                assertEquals("Last event should be member removed", MembershipEvent.MEMBER_REMOVED, event.getEventType());
                assertEquals(server2Member, event.getMember());
                assertEquals(getMembers(server1), event.getMembers());
            }
        });
    }

    @Test
    public void removedPhantomListener_thenFalse() throws Exception {
        assertFalse(client.getCluster().removeMembershipListener(randomString()));
    }

    @Test(expected = NullPointerException.class)
    public void removedNullListener_thenException() throws Exception {

        assertFalse(client.getCluster().removeMembershipListener(null));
    }


    @Test(expected = java.lang.NullPointerException.class)
    public void addNullListener_thenException() throws Exception {

        client.getCluster().addMembershipListener(null);
    }

    private Set<Member> getMembers(HazelcastInstance... instances) {
        Set<Member> result = new HashSet<Member>();
        for (HazelcastInstance hz : instances) {
            result.add(hz.getCluster().getLocalMember());
        }
        return result;
    }


    private class MemberShipEventLogger implements MembershipListener {

        public LinkedBlockingDeque<MembershipEvent> events = new LinkedBlockingDeque<MembershipEvent>();

        public void memberAdded(MembershipEvent event) {
            events.addLast(event);
        }

        public void memberRemoved(MembershipEvent event) {
            events.addLast(event);
        }

        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        }
    }
}