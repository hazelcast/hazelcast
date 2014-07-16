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
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MembershipListenerTest extends HazelcastTestSupport {

    private HazelcastInstance server1;
    private HazelcastInstance server2;

    private HazelcastInstance client;

    private MemberShipEventLogger listener;

    @Before
    public void setup() {
        server1 = Hazelcast.newHazelcastInstance();

        client = HazelcastClient.newHazelcastClient();

        listener = new MemberShipEventLogger();
    }

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    @Category(ProblematicTest.class)
    public void whenMemberAdded_thenMemberAddedEvent() throws Exception {
        client.getCluster().addMembershipListener(listener);

        // Start a second server and verify that the listener receives it
        server2 = Hazelcast.newHazelcastInstance();

        // Verify that the listener receives member added event
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
        // Start a second server and verify that the listener receives it
        server2 = Hazelcast.newHazelcastInstance();

        client.getCluster().addMembershipListener(listener);

        final Member memberServer2 = server2.getCluster().getLocalMember();
        server2.shutdown();

        // Verify that the correct member removed event was received
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.events.size());
                MembershipEvent event = listener.events.get(0);
                assertEquals(MembershipEvent.MEMBER_REMOVED, event.getEventType());
                assertEquals(memberServer2, event.getMember());
                assertEquals(getMembers(server1), event.getMembers());
            }
        });
    }

    @Test
    public void removedPhantomListener_thenFalse() throws Exception {
        assertFalse(client.getCluster().removeMembershipListener("_NOT_EXISTS_"));
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
        private final List<MembershipEvent> events = new Vector<MembershipEvent>();

        public void memberAdded(MembershipEvent event) {
            events.add(event);
        }

        public void memberRemoved(MembershipEvent event) {
            events.add(event);
        }

        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        }
    }
}