/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMemberAttributeTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testChangeMemberAttributes() throws Exception {
        final int count = 10;
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final ClientConfig config = new ClientConfig();
        final ListenerConfig listenerConfig = new ListenerConfig();
        final CountDownLatch countDownLatch = new CountDownLatch(count);
        listenerConfig.setImplementation(new LatchMembershipListener(countDownLatch));
        config.addListenerConfig(listenerConfig);
        hazelcastFactory.newHazelcastClient(config);

        final Member localMember = instance.getCluster().getLocalMember();
        for (int i = 0; i < count; i++) {
            localMember.setStringAttribute("key" + i, HazelcastTestSupport.randomString());
        }

        assertOpenEventually(countDownLatch);
    }

    @Test(timeout = 120000)
    public void testConfigAttributes() throws Exception {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);
        MemberAttributeConfig memberAttributeConfig = c.getMemberAttributeConfig();
        memberAttributeConfig.setIntAttribute("Test", 123);

        HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        assertEquals(123, (int) m1.getIntAttribute("Test"));

        HazelcastInstance h2 = hazelcastFactory.newHazelcastInstance(c);
        Member m2 = h2.getCluster().getLocalMember();
        assertEquals(123, (int) m2.getIntAttribute("Test"));

        assertClusterSize(2, h2);

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m.equals(h2.getCluster().getLocalMember())) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        Collection<Member> members = client.getCluster().getMembers();
        for (Member m : members) {
            assertEquals(123, (int) m.getIntAttribute("Test"));
        }
    }

    @Test(timeout = 120000)
    public void testPresharedAttributes() throws Exception {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);

        HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = hazelcastFactory.newHazelcastInstance(c);
        assertClusterSize(2, h2);

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m.equals(h2.getCluster().getLocalMember())) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        boolean found = false;
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        Collection<Member> members = client.getCluster().getMembers();
        for (Member m : members) {
            if (m.equals(m1)) {
                assertEquals(123, (int) m.getIntAttribute("Test"));
                found = true;
            }
        }

        assertTrue(found);
    }

    @Test(timeout = 120000)
    public void testAddAttributes() throws Exception {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);

        HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = hazelcastFactory.newHazelcastInstance(c);
        assertClusterSize(2, h2);

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m.equals(h2.getCluster().getLocalMember())) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final CountDownLatch latch = new CountDownLatch(3);
        final MembershipListener listener = new LatchMembershipListener(latch);
        h2.getCluster().addMembershipListener(listener);
        h1.getCluster().addMembershipListener(listener);
        client.getCluster().addMembershipListener(listener);

        m1.setIntAttribute("Test2", 321);

        // Force sleep to distribute value
        assertOpenEventually(latch);

        assertNotNull(member.getIntAttribute("Test2"));
        assertEquals(321, (int) member.getIntAttribute("Test2"));

        boolean found = false;
        Collection<Member> members = client.getCluster().getMembers();
        for (Member m : members) {
            if (m.equals(m1)) {
                assertEquals(321, (int) m.getIntAttribute("Test2"));
                found = true;
            }
        }

        assertTrue(found);
    }

    @Test(timeout = 120000)
    public void testChangeAttributes() throws Exception {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);

        HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = hazelcastFactory.newHazelcastInstance(c);
        assertClusterSize(2, h2);

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m.equals(h2.getCluster().getLocalMember())) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final CountDownLatch latch = new CountDownLatch(3);
        final MembershipListener listener = new LatchMembershipListener(latch);
        h2.getCluster().addMembershipListener(listener);
        h1.getCluster().addMembershipListener(listener);
        client.getCluster().addMembershipListener(listener);

        m1.setIntAttribute("Test", 321);

        // Force sleep to distribute value
        assertOpenEventually(latch);

        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(321, (int) member.getIntAttribute("Test"));

        boolean found = false;
        Collection<Member> members = client.getCluster().getMembers();
        for (Member m : members) {
            if (m.equals(m1)) {
                assertEquals(321, (int) m.getIntAttribute("Test"));
                found = true;
            }
        }

        assertTrue(found);
    }

    @Test(timeout = 120000)
    public void testRemoveAttributes() throws Exception {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);

        HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = hazelcastFactory.newHazelcastInstance(c);
        assertClusterSize(2, h2);

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m.equals(h2.getCluster().getLocalMember())) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final CountDownLatch latch = new CountDownLatch(3);
        final MembershipListener listener = new LatchMembershipListener(latch);
        h2.getCluster().addMembershipListener(listener);
        h1.getCluster().addMembershipListener(listener);
        client.getCluster().addMembershipListener(listener);

        m1.removeAttribute("Test");

        // Force sleep to distribute value
        assertOpenEventually(latch);

        assertNull(member.getIntAttribute("Test"));

        boolean found = false;
        Collection<Member> members = client.getCluster().getMembers();
        for (Member m : members) {
            if (m.equals(m1)) {
                assertNull(m.getIntAttribute("Test"));
                found = true;
            }
        }

        assertTrue(found);
    }

    private static class LatchMembershipListener implements MembershipListener {

        private final CountDownLatch latch;

        private LatchMembershipListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            latch.countDown();
        }
    }
}
