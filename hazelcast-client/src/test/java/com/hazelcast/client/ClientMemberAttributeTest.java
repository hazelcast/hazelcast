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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMemberAttributeTest extends HazelcastTestSupport {

    @After
    @Before
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testChangeMemberAttributes() throws Exception {
        final int count = 10;
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final ClientConfig config = new ClientConfig();
        final ListenerConfig listenerConfig = new ListenerConfig();
        final CountDownLatch countDownLatch = new CountDownLatch(count);
        listenerConfig.setImplementation(new LatchMembershipListener(countDownLatch));
        config.addListenerConfig(listenerConfig);
        HazelcastClient.newHazelcastClient(config);

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

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        assertEquals(123, (int) m1.getIntAttribute("Test"));

        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        Member m2 = h2.getCluster().getLocalMember();
        assertEquals(123, (int) m2.getIntAttribute("Test"));

        assertEquals(2, h2.getCluster().getMembers().size());

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m == h2.getCluster().getLocalMember()) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        Collection<Member> members = client.getCluster().getMembers();
        for (Member m : members) {
            assertEquals(123, (int) m.getIntAttribute("Test"));
        }

        client.shutdown();
        h1.shutdown();
        h2.shutdown();
    }

    @Test(timeout = 120000)
    public void testPresharedAttributes() throws Exception {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m == h2.getCluster().getLocalMember()) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        boolean found = false;
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        Collection<Member> members = client.getCluster().getMembers();
        for (Member m : members) {
            if (m.equals(m1)) {
                assertEquals(123, (int) m.getIntAttribute("Test"));
                found = true;
            }
        }

        assertTrue(found);

        client.shutdown();
        h1.shutdown();
        h2.shutdown();
    }

    @Test(timeout = 120000)
    public void testAddAttributes() throws Exception {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m == h2.getCluster().getLocalMember()) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        HazelcastInstance client = HazelcastClient.newHazelcastClient();

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

        client.shutdown();
        h1.shutdown();
        h2.shutdown();
    }

    @Test(timeout = 120000)
    public void testChangeAttributes() throws Exception {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m == h2.getCluster().getLocalMember()) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        HazelcastInstance client = HazelcastClient.newHazelcastClient();

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

        client.getLifecycleService().shutdown();
        h1.shutdown();
        h2.shutdown();
    }

    @Test(timeout = 120000)
    public void testRemoveAttributes() throws Exception {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m == h2.getCluster().getLocalMember()) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        HazelcastInstance client = HazelcastClient.newHazelcastClient();

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

        client.shutdown();
        h1.shutdown();
        h2.shutdown();
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