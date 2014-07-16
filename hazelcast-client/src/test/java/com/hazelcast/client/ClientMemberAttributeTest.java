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

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMemberAttributeTest extends HazelcastTestSupport {

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 40000)
    public void testChangeMemberAttributes() throws Exception {
        int count = 100;

        HazelcastInstance server = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        ListenerConfig listenerConfig = new ListenerConfig();
        CountDownLatch countDownLatch = new CountDownLatch(count);
        listenerConfig.setImplementation(new LatchMembershipListener(countDownLatch));
        clientConfig.addListenerConfig(listenerConfig);
        HazelcastClient.newHazelcastClient(clientConfig);

        Member localMember = server.getCluster().getLocalMember();
        for (int i = 0; i < count; i++) {
            localMember.setStringAttribute("key" + i, HazelcastTestSupport.randomString());
        }

        assertOpenEventually(countDownLatch);
    }

    @Test(timeout = 120000)
    public void testConfigAttributes() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        MemberAttributeConfig memberAttributeConfig = config.getMemberAttributeConfig();
        memberAttributeConfig.setIntAttribute("Test", 123);

        HazelcastInstance server1 = Hazelcast.newHazelcastInstance(config);
        Member member1 = server1.getCluster().getLocalMember();
        assertEquals(123, (int) member1.getIntAttribute("Test"));

        HazelcastInstance server2 = Hazelcast.newHazelcastInstance(config);
        Member member2 = server2.getCluster().getLocalMember();
        assertEquals(123, (int) member2.getIntAttribute("Test"));

        assertEquals(2, server2.getCluster().getMembers().size());

        Member actualMember = null;
        for (Member member : server2.getCluster().getMembers()) {
            if (member == server2.getCluster().getLocalMember()) {
                continue;
            }
            actualMember = member;
        }

        assertNotNull(actualMember);
        assertEquals(member1, actualMember);
        assertNotNull(actualMember.getIntAttribute("Test"));
        assertEquals(123, (int) actualMember.getIntAttribute("Test"));

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        Collection<Member> members = client.getCluster().getMembers();
        for (Member member : members) {
            assertEquals(123, (int) member.getIntAttribute("Test"));
        }

        client.shutdown();
        server1.shutdown();
        server2.shutdown();
    }

    @Test(timeout = 120000)
    public void testPresharedAttributes() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        createHazelcastInstanceFactory(2);

        HazelcastInstance server1 = Hazelcast.newHazelcastInstance(config);
        Member member1 = server1.getCluster().getLocalMember();
        member1.setIntAttribute("Test", 123);

        HazelcastInstance server2 = Hazelcast.newHazelcastInstance(config);
        assertEquals(2, server2.getCluster().getMembers().size());

        Member actualMember = null;
        for (Member member : server2.getCluster().getMembers()) {
            if (member == server2.getCluster().getLocalMember())
                continue;
            actualMember = member;
        }

        assertNotNull(actualMember);
        assertEquals(member1, actualMember);
        assertNotNull(actualMember.getIntAttribute("Test"));
        assertEquals(123, (int) actualMember.getIntAttribute("Test"));

        boolean found = false;
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        Collection<Member> members = client.getCluster().getMembers();
        for (Member member : members) {
            if (member.equals(member1)) {
                assertEquals(123, (int) member.getIntAttribute("Test"));
                found = true;
            }
        }

        assertTrue(found);

        client.shutdown();
        server1.shutdown();
        server2.shutdown();
    }

    @Test(timeout = 120000)
    public void testAddAttributes() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);

        HazelcastInstance server1 = Hazelcast.newHazelcastInstance(config);
        Member member1 = server1.getCluster().getLocalMember();
        member1.setIntAttribute("Test", 123);

        HazelcastInstance server2 = Hazelcast.newHazelcastInstance(config);
        assertEquals(2, server2.getCluster().getMembers().size());

        Member actualMember = null;
        for (Member member : server2.getCluster().getMembers()) {
            if (member == server2.getCluster().getLocalMember())
                continue;
            actualMember = member;
        }

        assertNotNull(actualMember);
        assertEquals(member1, actualMember);
        assertNotNull(actualMember.getIntAttribute("Test"));
        assertEquals(123, (int) actualMember.getIntAttribute("Test"));

        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        CountDownLatch latch = new CountDownLatch(3);
        MembershipListener listener = new LatchMembershipListener(latch);
        server2.getCluster().addMembershipListener(listener);
        server1.getCluster().addMembershipListener(listener);
        client.getCluster().addMembershipListener(listener);

        member1.setIntAttribute("Test2", 321);

        // Force sleep to distribute value
        assertOpenEventually(latch);

        assertNotNull(actualMember.getIntAttribute("Test2"));
        assertEquals(321, (int) actualMember.getIntAttribute("Test2"));

        boolean found = false;
        Collection<Member> members = client.getCluster().getMembers();
        for (Member member : members) {
            if (member.equals(member1)) {
                assertEquals(321, (int) member.getIntAttribute("Test2"));
                found = true;
            }
        }

        assertTrue(found);

        client.shutdown();
        server1.shutdown();
        server2.shutdown();
    }

    @Test(timeout = 120000)
    public void testChangeAttributes() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);

        HazelcastInstance server1 = Hazelcast.newHazelcastInstance(config);
        Member member1 = server1.getCluster().getLocalMember();
        member1.setIntAttribute("Test", 123);

        HazelcastInstance server2 = Hazelcast.newHazelcastInstance(config);
        assertEquals(2, server2.getCluster().getMembers().size());

        Member actualMember = null;
        for (Member member : server2.getCluster().getMembers()) {
            if (member == server2.getCluster().getLocalMember())
                continue;
            actualMember = member;
        }

        assertNotNull(actualMember);
        assertEquals(member1, actualMember);
        assertNotNull(actualMember.getIntAttribute("Test"));
        assertEquals(123, (int) actualMember.getIntAttribute("Test"));

        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        CountDownLatch latch = new CountDownLatch(3);
        MembershipListener listener = new LatchMembershipListener(latch);
        server2.getCluster().addMembershipListener(listener);
        server1.getCluster().addMembershipListener(listener);
        client.getCluster().addMembershipListener(listener);

        member1.setIntAttribute("Test", 321);

        // Force sleep to distribute value
        assertOpenEventually(latch);

        assertNotNull(actualMember.getIntAttribute("Test"));
        assertEquals(321, (int) actualMember.getIntAttribute("Test"));

        boolean found = false;
        Collection<Member> members = client.getCluster().getMembers();
        for (Member member : members) {
            if (member.equals(member1)) {
                assertEquals(321, (int) member.getIntAttribute("Test"));
                found = true;
            }
        }

        assertTrue(found);

        client.getLifecycleService().shutdown();
        server1.shutdown();
        server2.shutdown();
    }

    @Test(timeout = 120000)
    public void testRemoveAttributes() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);

        HazelcastInstance server1 = Hazelcast.newHazelcastInstance(config);
        Member member1 = server1.getCluster().getLocalMember();
        member1.setIntAttribute("Test", 123);

        HazelcastInstance server2 = Hazelcast.newHazelcastInstance(config);
        assertEquals(2, server2.getCluster().getMembers().size());

        Member actualMember = null;
        for (Member member : server2.getCluster().getMembers()) {
            if (member == server2.getCluster().getLocalMember())
                continue;
            actualMember = member;
        }

        assertNotNull(actualMember);
        assertEquals(member1, actualMember);
        assertNotNull(actualMember.getIntAttribute("Test"));
        assertEquals(123, (int) actualMember.getIntAttribute("Test"));

        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        CountDownLatch latch = new CountDownLatch(3);
        MembershipListener listener = new LatchMembershipListener(latch);
        server2.getCluster().addMembershipListener(listener);
        server1.getCluster().addMembershipListener(listener);
        client.getCluster().addMembershipListener(listener);

        member1.removeAttribute("Test");

        // Force sleep to distribute value
        assertOpenEventually(latch);

        assertNull(actualMember.getIntAttribute("Test"));

        boolean found = false;
        Collection<Member> members = client.getCluster().getMembers();
        for (Member member : members) {
            if (member.equals(member1)) {
                assertNull(member.getIntAttribute("Test"));
                found = true;
            }
        }

        assertTrue(found);

        client.shutdown();
        server1.shutdown();
        server2.shutdown();
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