/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberAttributeTest extends HazelcastTestSupport {

    @Test(timeout = 120000)
    public void testConfigAttributes() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        MemberAttributeConfig memberAttributeConfig = config.getMemberAttributeConfig();
        memberAttributeConfig.setAttribute("Test", "123");

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        Member m1 = h1.getCluster().getLocalMember();
        assertEquals("123", m1.getAttribute("Test"));

        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        Member m2 = h2.getCluster().getLocalMember();
        assertEquals("123", m2.getAttribute("Test"));

        assertClusterSize(2, h1, h2);

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m.equals(h2.getCluster().getLocalMember())) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getAttribute("Test"));
        assertEquals("123", member.getAttribute("Test"));

        for (Member m : h1.getCluster().getMembers()) {
            if (m == h1.getCluster().getLocalMember()) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m2, member);
        assertNotNull(member.getAttribute("Test"));
        assertEquals("123", member.getAttribute("Test"));

        h1.shutdown();
        h2.shutdown();
    }

    @Test(timeout = 120000)
    public void testPresharedAttributes() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance();
        Member m1 = h1.getCluster().getLocalMember();
        m1.setAttribute("Test", "123");

        HazelcastInstance h2 = factory.newHazelcastInstance();
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
        assertNotNull(member.getAttribute("Test"));
        assertEquals("123", member.getAttribute("Test"));

        h1.shutdown();
        h2.shutdown();
    }

    @Test(timeout = 120000)
    public void testAddAttributes() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance();
        Member m1 = h1.getCluster().getLocalMember();
        m1.setAttribute("Test", "123");

        HazelcastInstance h2 = factory.newHazelcastInstance();
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
        assertNotNull(member.getAttribute("Test"));
        assertEquals("123", member.getAttribute("Test"));

        final CountDownLatch latch = new CountDownLatch(2);
        final MembershipListener listener = new LatchMembershipListener(latch);
        h2.getCluster().addMembershipListener(listener);
        h1.getCluster().addMembershipListener(listener);

        m1.setAttribute("Test2", "321");

        // Force sleep to distribute value
        assertOpenEventually(latch);

        assertNotNull(member.getAttribute("Test2"));
        assertEquals("321", member.getAttribute("Test2"));

        h1.shutdown();
        h2.shutdown();
    }

    @Test(timeout = 120000)
    public void testChangeAttributes() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance();
        Member m1 = h1.getCluster().getLocalMember();
        m1.setAttribute("Test", "123");

        HazelcastInstance h2 = factory.newHazelcastInstance();
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
        assertNotNull(member.getAttribute("Test"));
        assertEquals("123", member.getAttribute("Test"));

        final CountDownLatch latch = new CountDownLatch(2);
        final MembershipListener listener = new LatchMembershipListener(latch);
        h2.getCluster().addMembershipListener(listener);
        h1.getCluster().addMembershipListener(listener);

        m1.setAttribute("Test", "321");

        // Force sleep to distribute value
        assertOpenEventually(latch);

        assertNotNull(member.getAttribute("Test"));
        assertEquals("321", member.getAttribute("Test"));

        h1.shutdown();
        h2.shutdown();
    }

    @Test(timeout = 120000)
    public void testRemoveAttributes() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance();
        Member m1 = h1.getCluster().getLocalMember();
        m1.setAttribute("Test", "123");

        HazelcastInstance h2 = factory.newHazelcastInstance();
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
        assertNotNull(member.getAttribute("Test"));
        assertEquals("123", member.getAttribute("Test"));

        final CountDownLatch latch = new CountDownLatch(2);
        final MembershipListener listener = new LatchMembershipListener(latch);
        h2.getCluster().addMembershipListener(listener);
        h1.getCluster().addMembershipListener(listener);

        m1.removeAttribute("Test");

        // Force sleep to distribute value
        assertOpenEventually(latch);

        assertNull(member.getAttribute("Test"));

        h1.shutdown();
        h2.shutdown();
    }

    @Test(timeout = 120000)
    public void testCommandLineAttributes() throws Exception {
        System.setProperty("hazelcast.member.attribute.Test-2", "1234");
        System.setProperty("hazelcast.member.attribute.Test-3", "12345");
        System.setProperty("hazelcast.member.attribute.Test-4", "123456");

        Config config = new Config();
        config.getMemberAttributeConfig().setAttribute("Test-1", "123");
        config.getMemberAttributeConfig().setAttribute("Test-2", "123");

        HazelcastInstance h1 = createHazelcastInstance(config);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setAttribute("Test-4", "1234567");

        assertEquals("123", m1.getAttribute("Test-1"));
        assertEquals("1234", m1.getAttribute("Test-2"));
        assertEquals("12345", m1.getAttribute("Test-3"));
        assertEquals("1234567", m1.getAttribute("Test-4"));

        h1.shutdown();
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
