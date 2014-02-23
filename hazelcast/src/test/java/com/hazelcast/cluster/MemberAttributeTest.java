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
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MemberAttributeTest extends HazelcastTestSupport {

    @Test(timeout = 120000)
    public void testConfigAttributes() throws Exception {
        Config c = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        MemberAttributeConfig memberAttributeConfig = c.getMemberAttributeConfig();
        memberAttributeConfig.setIntAttribute("Test", 123);

        HazelcastInstance h1 = factory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        assertEquals(123, (int) m1.getIntAttribute("Test"));

        HazelcastInstance h2 = factory.newHazelcastInstance(c);
        Member m2 = h2.getCluster().getLocalMember();
        assertEquals(123, (int) m2.getIntAttribute("Test"));

        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m == h2.getCluster().getLocalMember())
                continue;
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        for (Member m : h1.getCluster().getMembers()) {
            if (m == h1.getCluster().getLocalMember())
                continue;
            member = m;
        }

        assertNotNull(member);
        assertEquals(m2, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void testPresharedAttributes() throws Exception {
        Config c = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = factory.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m == h2.getCluster().getLocalMember())
                continue;
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void testAddAttributes() throws Exception {
        Config c = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = factory.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m == h2.getCluster().getLocalMember())
                continue;
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        final CountDownLatch latch = new CountDownLatch(2);
        final MembershipListener listener = new LatchMembershipListener(latch);
        h2.getCluster().addMembershipListener(listener);
        h1.getCluster().addMembershipListener(listener);

        m1.setIntAttribute("Test2", 321);

        // Force sleep to distribute value
        latch.await(2, TimeUnit.SECONDS);

        assertNotNull(member.getIntAttribute("Test2"));
        assertEquals(321, (int) member.getIntAttribute("Test2"));

        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void testChangeAttributes() throws Exception {
        Config c = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = factory.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m == h2.getCluster().getLocalMember())
                continue;
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        final CountDownLatch latch = new CountDownLatch(2);
        final MembershipListener listener = new LatchMembershipListener(latch);
        h2.getCluster().addMembershipListener(listener);
        h1.getCluster().addMembershipListener(listener);

        m1.setIntAttribute("Test", 321);

        // Force sleep to distribute value
        latch.await(2, TimeUnit.SECONDS);

        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(321, (int) member.getIntAttribute("Test"));

        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void testRemoveAttributes() throws Exception {
        Config c = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test", 123);

        HazelcastInstance h2 = factory.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m == h2.getCluster().getLocalMember())
                continue;
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getIntAttribute("Test"));
        assertEquals(123, (int) member.getIntAttribute("Test"));

        final CountDownLatch latch = new CountDownLatch(2);
        final MembershipListener listener = new LatchMembershipListener(latch);
        h2.getCluster().addMembershipListener(listener);
        h1.getCluster().addMembershipListener(listener);

        m1.removeAttribute("Test");

        // Force sleep to distribute value
        latch.await(2, TimeUnit.SECONDS);

        assertNull(member.getIntAttribute("Test"));

        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void testCommandLineAttributes() throws Exception {
        System.setProperty("hazelcast.member.attribute.Test-2", "1234");
        System.setProperty("hazelcast.member.attribute.Test-3", "12345");
        System.setProperty("hazelcast.member.attribute.Test-4", "123456");

        Config c = new Config();
        c.getMemberAttributeConfig().setIntAttribute("Test-1", 123);
        c.getMemberAttributeConfig().setIntAttribute("Test-2", 123);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);

        HazelcastInstance h1 = factory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setIntAttribute("Test-4", 1234567);

        assertEquals(123, (int) m1.getIntAttribute("Test-1"));
        assertEquals("1234", m1.getStringAttribute("Test-2"));
        assertEquals("12345", m1.getStringAttribute("Test-3"));
        assertEquals(1234567, (int) m1.getIntAttribute("Test-4"));

        h1.getLifecycleService().shutdown();
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