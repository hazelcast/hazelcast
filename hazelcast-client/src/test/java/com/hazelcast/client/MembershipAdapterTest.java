package com.hazelcast.client;
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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)


public class MembershipAdapterTest extends HazelcastTestSupport {


    private HazelcastInstance instance = null;
    private HazelcastInstance client = null;

    @Before
    public void setup() {
        instance = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMemberShipAdapterMemberAdded() {
        final CountDownLatch memberAddedLatch = new CountDownLatch(1);
        instance.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberAdded(final MembershipEvent membershipEvent) {
                memberAddedLatch.countDown();
            }
        });

        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        Set<Member> memberSet = instance2.getCluster().getMembers();
        assertEquals(2, memberSet.size());
        assertOpenEventually(memberAddedLatch);
    }

    @Test
    public void testMemberShipAdapterMemberRemoved() {
        final CountDownLatch memberRemovedLatch = new CountDownLatch(1);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        Set<Member> memberSet = instance.getCluster().getMembers();
        assertEquals(2, memberSet.size());
        client.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(final MembershipEvent membershipEvent) {
                memberRemovedLatch.countDown();
            }
        });

        instance.shutdown();

        assertOpenEventually(memberRemovedLatch);
    }

    @Test
    public void testMemberShipAdapterMemberAttributeChanged() {
        Member m1 = instance.getCluster().getLocalMember();
        m1.setIntAttribute("First", 1);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        Set<Member> memberSet = instance2.getCluster().getMembers();

        assertEquals(2, memberSet.size());

        Member member = null;
        for (Member m : memberSet) {
            if (m == instance2.getCluster().getLocalMember()) {
                continue;
            }
            member = m;
        }
        assertEquals(m1, member);
        assertEquals(1, (int) member.getIntAttribute("First"));

        final CountDownLatch memberAttributeChangedLatch = new CountDownLatch(1);
        client.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
                memberAttributeChangedLatch.countDown();
            }
        });

        m1.setIntAttribute("Test", 2);

        assertOpenEventually(memberAttributeChangedLatch);
        assertEquals(2, (int) m1.getIntAttribute("Test"));
    }
}
