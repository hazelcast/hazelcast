/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMemberAttributeTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test(timeout = 120000)
    public void testConfigAttributes() throws Exception {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);
        MemberAttributeConfig memberAttributeConfig = c.getMemberAttributeConfig();
        memberAttributeConfig.setAttribute("Test", "123");

        HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        assertEquals("123", m1.getAttribute("Test"));

        HazelcastInstance h2 = hazelcastFactory.newHazelcastInstance(c);
        Member m2 = h2.getCluster().getLocalMember();
        assertEquals("123", m2.getAttribute("Test"));

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

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        Collection<Member> members = client.getCluster().getMembers();
        for (Member m : members) {
            assertEquals("123", m.getAttribute("Test"));
        }
    }
}
