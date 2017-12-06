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

package com.hazelcast.reliableidgen.impl;

import com.hazelcast.reliableidgen.ReliableIdGenerator;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReliableIdGenerator_NodeIdOverflowIntegrationTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void when_memberOutOfRangeNodeId_then_theOtherMemberUsed() throws Exception {
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();
        assignOutOfRangeNodeId(instance2);

        // let's use the instance with out-of-range node ID to generate IDs, it should succeed
        ReliableIdGenerator gen = instance2.getReliableIdGenerator("gen");
        gen.newId();
    }

    @Test
    public void when_allMembersOutOfRangeNodeId_then_error() {
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();
        assignOutOfRangeNodeId(instance1);
        assignOutOfRangeNodeId(instance2);

        ReliableIdGenerator gen = instance1.getReliableIdGenerator("gen");

        exception.expect(HazelcastException.class);
        exception.expectMessage("All members have node ID out of range");
        gen.newId();
    }

    private void assignOutOfRangeNodeId(HazelcastInstance instance2) {
        MemberImpl member = (MemberImpl) instance2.getCluster().getLocalMember();
        member.setMemberListJoinVersion(100000);
    }
}
