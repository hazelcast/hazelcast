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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FlakeIdGenerator_NodeIdOverflowIntegrationTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
    private HazelcastInstance instance2;
    private HazelcastInstance instance1;

    @Before
    public void before() {
        Config cfg = new Config();
        cfg.getFlakeIdGeneratorConfig("gen").setPrefetchCount(1);
        instance1 = factory.newHazelcastInstance(cfg);
        instance2 = factory.newHazelcastInstance(cfg);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void when_memberOutOfRangeNodeId_then_theOtherMemberUsed() {
        assignOutOfRangeNodeId(instance2);

        // let's use the instance with out-of-range node ID to generate IDs, it should succeed
        FlakeIdGenerator gen = instance2.getFlakeIdGenerator("gen");
        for (int i = 0; i < 100; i++) {
            gen.newId();
        }
    }

    @Test
    public void when_allMembersOutOfRangeNodeId_then_error() {
        assignOutOfRangeNodeId(instance1);
        assignOutOfRangeNodeId(instance2);

        FlakeIdGenerator gen = instance1.getFlakeIdGenerator("gen");

        exception.expect(HazelcastException.class);
        exception.expectMessage("All members have node ID out of range");
        gen.newId();
    }

    private static void assignOutOfRangeNodeId(HazelcastInstance instance) {
        MemberImpl member = (MemberImpl) instance.getCluster().getLocalMember();
        member.setMemberListJoinVersion(100000);
    }
}
