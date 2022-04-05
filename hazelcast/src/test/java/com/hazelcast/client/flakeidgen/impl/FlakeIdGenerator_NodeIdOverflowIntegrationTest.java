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

package com.hazelcast.client.flakeidgen.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
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

    private static final ILogger LOGGER = Logger.getLogger(FlakeIdGenerator_NodeIdOverflowIntegrationTest.class);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private TestHazelcastFactory factory = new TestHazelcastFactory();
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
        // Design of this test: we create two members. To one of them, out-of-range memberListJoinVersion
        // is assigned. This causes the client message to fail and the client will retry with another member
        // and will never again try the failed member.
        //
        // Probability of initially choosing a good member and not going through the process of choosing another
        // one is 50%. To avoid false success, we retry 10 times, which gives the probability of false success
        // of 0.1%.
        assignOverflowedNodeId(instance2);

        ClientConfig clientConfig = new ClientConfig();
        // disable smart routing - such clients must also work reliably
        clientConfig.getNetworkConfig().setSmartRouting(false);
        for (int i = 0; i < 10; i++) {
            LOGGER.info("Creating client " + i);
            HazelcastInstance client = factory.newHazelcastClient(clientConfig);
            FlakeIdGenerator gen = client.getFlakeIdGenerator("gen");
            for (int j = 0; j < 100; j++) {
                // call should not fail
                gen.newId();
            }
        }
    }

    @Test
    public void when_allMembersOutOfRangeNodeId_then_error() {
        assignOverflowedNodeId(instance1);
        assignOverflowedNodeId(instance2);

        HazelcastInstance client = factory.newHazelcastClient();
        FlakeIdGenerator gen = client.getFlakeIdGenerator("gen");

        exception.expect(HazelcastException.class);
        exception.expectMessage("All members have node ID out of range");
        gen.newId();
    }

    private static void assignOverflowedNodeId(HazelcastInstance instance2) {
        MemberImpl member = (MemberImpl) instance2.getCluster().getLocalMember();
        member.setMemberListJoinVersion(100000);
    }
}
