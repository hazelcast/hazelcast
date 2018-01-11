/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.loadBalancer;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientRoundRobinLBTest {

    @Test
    public void testRoundRobinLB_withoutMembers() {
        RoundRobinLB lb = new RoundRobinLB();
        Member m = lb.next();
        assertNull(m);
    }

    @Test
    public void testRoundRobinLB_withMembers() {
        RoundRobinLB roundRobinLB = new RoundRobinLB();
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
        final HazelcastInstance server = factory.newHazelcastInstance();

        Cluster cluster = server.getCluster();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancer(roundRobinLB);

        roundRobinLB.init(cluster, clientConfig);

        Member member = cluster.getLocalMember();
        Member nextMember = roundRobinLB.next();

        assertEquals(member, nextMember);
        factory.terminateAll();
    }
}
