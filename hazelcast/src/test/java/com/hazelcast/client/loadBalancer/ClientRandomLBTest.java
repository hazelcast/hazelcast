/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientRandomLBTest {

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testRandomLB_withoutMembers() {
        RandomLB lb = new RandomLB();

        assertNull(lb.next());
        assertNull(lb.nextDataMember());
    }

    @Test
    public void testRandomLB_withMembers() {
        RandomLB randomLB = new RandomLB();

        HazelcastInstance server = factory.newHazelcastInstance();
        Cluster cluster = server.getCluster();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancer(randomLB);

        randomLB.init(cluster, clientConfig);

        Member member = cluster.getLocalMember();

        assertEquals(member, randomLB.next());
        assertEquals(member, randomLB.nextDataMember());
    }

    @Test
    public void testRandomLB_withLiteMembers() {
        RandomLB randomLB = new RandomLB();

        HazelcastInstance server = factory.newHazelcastInstance(new Config().setLiteMember(true));
        Cluster cluster = server.getCluster();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancer(randomLB);

        randomLB.init(cluster, clientConfig);

        Member member = cluster.getLocalMember();

        assertEquals(member, randomLB.next());
        assertNull(randomLB.nextDataMember());
    }
}
