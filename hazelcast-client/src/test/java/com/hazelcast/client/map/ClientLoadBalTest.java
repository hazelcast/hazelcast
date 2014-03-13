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

package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.util.AbstractLoadBalancer;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.client.util.StaticLB;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientLoadBalTest {

    @After
    public void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void randomLB_NextTest() {
        RandomLB lb = new RandomLB();
        Member m = lb.next();
        assertNull(m);
    }

    @Test
    public void RoundRobinLB_NextTest() {
        RoundRobinLB lb = new RoundRobinLB();
        Member m = lb.next();
        assertNull(m);
    }

    @Test
    public void StaticLBi_nitNextTest() {
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance server = factory.newHazelcastInstance();
        Member member = server.getCluster().getLocalMember();
        StaticLB lb = new StaticLB(member);
        Member nextMember = lb.next();
        assertEquals(member, nextMember);
    }

    @Test
    public void randomLB_initNextTest() {
        loadBalTest(new RandomLB());
    }

    @Test
    public void RoundRobinLB_initNextTest() {
        loadBalTest(new RoundRobinLB());
    }

    private void loadBalTest(AbstractLoadBalancer lb){
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        final HazelcastInstance server = factory.newHazelcastInstance();

        Cluster cluster = server.getCluster();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancer(lb);

        lb.init(cluster, clientConfig);

        Member member = cluster.getLocalMember();
        Member nextMember = lb.next();

        assertEquals(member, nextMember);
    }
}