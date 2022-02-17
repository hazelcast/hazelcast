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

package com.hazelcast.client.config;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.impl.clientside.ClientTestUtil;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.CustomLoadBalancer;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientConfigLoadBalancerTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void shouldCreateRoundRobinLoadBalancerWhenNoConfigProvided() {
        hazelcastFactory.newHazelcastInstance();

        HazelcastInstance instance = hazelcastFactory.newHazelcastClient(new ClientConfig());
        HazelcastClientInstanceImpl client = ClientTestUtil.getHazelcastClientInstanceImpl(instance);

        LoadBalancer actual = client.getLoadBalancer();
        assertTrue(actual instanceof RoundRobinLB);
    }

    @Test
    public void shouldUseConfigClassLoaderInstanceWhenClassNameNotSpecified() {
        hazelcastFactory.newHazelcastInstance();

        LoadBalancer loadBalancer = new RandomLB();

        ClientConfig config = new ClientConfig();
        config.setLoadBalancer(loadBalancer);

        HazelcastInstance instance = hazelcastFactory.newHazelcastClient(config);
        HazelcastClientInstanceImpl client = ClientTestUtil.getHazelcastClientInstanceImpl(instance);

        LoadBalancer actual = client.getLoadBalancer();
        assertSame(loadBalancer, actual);
    }

    @Test
    public void shouldCreateCustomLoadBalancerWhenConfigInstanceNotProvidedAndClassNameSpecified() {
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancerClassName("com.hazelcast.client.test.CustomLoadBalancer");

        HazelcastInstance instance = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl client = ClientTestUtil.getHazelcastClientInstanceImpl(instance);

        LoadBalancer actual = client.getLoadBalancer();
        assertTrue(actual instanceof CustomLoadBalancer);
    }
}
