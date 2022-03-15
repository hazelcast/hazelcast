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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.StaticLB;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.cluster.Member;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapWithIndexCreationTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    /**
     * Given a two members (A, B) cluster, a non-smart client connected to B attempts to create a map proxy targeting member A.
     */
    @Test
    public void test_createMapWithIndexes_whenProxyCreatedOnMemberOtherThanClientOwner() {
        Config config = new XmlConfigBuilder().build();

        MapConfig mapConfig = config.getMapConfig("test");
        List<IndexConfig> indexConfigs = mapConfig.getIndexConfigs();

        IndexConfig indexConfig = new IndexConfig();
        indexConfig.addAttribute("name");
        indexConfig.setType(IndexType.SORTED);
        indexConfigs.add(indexConfig);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        // ProxyManager#findNextAddressToSendCreateRequest uses the configured load balancer to find the next address
        // to which proxy creation request will be sent. We want this to be member hz1.
        clientConfig.setLoadBalancer(new StaticLB((Member) hz1.getLocalEndpoint()));
        clientConfig.getNetworkConfig().setSmartRouting(false);
        // the client only connects to member hz2.
        clientConfig.getNetworkConfig().addAddress(hz2.getCluster().getLocalMember().getAddress().getHost() + ":"
                + hz2.getCluster().getLocalMember().getAddress().getPort());

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IMap<String, SampleTestObjects.Employee> test = client.getMap("test");
        test.put("foo", new SampleTestObjects.Employee(1, "name", "age", 32, true, 230));
    }
}
