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

package com.hazelcast.internal.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.RestServerEndpointConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test is intentionally not in the {@link ParallelJVMTest} category,
 * since it starts real HazelcastInstances which have REST enabled.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class RestMultiendpointTest
        extends RestTest {

    @Override
    public Config getConfig() {
        Config config = super.getConfig();
        config.setClusterName(randomString());
        ServerSocketEndpointConfig memberEndpointConfig = new ServerSocketEndpointConfig();
        memberEndpointConfig.setName("MEMBER")
                      .setPort(6000)
                      .setPortAutoIncrement(true);

        ServerSocketEndpointConfig clientEndpointConfig = new ServerSocketEndpointConfig();
        clientEndpointConfig.setName("CLIENT")
                            .setPort(5000)
                            .setPortAutoIncrement(true);

        RestServerEndpointConfig restEndpoint = new RestServerEndpointConfig();
        restEndpoint.setName("Text")
                    .setPort(10000)
                    .setPortAutoIncrement(true)
                    .enableAllGroups();

        config.getAdvancedNetworkConfig()
              .setEnabled(true)
              .setMemberEndpointConfig(memberEndpointConfig)
              .setClientEndpointConfig(clientEndpointConfig)
              .setRestEndpointConfig(restEndpoint);

        // we start pairs of HazelcastInstances which form a cluster to have remote invocations for all operations
        JoinConfig join = config.getAdvancedNetworkConfig().getJoin();
        join.getMulticastConfig()
                .setEnabled(false);
        join.getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1:6000")
                .addMember("127.0.0.1:6001");

        instance = factory.newHazelcastInstance(config);

        remoteInstance = factory.newHazelcastInstance(config);

        communicator = new HTTPCommunicator(instance);
        return config;
    }

    @Test
    public void assertAdvancedNetworkInUse() {
        int numberOfEndpointsInConfig = instance.getConfig().getAdvancedNetworkConfig().getEndpointConfigs().size();
        MemberImpl local = getNode(instance).getClusterService().getLocalMember();
        assertTrue(local.getAddressMap().size() == numberOfEndpointsInConfig);
        assertFalse(local.getSocketAddress(EndpointQualifier.REST).equals(local.getSocketAddress(EndpointQualifier.MEMBER)));
    }
}
