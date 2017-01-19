/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.QueryDuringMigrationsStressTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;

/**
 * Test querying a cluster while members are shutting down and joining.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientQueryDuringMigrationsStressTest extends QueryDuringMigrationsStressTest {

    private HazelcastInstance[] clients;

    protected void setupInternal() {
        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        clients = new HazelcastInstance[CONCURRENT_QUERYING_CLIENTS];
        for (int i = 0; i < CONCURRENT_QUERYING_CLIENTS; i++) {
            clients[i] = f.newHazelcastClient(getClientConfig(getFirstMember()));
        }
    }

    @Override
    protected TestHazelcastInstanceFactory createFactory() {
        return new TestHazelcastFactory();
    }

    @Override
    protected HazelcastInstance getQueryingInstance(int ix) {
        return clients[ix];
    }

    // get client configuration that guarantees our client will connect to the specific member
    private ClientConfig getClientConfig(HazelcastInstance member) {
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        config.getNetworkConfig().setSmartRouting(false);

        InetSocketAddress socketAddress = member.getCluster().getLocalMember().getSocketAddress();

        config.getNetworkConfig().
                addAddress(socketAddress.getHostName() + ":" + socketAddress.getPort());

        return config;
    }
}
