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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DummyClientMapQueryPartitionIteratorTest extends AbstractMapQueryPartitionIteratorTest {

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();

        Config config = getConfig();
        server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        client = factory.newHazelcastClient(getClientConfig(server));
    }

    private ClientConfig getClientConfig(HazelcastInstance instance) {
        Address address = instance.getCluster().getLocalMember().getAddress();
        String addressString = address.getHost() + ":" + address.getPort();

        ClientNetworkConfig networkConfig = new ClientNetworkConfig()
                .setSmartRouting(false)
                .addAddress(addressString);

        return getClientConfig()
                .setNetworkConfig(networkConfig);
    }
}
