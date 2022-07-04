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

package com.hazelcast.client.impl.spi.impl.discovery;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.TestEnvironment.isSolaris;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientAutoDetectionDiscoveryTest extends HazelcastTestSupport {

    @After
    public void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    @Test
    public void defaultDiscovery() {
        Config c = new Config();
        c.setClusterName(getTestMethodName());

        if (isSolaris()) {
            c.setProperty(ClusterProperty.MULTICAST_SOCKET_SET_INTERFACE.getName(), "false");
        }

        Hazelcast.newHazelcastInstance(c);
        Hazelcast.newHazelcastInstance(c);

        ClientConfig clientConfig = new ClientConfig().setClusterName(getTestMethodName());
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        assertClusterSizeEventually(2, client);
    }

    @Test
    public void autoDetectionDisabled() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getAutoDetectionConfig().setEnabled(false);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        // uses 127.0.0.1 and finds only one standalone member
        assertClusterSizeEventually(1, client);
    }

    @Test
    public void autoDetectionNotUsedWhenOtherDiscoveryEnabled() {
        Config config = new Config();
        config.getNetworkConfig().setPort(5710);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5710");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        assertClusterSizeEventually(1, client);
    }
}
