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
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConflictingConfigTest {


    @Test(expected = IllegalStateException.class)
    public void testHazelcastCloud_and_DiscoverySPIEnabled() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getCloudConfig().setEnabled(true);
        config.setProperty(ClientProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        HazelcastClient.newHazelcastClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testHazelcastCloudViaProperty_and_DiscoverySPIEnabled() {
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        config.setProperty(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), "TOKEN");
        HazelcastClient.newHazelcastClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testHazelcastCloud_firstClass_and_propertyBased() {
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), "TOKEN");
        config.getNetworkConfig().getCloudConfig().setEnabled(true);
        HazelcastClient.newHazelcastClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testClusterMembersGiven_and_DiscoverySPIEnabled() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.setProperty(ClientProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        HazelcastClient.newHazelcastClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testClusterMembersGiven_and_HazelcastCloudEnabled() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().getCloudConfig().setEnabled(true);
        HazelcastClient.newHazelcastClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testClusterMembersGiven_and_HazelcastCloudViaProperty() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.setProperty(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), "TOKEN");
        HazelcastClient.newHazelcastClient(config);
    }
}
