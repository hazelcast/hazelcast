/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

class AbstractClientFailoverConfigBuilderTest {
    protected ClientFailoverConfig fullClientConfig;

    @After
    @Before
    public void beforeAndAfter() {
        System.clearProperty("hazelcast.client.failover.config");
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingFile() {
        System.setProperty("hazelcast.client.failover.config", "idontexist");
        new YamlClientFailoverConfigBuilder();
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingClasspathResource() {
        System.setProperty("hazelcast.client.failover.config", "classpath:idontexist");
        new YamlClientFailoverConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() {
        System.setProperty("hazelcast.client.failover.config", "classpath:hazelcast-client-failover-sample.yaml");

        YamlClientFailoverConfigBuilder configBuilder = new YamlClientFailoverConfigBuilder();
        ClientFailoverConfig config = configBuilder.build();
        assertEquals(2, config.getClientConfigs().size());
        assertEquals("cluster1", config.getClientConfigs().get(0).getGroupConfig().getName());
        assertEquals("cluster2", config.getClientConfigs().get(1).getGroupConfig().getName());
        assertEquals(4, config.getTryCount());
    }

    @Test
    public void testClientFailoverConfig() {
        List<ClientConfig> clientConfigs = fullClientConfig.getClientConfigs();
        assertEquals(2, clientConfigs.size());
        assertEquals("cluster1", clientConfigs.get(0).getGroupConfig().getName());
        assertEquals("cluster2", clientConfigs.get(1).getGroupConfig().getName());
        assertEquals(4, fullClientConfig.getTryCount());
    }
}
