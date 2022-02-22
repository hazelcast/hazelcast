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

import com.hazelcast.core.HazelcastException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class AbstractClientFailoverConfigBuilderTest {
    protected ClientFailoverConfig fullClientConfig;

    @After
    @Before
    public void beforeAndAfter() {
        System.clearProperty("hazelcast.client.failover.config");
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingFile() {
        System.setProperty("hazelcast.client.failover.config", "idontexist");
        buildConfig();
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingClasspathResource() {
        System.setProperty("hazelcast.client.failover.config", "classpath:idontexist");
        buildConfig();
    }

    @Test
    public abstract void loadingThroughSystemProperty_existingClasspathResource();

    @Test
    public void testClientFailoverConfig() {
        assertSampleFailoverConfig(fullClientConfig);
    }

    @Test
    public abstract void testVariableReplacementFromSystemProperties();

    @Test
    public abstract void testVariableReplacementFromProperties();

    @Test
    public abstract void testWithClasspathConfig();

    @Test
    public abstract void testVariableReplacementFromSystemPropertiesWithClasspathConfig();

    abstract ClientFailoverConfig buildConfig();

    void assertSampleFailoverConfig(ClientFailoverConfig config) {
        List<ClientConfig> clientConfigs = fullClientConfig.getClientConfigs();
        assertEquals(2, clientConfigs.size());
        assertEquals("cluster1", clientConfigs.get(0).getClusterName());
        assertEquals("cluster2", clientConfigs.get(1).getClusterName());
        assertEquals(4, config.getTryCount());
    }
}
