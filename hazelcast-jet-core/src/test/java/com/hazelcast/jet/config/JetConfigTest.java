/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.config;

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastParallelClassRunner;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class JetConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void when_setInstanceConfig_thenReturnsInstanceConfig() {
        // When
        JetConfig config = new JetConfig();
        InstanceConfig instanceConfig = new InstanceConfig();
        config.setInstanceConfig(instanceConfig);

        // Then
        assertEquals(instanceConfig, config.getInstanceConfig());
    }

    @Test
    public void when_setEdgeConfig_thenReturnsEdgeConfig() {
        // When
        JetConfig config = new JetConfig();
        EdgeConfig edgeConfig = new EdgeConfig();
        config.setDefaultEdgeConfig(edgeConfig);

        // Then
        assertEquals(edgeConfig, config.getDefaultEdgeConfig());
    }

    @Test
    public void when_setHzConfig_thenReturnsHzConfig() {
        // When
        JetConfig jetConfig = new JetConfig();
        Config hzConfig = new Config();
        jetConfig.setHazelcastConfig(hzConfig);

        // Then
        assertEquals(hzConfig, jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_setProperties_thenReturnsProperties() {
        // When
        JetConfig jetConfig = new JetConfig();
        Properties properties = new Properties();
        jetConfig.setProperties(properties);

        // Then
        assertEquals(properties, jetConfig.getProperties());
    }
}
