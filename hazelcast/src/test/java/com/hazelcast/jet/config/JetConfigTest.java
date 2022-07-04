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

package com.hazelcast.jet.config;

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
    public void testJetIsDisabledByDefault() {
        assertFalse(new JetConfig().isEnabled());
        assertFalse(new Config().getJetConfig().isEnabled());
    }
}
