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

package com.hazelcast.config;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.internal.config.DiscoveryConfigReadOnly;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.NodeFilter;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiscoveryTest {

    @Test
    public void test_DiscoveryConfig_setDiscoveryServiceProvider() {
        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        DiscoveryServiceProvider discoveryServiceProvider = new TestDiscoveryServiceProvider();
        discoveryConfig.setDiscoveryServiceProvider(discoveryServiceProvider);
        assertSame(discoveryServiceProvider, discoveryConfig.getDiscoveryServiceProvider());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_DiscoveryConfigReadOnly_setDiscoveryServiceProvider_thenUnsupportedOperationException() {
        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        DiscoveryConfig readOnly = new DiscoveryConfigReadOnly(discoveryConfig);
        readOnly.setDiscoveryServiceProvider(new TestDiscoveryServiceProvider());
    }

    @Test
    public void test_DiscoveryConfig_setNodeFilter() {
        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        TestNodeFilter nodeFilter = new TestNodeFilter();
        discoveryConfig.setNodeFilter(nodeFilter);
        assertSame(nodeFilter, discoveryConfig.getNodeFilter());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_DiscoveryConfigReadOnly_setNodeFilter_thenUnsupportedOperationException() {
        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        DiscoveryConfig readOnly = new DiscoveryConfigReadOnly(discoveryConfig);
        readOnly.setNodeFilter(new TestNodeFilter());
    }

    @Test
    public void test_DiscoveryConfig_setNodeFilterClass() {
        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        String nodeFilterClass = TestNodeFilter.class.getName();
        discoveryConfig.setNodeFilterClass(nodeFilterClass);
        assertEquals(nodeFilterClass, discoveryConfig.getNodeFilterClass());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_DiscoveryConfigReadOnly_setNodeFilterClass_thenUnsupportedOperationException() {
        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        DiscoveryConfig readOnly = new DiscoveryConfigReadOnly(discoveryConfig);
        readOnly.setNodeFilterClass(TestNodeFilter.class.getName());
    }

    @Test
    public void test_DiscoveryConfigReadOnly_addDiscoveryStrategyConfig() {
        DiscoveryConfig discoveryConfig = new DiscoveryConfig();

        DiscoveryStrategyFactory discoveryStrategyFactory = new TestDiscoveryStrategyFactory();
        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(discoveryStrategyFactory);
        discoveryConfig.addDiscoveryStrategyConfig(discoveryStrategyConfig);

        assertSame(discoveryStrategyConfig, discoveryConfig.getDiscoveryStrategyConfigs().iterator().next());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_DiscoveryConfigReadOnly_addDiscoveryStrategyConfig_thenUnsupportedOperationException() {
        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        DiscoveryConfig readOnly = new DiscoveryConfigReadOnly(discoveryConfig);

        DiscoveryStrategyFactory discoveryStrategyFactory = new TestDiscoveryStrategyFactory();
        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(discoveryStrategyFactory);
        readOnly.addDiscoveryStrategyConfig(discoveryStrategyConfig);
    }

    private static class TestDiscoveryServiceProvider implements DiscoveryServiceProvider {

        @Override
        public DiscoveryService newDiscoveryService(DiscoveryServiceSettings settings) {
            return null;
        }
    }

    private static class TestNodeFilter implements NodeFilter {

        @Override
        public boolean test(DiscoveryNode candidate) {
            return false;
        }
    }

    private static class TestDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return null;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                      Map<String, Comparable> properties) {
            return null;
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return null;
        }
    }
}
