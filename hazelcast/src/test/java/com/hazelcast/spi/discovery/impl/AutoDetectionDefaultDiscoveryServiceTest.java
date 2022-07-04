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

package com.hazelcast.spi.discovery.impl;

import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AutoDetectionDefaultDiscoveryServiceTest {
    private static final ILogger LOGGER = Logger.getLogger(AutoDetectionDefaultDiscoveryServiceTest.class);

    private final DiscoveryServiceSettings settings = discoveryServiceSettings();

    @After
    @Before
    public void clearAutoDetectionFields() {
        TestDiscoveryStrategyFactory.isAutoDetectionApplicable = false;
        TestHighPriorityDiscoveryStrategyFactory.isAutoDetectionApplicable = false;
    }

    private DiscoveryServiceSettings discoveryServiceSettings() {
        DiscoveryServiceSettings settings = new DiscoveryServiceSettings();
        settings.setLogger(LOGGER);
        settings.setDiscoveryConfig(new DiscoveryConfig());
        settings.setAutoDetectionEnabled(true);
        return settings;
    }

    @Test
    public void selectStrategyWhenAutoDetectionApplicable() {
        TestDiscoveryStrategyFactory.isAutoDetectionApplicable = true;
        assertDiscoveryStrategy(TestDiscoveryStrategy.class);
    }

    @Test
    public void selectStrategyWithHigherPriority() {
        TestDiscoveryStrategyFactory.isAutoDetectionApplicable = true;
        TestHighPriorityDiscoveryStrategyFactory.isAutoDetectionApplicable = true;
        assertDiscoveryStrategy(TestHighPriorityDiscoveryStrategy.class);
    }

    private void assertDiscoveryStrategy(Class<?> clazz) {
        DefaultDiscoveryService discoveryService = new DefaultDiscoveryService(settings);

        Iterator<DiscoveryStrategy> discoveryStrategies = discoveryService.getDiscoveryStrategies().iterator();
        DiscoveryStrategy discoveryStrategy = discoveryStrategies.next();
        assertEquals(clazz, discoveryStrategy.getClass());
        assertFalse(discoveryStrategies.hasNext());

    }

    private static class TestDiscoveryStrategyFactory implements DiscoveryStrategyFactory {
        private static boolean isAutoDetectionApplicable = false;

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return TestDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                      Map<String, Comparable> properties) {
            return new TestDiscoveryStrategy(logger, properties);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return Collections.emptyList();
        }

        @Override
        public boolean isAutoDetectionApplicable() {
            return isAutoDetectionApplicable;
        }

        @Override
        public DiscoveryStrategyLevel discoveryStrategyLevel() {
            return DiscoveryStrategyLevel.CLOUD_VM;
        }
    }

    private static class TestDiscoveryStrategy extends AbstractDiscoveryStrategy {
        private TestDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return null;
        }
    }

    private static class TestHighPriorityDiscoveryStrategyFactory implements DiscoveryStrategyFactory {
        private static boolean isAutoDetectionApplicable = false;

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return TestHighPriorityDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                      Map<String, Comparable> properties) {
            return new TestHighPriorityDiscoveryStrategy(logger, properties);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return Collections.emptyList();
        }

        @Override
        public boolean isAutoDetectionApplicable() {
            return isAutoDetectionApplicable;
        }

        @Override
        public DiscoveryStrategyLevel discoveryStrategyLevel() {
            return DiscoveryStrategyLevel.PLATFORM;
        }
    }

    private static class TestHighPriorityDiscoveryStrategy extends AbstractDiscoveryStrategy {
        private TestHighPriorityDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return null;
        }
    }

}
