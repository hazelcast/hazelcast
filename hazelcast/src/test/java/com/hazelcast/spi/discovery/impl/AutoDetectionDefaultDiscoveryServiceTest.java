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
import com.hazelcast.spi.discovery.multicast.MulticastDiscoveryStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
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

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AutoDetectionDefaultDiscoveryServiceTest {
    private static final ILogger LOGGER = Logger.getLogger(AutoDetectionDefaultDiscoveryServiceTest.class);

    private final DiscoveryServiceSettings settings = discoveryServiceSettings();

    @Before
    public void setUp() {
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
    public void selectMulticastByDefault() {
        assertDiscoveryStrategy(MulticastDiscoveryStrategy.class);
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
        public TestDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
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
        public TestHighPriorityDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return null;
        }
    }

}
