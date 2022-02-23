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
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.NodeFilter;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.internal.util.ServiceLoader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.CollectionUtil.nullToEmpty;

public class DefaultDiscoveryService
        implements DiscoveryService {

    private static final String SERVICE_LOADER_TAG = DiscoveryStrategyFactory.class.getCanonicalName();

    private final ILogger logger;
    private final DiscoveryNode discoveryNode;
    private final NodeFilter nodeFilter;
    private final Iterable<DiscoveryStrategy> discoveryStrategies;

    public DefaultDiscoveryService(DiscoveryServiceSettings settings) {
        this.logger = settings.getLogger();
        this.discoveryNode = settings.getDiscoveryNode();
        this.nodeFilter = getNodeFilter(settings);
        this.discoveryStrategies = loadDiscoveryStrategies(settings);
    }

    @Override
    public void start() {
        for (DiscoveryStrategy discoveryStrategy : discoveryStrategies) {
            discoveryStrategy.start();
        }
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        Set<DiscoveryNode> discoveryNodes = new HashSet<DiscoveryNode>();
        for (DiscoveryStrategy discoveryStrategy : discoveryStrategies) {
            Iterable<DiscoveryNode> candidates = discoveryStrategy.discoverNodes();

            if (candidates != null) {
                for (DiscoveryNode candidate : candidates) {
                    if (validateCandidate(candidate)) {
                        discoveryNodes.add(candidate);
                    }
                }
            }
        }
        return discoveryNodes;
    }

    @Override
    public Map<String, String> discoverLocalMetadata() {
        Map<String, String> metadata = new HashMap<>();
        for (DiscoveryStrategy discoveryStrategy : discoveryStrategies) {
            metadata.putAll(discoveryStrategy.discoverLocalMetadata());
        }
        return metadata;
    }

    @Override
    public void destroy() {
        for (DiscoveryStrategy discoveryStrategy : discoveryStrategies) {
            discoveryStrategy.destroy();
        }
    }

    public Iterable<DiscoveryStrategy> getDiscoveryStrategies() {
        return discoveryStrategies;
    }

    private NodeFilter getNodeFilter(DiscoveryServiceSettings settings) {
        DiscoveryConfig discoveryConfig = settings.getDiscoveryConfig();
        ClassLoader configClassLoader = settings.getConfigClassLoader();
        if (discoveryConfig.getNodeFilter() != null) {
            return discoveryConfig.getNodeFilter();
        }
        if (discoveryConfig.getNodeFilterClass() != null) {
            try {
                ClassLoader cl = configClassLoader;
                if (cl == null) {
                    cl = DefaultDiscoveryService.class.getClassLoader();
                }

                String className = discoveryConfig.getNodeFilterClass();
                return (NodeFilter) cl.loadClass(className).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to configure discovery node filter", e);
            }
        }
        return null;
    }

    private boolean validateCandidate(DiscoveryNode candidate) {
        return nodeFilter == null || nodeFilter.test(candidate);
    }

    private Iterable<DiscoveryStrategy> loadDiscoveryStrategies(DiscoveryServiceSettings settings) {
        ClassLoader configClassLoader = settings.getConfigClassLoader();

        try {
            Collection<DiscoveryStrategyConfig> discoveryStrategyConfigs = new ArrayList<DiscoveryStrategyConfig>(
                    settings.getAllDiscoveryConfigs());
            List<DiscoveryStrategyFactory> factories = collectFactories(discoveryStrategyConfigs, configClassLoader);

            List<DiscoveryStrategy> discoveryStrategies = new ArrayList<DiscoveryStrategy>();
            for (DiscoveryStrategyConfig config : discoveryStrategyConfigs) {
                DiscoveryStrategy discoveryStrategy = buildDiscoveryStrategy(config, factories);
                discoveryStrategies.add(discoveryStrategy);
            }

            if (discoveryStrategies.isEmpty() && settings.isAutoDetectionEnabled()) {
                logger.fine("Discovery auto-detection enabled, looking for available discovery strategies");
                DiscoveryStrategyFactory autoDetectedFactory = detectDiscoveryStrategyFactory(factories);
                if (autoDetectedFactory != null) {
                    logger.info(String.format("Auto-detection selected discovery strategy: %s", autoDetectedFactory.getClass()));
                    discoveryStrategies
                            .add(autoDetectedFactory.newDiscoveryStrategy(discoveryNode, logger, Collections.emptyMap()));
                } else {
                    logger.fine("No discovery strategy is applicable for auto-detection");
                }
            }
            return discoveryStrategies;
        } catch (Exception e) {
            if (e instanceof ValidationException) {
                throw new InvalidConfigurationException("Invalid configuration", e);
            } else {
                throw new RuntimeException("Failed to configure discovery strategies", e);
            }
        }
    }

    private List<DiscoveryStrategyFactory> collectFactories(Collection<DiscoveryStrategyConfig> strategyConfigs,
                                                            ClassLoader classloader)
            throws Exception {
        Iterator<DiscoveryStrategyFactory> iterator = ServiceLoader
                .iterator(DiscoveryStrategyFactory.class, SERVICE_LOADER_TAG, classloader);

        // Collect possible factories
        List<DiscoveryStrategyFactory> factories = new ArrayList<DiscoveryStrategyFactory>();
        while (iterator.hasNext()) {
            factories.add(iterator.next());
        }
        for (DiscoveryStrategyConfig config : strategyConfigs) {
            DiscoveryStrategyFactory factory = config.getDiscoveryStrategyFactory();
            if (factory != null) {
                factories.add(factory);
            }
        }
        return factories;
    }

    private DiscoveryStrategy buildDiscoveryStrategy(DiscoveryStrategyConfig config,
                                                     List<DiscoveryStrategyFactory> candidateFactories) {
        for (DiscoveryStrategyFactory factory : candidateFactories) {
            Class<? extends DiscoveryStrategy> discoveryStrategyType = factory.getDiscoveryStrategyType();
            String className = discoveryStrategyType.getName();
            String factoryClassName = getFactoryClassName(config);
            if (className.equals(factoryClassName)) {
                Map<String, Comparable> properties = DiscoveryServicePropertiesUtil
                        .prepareProperties(config.getProperties(), nullToEmpty(factory.getConfigurationProperties()));
                return factory.newDiscoveryStrategy(discoveryNode, logger, properties);
            }
        }
        throw new ValidationException(
                "There is no discovery strategy factory to create '" + config + "' Is it a typo in a strategy classname? "
                        + "Perhaps you forgot to include implementation on a classpath?");
    }

    private DiscoveryStrategyFactory detectDiscoveryStrategyFactory(List<DiscoveryStrategyFactory> factories) {
        DiscoveryStrategyFactory highestPriorityFactory = null;
        for (DiscoveryStrategyFactory factory : factories) {
            try {
                if (factory.isAutoDetectionApplicable()) {
                    logger.fine(
                            String.format("Discovery strategy factory '%s' is auto-applicable to the current runtime environment",
                                    factory.getClass()));
                    if (highestPriorityFactory == null || factory.discoveryStrategyLevel().getPriority()
                            > highestPriorityFactory.discoveryStrategyLevel().getPriority()) {
                        highestPriorityFactory = factory;
                    }
                } else {
                    logger.fine(String.format("Discovery Factory '%s' is not auto-applicable to the current runtime environment",
                            factory.getClass()));
                }
            } catch (Exception e) {
                // exception in auto-detection should not prevent Hazelcast from starting
                logger.finest(e);
            }
        }
        return highestPriorityFactory;
    }

    private String getFactoryClassName(DiscoveryStrategyConfig config) {
        if (config.getDiscoveryStrategyFactory() != null) {
            DiscoveryStrategyFactory factory = config.getDiscoveryStrategyFactory();
            return factory.getDiscoveryStrategyType().getName();
        }
        return config.getClassName();
    }
}
