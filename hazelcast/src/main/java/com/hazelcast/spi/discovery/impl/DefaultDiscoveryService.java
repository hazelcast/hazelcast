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

package com.hazelcast.spi.discovery.impl;

import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.config.properties.ValueValidator;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.NodeFilter;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.util.ServiceLoader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultDiscoveryService
        implements DiscoveryService {

    private static final String SERVICE_LOADER_TAG = DiscoveryStrategyFactory.class.getCanonicalName();

    private final DiscoveryNode discoveryNode;
    private final ILogger logger;
    private final Iterable<DiscoveryStrategy> discoveryStrategies;
    private final NodeFilter nodeFilter;

    public DefaultDiscoveryService(DiscoveryServiceSettings settings) {
        this.discoveryNode = settings.getDiscoveryNode();
        this.logger = settings.getLogger();
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
    public Map<String, Object> discoverLocalMetadata() {
        Map<String, Object> metadata = new HashMap<String, Object>();
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
        DiscoveryConfig discoveryConfig = settings.getDiscoveryConfig();
        ClassLoader configClassLoader = settings.getConfigClassLoader();

        try {
            Collection<DiscoveryStrategyConfig> discoveryStrategyConfigs = new ArrayList<DiscoveryStrategyConfig>(
                    discoveryConfig.getDiscoveryStrategyConfigs());

            Iterator<DiscoveryStrategyFactory> iterator = ServiceLoader
                    .iterator(DiscoveryStrategyFactory.class, SERVICE_LOADER_TAG, configClassLoader);

            // Collect possible factories
            List<DiscoveryStrategyFactory> factories = new ArrayList<DiscoveryStrategyFactory>();
            while (iterator.hasNext()) {
                factories.add(iterator.next());
            }
            for (DiscoveryStrategyConfig config : discoveryStrategyConfigs) {
                DiscoveryStrategyFactory factory = config.getDiscoveryStrategyFactory();
                if (factory != null) {
                    factories.add(factory);
                }
            }

            List<DiscoveryStrategy> discoveryStrategies = new ArrayList<DiscoveryStrategy>();
            for (DiscoveryStrategyFactory factory : factories) {
                DiscoveryStrategy discoveryStrategy = buildDiscoveryStrategy(factory, discoveryStrategyConfigs);
                if (discoveryStrategy != null) {
                    discoveryStrategies.add(discoveryStrategy);
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

    private Map<String, Comparable> buildProperties(DiscoveryStrategyFactory factory, DiscoveryStrategyConfig config,
                                                    String className) {
        Collection<PropertyDefinition> propertyDefinitions = factory.getConfigurationProperties();
        if (propertyDefinitions == null) {
            return Collections.emptyMap();
        }

        Map<String, Comparable> properties = config.getProperties();
        Map<String, Comparable> mappedProperties = new HashMap<String, Comparable>();

        for (PropertyDefinition propertyDefinition : propertyDefinitions) {
            String propertyKey = propertyDefinition.key();
            Comparable value = properties.get(propertyKey);
            if (value == null) {
                if (!propertyDefinition.optional()) {
                    throw new HazelcastException(
                            "Missing property '" + propertyKey + "' on discovery strategy '" + className + "' configuration");
                }
                continue;
            }

            TypeConverter typeConverter = propertyDefinition.typeConverter();
            Comparable mappedValue = typeConverter.convert(value);

            ValueValidator validator = propertyDefinition.validator();
            if (validator != null) {
                validator.validate(mappedValue);
            }

            mappedProperties.put(propertyKey, mappedValue);
        }

        return mappedProperties;
    }

    private DiscoveryStrategy buildDiscoveryStrategy(DiscoveryStrategyFactory factory,
                                                     Collection<DiscoveryStrategyConfig> discoveryStrategyConfigs) {
        Class<? extends DiscoveryStrategy> discoveryStrategyType = factory.getDiscoveryStrategyType();
        String className = discoveryStrategyType.getName();

        for (DiscoveryStrategyConfig config : discoveryStrategyConfigs) {
            String factoryClassName = getFactoryClassName(config);
            if (className.equals(factoryClassName)) {
                Map<String, Comparable> properties = buildProperties(factory, config, className);
                return factory.newDiscoveryStrategy(discoveryNode, logger, properties);
            }
        }
        return null;
    }

    private String getFactoryClassName(DiscoveryStrategyConfig config) {
        if (config.getDiscoveryStrategyFactory() != null) {
            DiscoveryStrategyFactory factory = config.getDiscoveryStrategyFactory();
            return factory.getDiscoveryStrategyType().getName();
        }
        return config.getClassName();
    }
}
