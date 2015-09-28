/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.DiscoveryStrategiesConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.ValueValidator;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.spi.discovery.DiscoveredNode;
import com.hazelcast.spi.discovery.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.NodeFilter;
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

    private final Iterable<DiscoveryStrategy> discoveryProviders;
    private final DiscoveryMode discoveryMode;
    private final NodeFilter nodeFilter;

    public DefaultDiscoveryService(DiscoveryMode discoveryMode, DiscoveryStrategiesConfig discoveryStrategiesConfig,
                                   ClassLoader configClassLoader) {

        this.discoveryMode = discoveryMode;
        this.nodeFilter = getNodeFilter(discoveryStrategiesConfig, configClassLoader);
        this.discoveryProviders = loadDiscoveryProviders(discoveryStrategiesConfig, configClassLoader);
    }

    @Override
    public void start() {
        for (DiscoveryStrategy discoveryStrategy : discoveryProviders) {
            discoveryStrategy.start(discoveryMode);
        }
    }

    @Override
    public Iterable<DiscoveredNode> discoverNodes() {
        Set<DiscoveredNode> discoveredNodes = new HashSet<DiscoveredNode>();
        for (DiscoveryStrategy discoveryStrategy : discoveryProviders) {
            Iterable<DiscoveredNode> candidates = discoveryStrategy.discoverNodes();

            if (candidates != null) {
                for (DiscoveredNode candidate : candidates) {
                    if (validateCandidate(candidate)) {
                        discoveredNodes.add(candidate);
                    }
                }
            }
        }
        return discoveredNodes;
    }

    @Override
    public void destroy() {
        for (DiscoveryStrategy discoveryStrategy : discoveryProviders) {
            discoveryStrategy.destroy();
        }
    }

    private NodeFilter getNodeFilter(DiscoveryStrategiesConfig discoveryStrategiesConfig, ClassLoader configClassLoader) {
        if (discoveryStrategiesConfig.getNodeFilter() != null) {
            return discoveryStrategiesConfig.getNodeFilter();
        }
        if (discoveryStrategiesConfig.getNodeFilterClass() != null) {
            try {
                ClassLoader cl = configClassLoader;
                if (cl == null) {
                    cl = DefaultDiscoveryService.class.getClassLoader();
                }

                String className = discoveryStrategiesConfig.getNodeFilterClass();
                return (NodeFilter) cl.loadClass(className).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to configure discovery node filter", e);
            }
        }
        return null;
    }

    private boolean validateCandidate(DiscoveredNode candidate) {
        return nodeFilter == null || nodeFilter.test(candidate);
    }

    private Iterable<DiscoveryStrategy> loadDiscoveryProviders(DiscoveryStrategiesConfig providersConfig,
                                                               ClassLoader configClassLoader) {
        try {
            Collection<DiscoveryStrategyConfig> discoveryStrategyConfigs = providersConfig.getDiscoveryStrategyConfigs();

            Iterator<DiscoveryStrategyFactory> iterator = ServiceLoader
                    .iterator(DiscoveryStrategyFactory.class, SERVICE_LOADER_TAG, configClassLoader);

            List<DiscoveryStrategy> discoveryStrategies = new ArrayList<DiscoveryStrategy>();
            while (iterator.hasNext()) {
                DiscoveryStrategyFactory factory = iterator.next();
                DiscoveryStrategy discoveryStrategy = buildDiscoveryProvider(factory, discoveryStrategyConfigs);
                if (discoveryStrategy != null) {
                    discoveryStrategies.add(discoveryStrategy);
                }
            }
            return discoveryStrategies;
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure discovery strategies", e);
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

    private DiscoveryStrategy buildDiscoveryProvider(DiscoveryStrategyFactory factory,
                                                     Collection<DiscoveryStrategyConfig> discoveryStrategyConfigs) {
        Class<? extends DiscoveryStrategy> discoveryProviderType = factory.getDiscoveryStrategyType();
        String className = discoveryProviderType.getName();

        for (DiscoveryStrategyConfig config : discoveryStrategyConfigs) {
            if (config.getClassName().equals(className)) {
                Map<String, Comparable> properties = buildProperties(factory, config, className);
                return factory.newDiscoveryStrategy(properties);
            }
        }
        return null;
    }

}
