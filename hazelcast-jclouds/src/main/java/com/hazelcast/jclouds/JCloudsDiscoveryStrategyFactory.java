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

package com.hazelcast.jclouds;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Factory class which returns {@link JCloudsDiscoveryStrategy} to Discovery SPI
 */
public class JCloudsDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

    private static final Collection<PropertyDefinition> PROPERTY_DEFINITIONS;

    static {
        List<PropertyDefinition> propertyDefinitions = new ArrayList<PropertyDefinition>();
        propertyDefinitions.add(JCloudsProperties.CREDENTIAL);
        propertyDefinitions.add(JCloudsProperties.CREDENTIAL_PATH);
        propertyDefinitions.add(JCloudsProperties.GROUP);
        propertyDefinitions.add(JCloudsProperties.IDENTITY);
        propertyDefinitions.add(JCloudsProperties.TAG_KEYS);
        propertyDefinitions.add(JCloudsProperties.TAG_VALUES);
        propertyDefinitions.add(JCloudsProperties.PROVIDER);
        propertyDefinitions.add(JCloudsProperties.REGIONS);
        propertyDefinitions.add(JCloudsProperties.ROLE_NAME);
        propertyDefinitions.add(JCloudsProperties.ZONES);
        propertyDefinitions.add(JCloudsProperties.HZ_PORT);
        PROPERTY_DEFINITIONS = Collections.unmodifiableCollection(propertyDefinitions);
    }

    @Override
    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return JCloudsDiscoveryStrategy.class;
    }

    @Override
    public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode node, ILogger logger, Map<String, Comparable> properties) {
        return new JCloudsDiscoveryStrategy(properties);
    }

    public Collection<PropertyDefinition> getConfigurationProperties() {
        return PROPERTY_DEFINITIONS;
    }
}
