/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.azure;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Factory class which returns {@link AzureDiscoveryStrategy} to Discovery SPI
 */
public class AzureDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

    @Override
    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return AzureDiscoveryStrategy.class;
    }

    @Override
    public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode node, ILogger logger,
                                                  Map<String, Comparable> properties) {
        return new AzureDiscoveryStrategy(properties);
    }

    @Override
    public Collection<PropertyDefinition> getConfigurationProperties() {
        List<PropertyDefinition> result = new ArrayList<PropertyDefinition>();
        for (AzureProperties property : AzureProperties.values()) {
            result.add(property.getDefinition());
        }
        return result;
    }
}
