/*
 * Copyright (c) 2015, Christoph Engelbert (aka noctarius) and
 * contributors. All rights reserved.
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
package com.noctarius.hazelcast.kubernetes;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Just the factory to create the Kubernetes Discovery Strategy
 */
public class HazelcastKubernetesDiscoveryStrategyFactory
        implements DiscoveryStrategyFactory {

    private static final Collection<PropertyDefinition> PROPERTY_DEFINITIONS;

    static {
        PROPERTY_DEFINITIONS = Collections.unmodifiableCollection(Arrays.asList( //
                KubernetesProperties.SERVICE_DNS, //
                KubernetesProperties.SERVICE_NAME, //
                KubernetesProperties.NAMESPACE, //
                KubernetesProperties.SERVICE_LABEL_NAME, //
                KubernetesProperties.SERVICE_LABEL_VALUE,
				KubernetesProperties.KUBERNETES_MASTER_URL,
				KubernetesProperties.KUBERNETES_API_TOKEN));
    }

    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return HazelcastKubernetesDiscoveryStrategy.class;
    }

    public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                  Map<String, Comparable> properties) {

        return new HazelcastKubernetesDiscoveryStrategy(logger, properties);
    }

    public Collection<PropertyDefinition> getConfigurationProperties() {
        return PROPERTY_DEFINITIONS;
    }
}
