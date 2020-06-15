/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.discovery;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;

import java.util.Collection;
import java.util.Map;

import static com.hazelcast.spi.discovery.DiscoveryStrategyFactory.DiscoveryStrategyLevel.UNKNOWN;

/**
 * The <code>DiscoveryStrategyFactory</code> is the entry point for strategy vendors. Every
 * {@link DiscoveryStrategy} should have its own factory building it. In rare cases (like
 * multiple version support or similar) one factory might return different provider
 * implementations based on certain criteria. It is also up to the <code>DiscoveryStrategyFactory</code>
 * to cache instances and return them in some kind of a Singleton-like fashion.
 * <p>
 * The defined set of configuration properties describes the existing properties inside
 * of the Hazelcast configuration. It will be used for automatic conversion,
 * type-checking and validation before handing them to the {@link DiscoveryStrategy}.
 * This removes a lot of boilerplate from the provider vendor and provides some convenience as
 * well as guarantee to execute the expected configuration checks. The later is especially
 * important because there is no schema support for properties necessary or provided by the
 * provider plugins. Any kind of violation while verification of any type conversion error
 * as well as missing non-optional properties will throw an exception and prevent the node
 * from starting up.
 *
 * @since 3.6
 */
public interface DiscoveryStrategyFactory {

    /**
     * Returns the type of the {@link DiscoveryStrategy} itself.
     *
     * @return the type of the discovery strategy
     */
    Class<? extends DiscoveryStrategy> getDiscoveryStrategyType();

    /**
     * Instantiates a new instance of the {@link DiscoveryStrategy} with the given configuration
     * properties. The provided {@link HazelcastInstance} can be used to register instances in
     * a service registry whenever the discovery strategy is started.
     *
     * @param discoveryNode the current local <code>DiscoveryNode</code>, representing the local
     *                      connection information if running on a Hazelcast member, otherwise on
     *                      Hazelcast clients always <code>null</code>
     * @param logger        the logger instance
     * @param properties    the properties parsed from the configuration
     * @return a new instance of the discovery strategy
     */
    DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties);

    /**
     * Returns a set of the expected configuration properties. These properties contain
     * information about the value type of the property, if it is required and a possible
     * validator to automatically test and convert values from the XML configuration.
     *
     * @return a set of expected configuration properties
     */
    Collection<PropertyDefinition> getConfigurationProperties();

    /**
     * Checks whether the given discovery strategy may be applied to the environment in which Hazelcast is currently running.
     * <p>
     * User by the auto detection mechanism to decide if which strategy could be used.
     */
    default boolean isApplicableToCurrentEnvironment() {
        return false;
    }

    /**
     * Level of the discovery strategy.
     */
    default DiscoveryStrategyLevel discoveryStrategyLevel() {
        return UNKNOWN;
    }

    /**
     * Level of the discovery strategy.
     * <p>
     * In general the discovery strategies can be at the level of Cloud Virtual Machines, for example, AWS EC2 Instance or GCP
     * Virtual Machine. They can also be at the level of some specific platform or framework, like Kubernetes.
     * <p>
     * It decides on the priority in the auto detection mechanism, because you can have a Kubernetes environment installed on
     * AWS EC2 Instances and then Hazelcast Kubernetes Discovery Strategy should take precedence over AWS Discovery Strategy.
     */
    enum DiscoveryStrategyLevel {
        UNKNOWN(0), CLOUD_VM(10), PLATFORM(20), CUSTOM(50);

        private int priority;

        DiscoveryStrategyLevel(int priority) {
            this.priority = priority;
        }

        public int getPriority() {
            return priority;
        }
    }
}
