/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.client.UnsupportedRoutingModeException;
import com.hazelcast.client.UnsupportedClusterVersionException;

import java.util.Objects;

/**
 * Config for client routing and associated options. The overall approach to routing used
 * by the client is defined by the {@link RoutingMode}, with three modes available:
 * <ul>
 *      <li>{@link RoutingMode#SINGLE_MEMBER}</li>
 *      <li>{@link RoutingMode#MULTI_MEMBER}</li>
 *      <li>{@link RoutingMode#ALL_MEMBERS}</li>
 * </ul>
 * <p>
 * <b>Special notes for {@link RoutingMode#MULTI_MEMBER} routing</b>
 * This feature requires the cluster members to be Enterprise nodes. If the cluster is not
 * licensed appropriately, the client will not be able to connect to the cluster,
 * failing with an {@link UnsupportedRoutingModeException}
 *
 * The minimum cluster version required for this feature is 5.5. If the cluster version is
 * less than 5.5, the client will not be able to connect to the cluster, failing with an
 * {@link UnsupportedClusterVersionException}
 *
 * @since 5.5
 */
public class ClusterRoutingConfig {
    /**
     * The default {@link RoutingStrategy} to use when one is not explicitly defined
     */
    public static final RoutingStrategy DEFAULT_ROUTING_STRATEGY = RoutingStrategy.PARTITION_GROUPS;
    /**
     * The default {@link RoutingMode} to use when one is not explicitly defined
     */
    public static final RoutingMode DEFAULT_ROUTING_MODE = RoutingMode.ALL_MEMBERS;

    /**
     * The main {@link RoutingMode} used for routing to the cluster.
     */
    private RoutingMode routingMode = DEFAULT_ROUTING_MODE;
    /**
     * The {@link RoutingStrategy} to use with the provided {@link RoutingMode}.
     * Currently, only {@link RoutingMode#MULTI_MEMBER} routing supports a strategy selection.
     */
    private RoutingStrategy routingStrategy = DEFAULT_ROUTING_STRATEGY;

    public ClusterRoutingConfig() {
    }

    public ClusterRoutingConfig(ClusterRoutingConfig clusterRoutingConfig) {
        this.routingMode = clusterRoutingConfig.routingMode;
        this.routingStrategy = clusterRoutingConfig.routingStrategy;
    }

    /**
     * Returns the defined {@link RoutingMode} for this client to use when connecting to cluster members.
     * @return the configured {@link RoutingMode}
     */
    public RoutingMode getRoutingMode() {
        return routingMode;
    }

    /**
     * Sets the {@link RoutingMode} for this client to use when connecting to cluster members.
     *
     * @param routingMode the legacy {@link RoutingMode} enumeration value to use
     * @return this configuration.
     *
     * @deprecated since 5.5.3, use {@link #setRoutingMode(RoutingMode)} instead.
     */
    @Deprecated(since = "5.5.3", forRemoval = true)
    public ClusterRoutingConfig setRoutingMode(com.hazelcast.client.impl.connection.tcp.RoutingMode routingMode) {
        this.routingMode = RoutingMode.valueOf(routingMode.name());
        return this;
    }

    /**
     * Sets the {@link RoutingMode} for this client to use when connecting to cluster members.
     * @param routingMode the {@link RoutingMode} to use
     * @return this configuration.
     */
    public ClusterRoutingConfig setRoutingMode(RoutingMode routingMode) {
        this.routingMode = routingMode;
        return this;
    }

    /**
     * Returns the strategy for routing client connections to members when {@link RoutingMode#MULTI_MEMBER} is configured.
     * @return the configured strategy.
     */
    public RoutingStrategy getRoutingStrategy() {
        return routingStrategy;
    }

    /**
     * Sets the strategy for routing client connections to members when {@link RoutingMode#MULTI_MEMBER} is configured.
     * @param routingStrategy the strategy to set.
     * @return this configuration.
     */
    public ClusterRoutingConfig setRoutingStrategy(RoutingStrategy routingStrategy) {
        this.routingStrategy = routingStrategy;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterRoutingConfig that = (ClusterRoutingConfig) o;
        return routingMode == that.routingMode && routingStrategy == that.routingStrategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(routingMode, routingStrategy);
    }

    @Override
    public String toString() {
        return "ClusterRoutingConfig{"
                + "routingMode=" + routingMode
                + ", routingStrategy=" + routingStrategy
                + '}';
    }
}
