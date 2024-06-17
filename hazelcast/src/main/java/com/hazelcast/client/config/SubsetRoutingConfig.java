/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Config for routing client connections to subset of cluster members.
 *
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
public class SubsetRoutingConfig {

    /**
     * Default {@link RoutingStrategy}
     */
    public static final RoutingStrategy DEFAULT_ROUTING_STRATEGY = RoutingStrategy.PARTITION_GROUPS;

    private boolean enabled;
    private RoutingStrategy routingStrategy = DEFAULT_ROUTING_STRATEGY;

    public SubsetRoutingConfig() {
    }

    public SubsetRoutingConfig(SubsetRoutingConfig subsetRoutingConfig) {
        this.enabled = subsetRoutingConfig.enabled;
        this.routingStrategy = subsetRoutingConfig.routingStrategy;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public SubsetRoutingConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns the strategy for routing client connections to members.
     * @return the configured strategy.
     */
    public RoutingStrategy getRoutingStrategy() {
        return routingStrategy;
    }

    /**
     * Sets the strategy for routing client connections to members.
     * @param routingStrategy the strategy to set.
     * @return this configuration.
     */
    public SubsetRoutingConfig setRoutingStrategy(RoutingStrategy routingStrategy) {
        this.routingStrategy = routingStrategy;
        return this;
    }
}
