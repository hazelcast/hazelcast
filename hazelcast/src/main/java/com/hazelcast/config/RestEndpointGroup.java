/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

/**
 * Enum of REST endpoint groups. A REST group is predefined set of REST endpoints which can be enabled or disabled. Groups don't
 * overlap - each Hazelcast REST endpoint belongs to exactly one group. Each group has a default value
 * ({@link #isEnabledByDefault()}) which controls if it will be included by default in {@link RestApiConfig} configuration.
 *
 * @see RestApiConfig
 */
public enum RestEndpointGroup {

    /**
     * Group of operations for retrieving cluster state and its version.
     */
    CLUSTER_READ(true),
    /**
     * Operations which changes cluster or node state or their configurations.
     */
    CLUSTER_WRITE(false),
    /**
     * Group of endpoints for HTTP health checking.
     */
    HEALTH_CHECK(true),
    /**
     * Group of HTTP REST APIs related to Hot Restart feature.
     */
    HOT_RESTART(false),
    /**
     * Group of HTTP REST APIs related to WAN Replication feature.
     */
    WAN(false),
    /**
     * Group of HTTP REST APIs for data manipulation in the cluster (e.g. IMap and IQueue operations).
     */
    DATA(false);

    private final boolean enabledByDefault;

    RestEndpointGroup(boolean enabledByDefault) {
        this.enabledByDefault = enabledByDefault;
    }

    /**
     * Returns if this group is enabled by default.
     */
    public boolean isEnabledByDefault() {
        return enabledByDefault;
    }
}
