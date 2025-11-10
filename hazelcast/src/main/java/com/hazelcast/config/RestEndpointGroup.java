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

package com.hazelcast.config;

import com.hazelcast.config.rest.RestConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum of REST endpoint groups. A REST group is predefined set of REST endpoints which can be enabled or disabled. Groups don't
 * overlap - each Hazelcast REST endpoint belongs to exactly one group. Each group has a default value
 * ({@link #isEnabledByDefault()}) which controls if it will be included by default in {@link RestApiConfig} configuration.
 *
 * @deprecated use RestConfig instead. Will be removed at 6.0.
 * @see RestConfig
 */
@Deprecated(since = "5.5", forRemoval = true)
public enum RestEndpointGroup {

    /**
     * Group of operations for retrieving cluster state and its version.
     */
    CLUSTER_READ(0, true),
    /**
     * Operations which changes cluster or node state or their configurations.
     */
    CLUSTER_WRITE(1, false),
    /**
     * Group of endpoints for HTTP health checking.
     */
    HEALTH_CHECK(2, true),
    /**
     * Group of HTTP REST APIs related to Persistence feature.
     */
    PERSISTENCE(3, false),
    /**
     * @deprecated
     * Please use {@link #PERSISTENCE} instead.
     * If this deprecated endpoint group is tried to be activated, we apply this
     * change as it's made to RestEndpointGroup#PERSISTENCE.
     */
    @Deprecated(since = "5.0")
    HOT_RESTART(3, false),
    /**
     * Group of HTTP REST APIs related to WAN Replication feature.
     */
    WAN(4, false),
    /**
     * Group of HTTP REST APIs for data manipulation in the cluster (e.g. IMap and IQueue operations).
     */
    DATA(5, false),

    /**
     * Groups of HTTP REST APIs for CP subsystem interaction
     */
    CP(6, false);


    private static final Map<Integer, RestEndpointGroup> CODE_TO_ENDPOINT_GROUPS_MAP =
            new HashMap<>();

    static {
        for (RestEndpointGroup group : RestEndpointGroup.values()) {
            if (group != HOT_RESTART) {
                CODE_TO_ENDPOINT_GROUPS_MAP.put(group.getCode(), group);
            }
        }
    }

    private final boolean enabledByDefault;
    private final int code;

    RestEndpointGroup(int code, boolean enabledByDefault) {
        this.code = code;
        this.enabledByDefault = enabledByDefault;
    }

    public static Collection<RestEndpointGroup> getAllEndpointGroups() {
        return CODE_TO_ENDPOINT_GROUPS_MAP.values();
    }

    /**
     * Returns if this group is enabled by default.
     */
    public boolean isEnabledByDefault() {
        return enabledByDefault;
    }

    public int getCode() {
        return code;
    }

    public static RestEndpointGroup getRestEndpointGroup(int code) {
        return CODE_TO_ENDPOINT_GROUPS_MAP.get(code);
    }
}
