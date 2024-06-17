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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.config.RoutingStrategy;

/**
 * <p>Client requests can be routed to members in one of 3 modes:</p>
 * <ul>
 *     <li>{@code UNISOCKET}: All requests are sent to a single member</li>
 *     <li>{@code SMART}: {@link com.hazelcast.client.config.ClientNetworkConfig#setSmartRouting}</li>
 *     <li>{@code SUBSET}: A request can be sent to a subset of members based on
 *     {@link RoutingStrategy}.</li>
 * </ul>
 * <p>The {@code UNKNOWN} enumeration represents a state where the client's
 * {@link RoutingMode} is not known, usually due to < 5.5 client versions</p>
 */
public enum RoutingMode {
    /**
     * Represents a single member routing mode
     */
    UNISOCKET(0),

    /**
     * Represents an all members routing mode
     */

    SMART(1),
    /**
     * Represents a multi member routing mode that does not connect to all members
     */
    SUBSET(2),

    /**
     * Represents an unknown routing mode, usually because of a pre-5.5 client
     * <b>Note: This mode should never be configured as a {@link RoutingMode}</b>
     */
    UNKNOWN(-1);

    private static final RoutingMode[] VALUES = values();

    private final int id;

    RoutingMode(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    /**
     * Returns whether this {@link RoutingMode} is a concrete enumeration
     * representing a real routing mode, or if it's a placeholder for an
     * unknown value that could not be parsed.
     *
     * @return {@code true} if this {@link RoutingMode} is a real mode, or {@code false} otherwise
     */
    public boolean isKnown() {
        return this != UNKNOWN;
    }

    public static RoutingMode getById(int id) {
        if (id >= 0 && id < VALUES.length - 1) {
            return VALUES[id];
        }
        return UNKNOWN;
    }
}
