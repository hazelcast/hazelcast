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
 *     <li>UNISOCKET: All requests are sent to a single member</li>
 *     <li>SMART: {@link com.hazelcast.client.config.ClientNetworkConfig#setSmartRouting}</li>
 *     <li>SUBSET: A request can be sent to a subset of members based on
 *     {@link RoutingStrategy}.</li>
 * </ul>
 */
public enum RoutingMode {
    UNISOCKET(0),
    SMART(1),
    SUBSET(2);

    private static final RoutingMode[] VALUES = values();

    private final int id;

    RoutingMode(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static RoutingMode getById(int id) {
        return VALUES[id];
    }
}
