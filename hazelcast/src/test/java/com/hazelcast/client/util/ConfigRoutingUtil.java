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

package com.hazelcast.client.util;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.RoutingMode;

/**
 * Utility for creating and modifying {@link ClientConfig} instances
 * based on {@link RoutingMode} quickly and easily.
 */
public class ConfigRoutingUtil {

    /**
     * Creates a new {@link ClientConfig} instance configured to use the
     * provided {@link RoutingMode}.
     *
     * @param routingMode the {@link RoutingMode} to use for this config
     * @return a new {@link ClientConfig} configured to the provided {@link RoutingMode}
     */
    public static ClientConfig newClientConfig(RoutingMode routingMode) {
        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.getClusterRoutingConfig().setRoutingMode(routingMode);
        return clientConfig;
    }
}
