/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config.impl;

import com.hazelcast.client.config.ClientAltoConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;

public final class ClientConfigHelper {

    private ClientConfigHelper() {
    }

    /**
     * Returns whether the unisocket mode is enabled by taking
     * {@link ClientAltoConfig#isEnabled()} and {@link ClientNetworkConfig#isSmartRouting()}
     * into consideration.
     * <p>
     * When Alto configuration is enabled, it will override the value
     * selected in the network configuration, no matter what it is.
     *
     * @param config to consider
     * @return {@code true} is the unisocket mode is enabled, {@code false} otherwise
     */
    public static boolean unisocketModeConfigured(ClientConfig config) {
        if (config.getAltoConfig().isEnabled()) {
            return false;
        }

        return !config.getNetworkConfig().isSmartRouting();
    }
}
