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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InvalidConfigurationException;

import java.util.List;

import java.util.Arrays;

/**
 * Utility class for Aliased Discovery Configs in Hazelcast Client.
 */
public final class ClientAliasedDiscoveryConfigUtils {

    private ClientAliasedDiscoveryConfigUtils() {
    }

    /**
     * Extracts aliased discovery configs from {@code config} and creates a list of {@link DiscoveryStrategyConfig} out of them.
     */
    public static List<DiscoveryStrategyConfig> createDiscoveryStrategyConfigs(ClientConfig config) {
        return AliasedDiscoveryConfigUtils.map(aliasedDiscoveryConfigsFrom(config.getNetworkConfig()));
    }

    /**
     * Gets the {@link AliasedDiscoveryConfig} from {@code config} by {@code tag}.
     */
    public static AliasedDiscoveryConfig getConfigByTag(ClientNetworkConfig config, String tag) {
        if ("aws".equals(tag)) {
            return config.getAwsConfig();
        } else if ("gcp".equals(tag)) {
            return config.getGcpConfig();
        } else if ("azure".equals(tag)) {
            return config.getAzureConfig();
        } else if ("kubernetes".equals(tag)) {
            return config.getKubernetesConfig();
        } else if ("eureka".equals(tag)) {
            return config.getEurekaConfig();
        } else {
            throw new InvalidConfigurationException(String.format("Invalid configuration tag: '%s'", tag));
        }
    }

    /**
     * Gets a list of all aliased discovery configs from {@code config}.
     */
    public static List<AliasedDiscoveryConfig<?>> aliasedDiscoveryConfigsFrom(ClientNetworkConfig networkConfig) {
        return Arrays.<AliasedDiscoveryConfig<?>>asList(networkConfig.getAwsConfig(), networkConfig.getGcpConfig(),
                networkConfig.getAzureConfig(), networkConfig.getKubernetesConfig(), networkConfig.getEurekaConfig());
    }

}
