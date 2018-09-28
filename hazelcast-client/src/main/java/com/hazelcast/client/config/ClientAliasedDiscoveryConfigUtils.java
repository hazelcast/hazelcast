/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InvalidConfigurationException;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Utility class for Aliased Discovery Configs in Hazelcast Client.
 */
public final class ClientAliasedDiscoveryConfigUtils {

    private ClientAliasedDiscoveryConfigUtils() {
    }

    public static List<DiscoveryStrategyConfig> createDiscoveryStrategyConfigs(ClientConfig config) {
        return AliasedDiscoveryConfigUtils.map(aliasedDiscoveryConfigsFrom(config));
    }

    public static List<AliasedDiscoveryConfig<?>> aliasedDiscoveryConfigsFrom(ClientConfig config) {
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        return asList(networkConfig.getAwsConfig(), networkConfig.getGcpConfig(), networkConfig.getAzureConfig(),
                networkConfig.getKubernetesConfig(), networkConfig.getEurekaConfig());
    }

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

    public static AliasedDiscoveryConfig newAliasedDiscoveryConfig(String name) {
        if ("aws".equals(name)) {
            return new ClientAwsConfig();
        } else {
            return AliasedDiscoveryConfigUtils.newConfigFor(name);
        }
    }
}
