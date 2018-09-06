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

package com.hazelcast.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps Discovery Strategy aliases to Discovery Strategy configs.
 * <p>
 * For example, an XML configuration with {@literal <aws>} is mapped to the strategy class {@literal com.hazelcast.aws
 * .AwsDiscoveryStrategy}.
 * <p>
 * Note that the mapped strategies are only the ones that are officially supported by Hazelcast.
 */
public final class AliasedDiscoveryConfigMapper {
    private static final Map<String, String> ALIAS_MAPPINGS = new HashMap<String, String>();

    private AliasedDiscoveryConfigMapper() {
    }

    static {
        ALIAS_MAPPINGS.put("aws", "com.hazelcast.aws.AwsDiscoveryStrategy");
        ALIAS_MAPPINGS.put("gcp", "com.hazelcast.gcp.GcpDiscoveryStrategy");
        ALIAS_MAPPINGS.put("azure", "com.hazelcast.azure.AzureDiscoveryStrategy");
        ALIAS_MAPPINGS.put("kubernetes", "com.hazelcast.kubernetes.HazelcastKubernetesDiscoveryStrategy");
        ALIAS_MAPPINGS.put("eureka", "com.hazelcast.eureka.one.HazelcastKubernetesDiscoveryStrategy");
        ALIAS_MAPPINGS.put("zookeeper", "com.hazelcast.zookeeper.ZookeeperDiscoveryStrategy");
    }

    public static boolean supports(String envioronment) {
        return ALIAS_MAPPINGS.containsKey(envioronment);
    }

    public static List<DiscoveryStrategyConfig> map(List<AliasedDiscoveryConfig> aliasedDiscoveryConfigs) {
        List<DiscoveryStrategyConfig> result = new ArrayList<DiscoveryStrategyConfig>();
        for (AliasedDiscoveryConfig config : aliasedDiscoveryConfigs) {
            if (config.isEnabled()) {
                result.add(createDiscoveryStrategyConfig(config));
            }
        }
        return result;
    }

    private static DiscoveryStrategyConfig createDiscoveryStrategyConfig(AliasedDiscoveryConfig config) {
        validateConfig(config);

        String className = ALIAS_MAPPINGS.get(config.getEnvironment());
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        for (String key : config.getProperties().keySet()) {
            putIfKeyNotNull(properties, key, config.getProperties().get(key));
        }
        return new DiscoveryStrategyConfig(className, properties);
    }

    private static void validateConfig(AliasedDiscoveryConfig config) {
        if (!ALIAS_MAPPINGS.containsKey(config.getEnvironment())) {
            throw new InvalidConfigurationException(String.format("Unknown environment tag: '%s'", config.getEnvironment()));
        }
    }

    private static void putIfKeyNotNull(Map<String, Comparable> properties, String key, String value) {
        if (key != null) {
            properties.put(key, value);
        }
    }
}
