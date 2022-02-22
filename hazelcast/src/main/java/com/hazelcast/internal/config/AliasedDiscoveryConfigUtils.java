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

package com.hazelcast.internal.config;

import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.config.WanBatchPublisherConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Utility class for Aliased Discovery Configs.
 */
public final class AliasedDiscoveryConfigUtils {
    private static final Map<String, String> ALIAS_MAPPINGS = new HashMap<>();

    private AliasedDiscoveryConfigUtils() {
    }

    static {
        ALIAS_MAPPINGS.put("aws", "com.hazelcast.aws.AwsDiscoveryStrategy");
        ALIAS_MAPPINGS.put("gcp", "com.hazelcast.gcp.GcpDiscoveryStrategy");
        ALIAS_MAPPINGS.put("azure", "com.hazelcast.azure.AzureDiscoveryStrategy");
        ALIAS_MAPPINGS.put("kubernetes", "com.hazelcast.kubernetes.HazelcastKubernetesDiscoveryStrategy");
        ALIAS_MAPPINGS.put("eureka", "com.hazelcast.eureka.one.EurekaOneDiscoveryStrategy");
    }

    /**
     * Returns the collection of supported tags.
     */
    public static Collection<String> getTags() {
        return ALIAS_MAPPINGS.keySet();
    }

    /**
     * Checks whether the given XML {@code tag} is supported.
     */
    public static boolean supports(String tag) {
        return ALIAS_MAPPINGS.containsKey(tag);
    }

    /**
     * Returns an XML tag (e.g. {@literal <gcp>}) for the given config.
     */
    public static String tagFor(AliasedDiscoveryConfig config) {
        return config.getTag();
    }

    /**
     * Extracts aliased discovery configs from {@code config} and creates a list of {@link DiscoveryStrategyConfig} out of them.
     */
    public static List<DiscoveryStrategyConfig> createDiscoveryStrategyConfigs(JoinConfig config) {
        return map(aliasedDiscoveryConfigsFrom(config));
    }

    /**
     * Extracts aliased discovery configs from {@code config} and creates a list of {@link DiscoveryStrategyConfig} out of them.
     */
    public static List<DiscoveryStrategyConfig> createDiscoveryStrategyConfigs(WanBatchPublisherConfig config) {
        return map(aliasedDiscoveryConfigsFrom(config));
    }

    /**
     * Maps aliased discovery strategy configs into discovery strategy configs.
     */
    public static List<DiscoveryStrategyConfig> map(List<AliasedDiscoveryConfig<?>> aliasedDiscoveryConfigs) {
        List<DiscoveryStrategyConfig> result = new ArrayList<>();
        for (AliasedDiscoveryConfig config : aliasedDiscoveryConfigs) {
            if (config.isEnabled()) {
                result.add(createDiscoveryStrategyConfig(config));
            }
        }
        return result;
    }

    private static DiscoveryStrategyConfig createDiscoveryStrategyConfig(AliasedDiscoveryConfig<?> config) {
        validateConfig(config);

        String className = discoveryStrategyFrom(config);
        Map<String, Comparable> properties = new HashMap<>();
        for (String key : config.getProperties().keySet()) {
            putIfKeyNotNull(properties, key, config.getProperties().get(key));
        }
        return new DiscoveryStrategyConfig(className, properties);
    }

    private static void validateConfig(AliasedDiscoveryConfig config) {
        if (!ALIAS_MAPPINGS.containsKey(config.getTag())) {
            throw new InvalidConfigurationException(
                    String.format("Invalid configuration class: '%s'", config.getClass().getName()));
        }
    }

    private static String discoveryStrategyFrom(AliasedDiscoveryConfig config) {
        return ALIAS_MAPPINGS.get(config.getTag());
    }

    private static void putIfKeyNotNull(Map<String, Comparable> properties, String key, String value) {
        if (key != null) {
            properties.put(key, value);
        }
    }

    /**
     * Gets the {@link AliasedDiscoveryConfig} from {@code config} by {@code tag}.
     */
    public static AliasedDiscoveryConfig getConfigByTag(JoinConfig config, String tag) {
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
            throw new IllegalArgumentException(String.format("Invalid tag: '%s'", tag));
        }
    }

    /**
     * Gets the {@link AliasedDiscoveryConfig} from {@code config} by {@code tag}.
     */
    public static AliasedDiscoveryConfig getConfigByTag(WanBatchPublisherConfig config, String tag) {
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
            throw new IllegalArgumentException(String.format("Invalid tag: '%s'", tag));
        }
    }

    /**
     * Gets a list of all aliased discovery configs from {@code config}.
     */
    public static List<AliasedDiscoveryConfig<?>> aliasedDiscoveryConfigsFrom(JoinConfig config) {
        return asList(config.getAwsConfig(), config.getGcpConfig(), config.getAzureConfig(), config.getKubernetesConfig(),
                config.getEurekaConfig());
    }

    /**
     * Gets a list of all aliased discovery configs from {@code config}.
     */
    public static List<AliasedDiscoveryConfig<?>> aliasedDiscoveryConfigsFrom(WanBatchPublisherConfig config) {
        return asList(config.getAwsConfig(), config.getGcpConfig(), config.getAzureConfig(), config.getKubernetesConfig(),
                config.getEurekaConfig());
    }

    /**
     * Checks whether all aliased discovery configs have the tag {@literal <use-public-ip>true</use-public-ip}.
     * <p>
     * Note that if no config is enabled, then the method returns {@literal false}.
     */
    public static boolean allUsePublicAddress(List<AliasedDiscoveryConfig<?>> configs) {
        boolean atLeastOneEnabled = false;
        for (AliasedDiscoveryConfig config : configs) {
            if (config.isEnabled()) {
                atLeastOneEnabled = true;
                if (!config.isUsePublicIp()) {
                    return false;
                }
            }
        }
        return atLeastOneEnabled;
    }

    /**
     * Creates new {@link AliasedDiscoveryConfig} by the given {@code tag}.
     */
    @SuppressWarnings("unchecked")
    public static AliasedDiscoveryConfig newConfigFor(String tag) {
        if ("aws".equals(tag)) {
            return new AwsConfig();
        } else if ("gcp".equals(tag)) {
            return new GcpConfig();
        } else if ("azure".equals(tag)) {
            return new AzureConfig();
        } else if ("kubernetes".equals(tag)) {
            return new KubernetesConfig();
        } else if ("eureka".equals(tag)) {
            return new EurekaConfig();
        } else {
            throw new IllegalArgumentException(String.format("Invalid tag: '%s'", tag));
        }
    }
}
