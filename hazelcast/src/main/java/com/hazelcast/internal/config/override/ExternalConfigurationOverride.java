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
package com.hazelcast.internal.config.override;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.impl.YamlClientDomConfigProcessor;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.YamlMemberDomConfigProcessor;
import com.hazelcast.internal.config.override.SystemPropertiesConfigProvider.SystemPropertiesProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import static com.hazelcast.internal.config.LicenseKey.maskLicense;
import static com.hazelcast.internal.config.override.PropertiesToNodeConverter.propsToNode;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * A class used to process external configuration overrides coming from environment variables/system properties,
 * and applying them over existing {@link Config}/{@link ClientConfig} configuration.
 */
public class ExternalConfigurationOverride {


    private static final ILogger LOGGER = Logger.getLogger(ExternalConfigurationOverride.class);
    private final Map<String, String> envVariables;
    private final SystemPropertiesProvider systemPropertiesProvider;

    public ExternalConfigurationOverride() {
        this(System.getenv(), System::getProperties);
    }

    /**
     * Used externally only for testing
     */
    ExternalConfigurationOverride(Map<String, String> envVariables, SystemPropertiesProvider systemPropertiesProvider) {
        this.envVariables = envVariables;
        this.systemPropertiesProvider = systemPropertiesProvider;
    }

    public Config overwriteMemberConfig(Config config) {
        return overwrite(config, (provider, rootNode, target) -> {
                    try {
                        new YamlMemberDomConfigProcessor(true, target, false)
                                .buildConfig(new ConfigOverrideElementAdapter(rootNode));
                    } catch (Exception e) {
                        throw new InvalidConfigurationException("failed to overwrite configuration coming from " + provider, e);
                    }
                },
                new EnvConfigProvider(EnvVariablesConfigParser.member(), envVariables),
                new SystemPropertiesConfigProvider(SystemPropertiesConfigParser.member(), systemPropertiesProvider));
    }

    public ClientConfig overwriteClientConfig(ClientConfig config) {
        return overwrite(config, (provider, rootNode, target) -> {
                    try {
                        new YamlClientDomConfigProcessor(true, target, false)
                                .buildConfig(new ConfigOverrideElementAdapter(rootNode));
                    } catch (Exception e) {
                        throw new InvalidConfigurationException("failed to overwrite configuration coming from " + provider, e);
                    }
                },
                new EnvConfigProvider(EnvVariablesConfigParser.client(), envVariables),
                new SystemPropertiesConfigProvider(SystemPropertiesConfigParser.client(), systemPropertiesProvider));
    }

    private <T> T overwrite(T config, ConfigConsumer<T> configProcessor, ConfigProvider... providers) {
        ConfigOverrideValidator.validate(new HashSet<>(Arrays.asList(providers)));

        for (ConfigProvider configProvider : providers) {
            Map<String, String> properties = configProvider.properties();

            if (!properties.isEmpty()) {

                ConfigNode rootNode = propsToNode(properties);
                configProcessor.apply(configProvider.name(), rootNode, config);

                Map<String, String> unprocessed = new ConfigNodeStateTracker().unprocessedNodes(rootNode);

                LOGGER.info(format("Detected external configuration entries in %s: [%s]",
                  configProvider.name(),
                  properties.entrySet().stream()
                    .filter(e -> !unprocessed.containsKey(e.getKey()))
                    .map(e -> {
                        if (e.getKey().equals("hazelcast.licensekey")) {
                            return e.getKey() + "=" + maskLicense(e.getValue());
                        } else {
                            return e.getKey() + "=" + e.getValue();
                        }
                    })
                    .collect(joining(","))));

                if (!unprocessed.isEmpty()) {
                    LOGGER.warning(format("Unrecognized %s configuration entries: %s",
                      configProvider.name(),
                      unprocessed.keySet()));
                }
            }
        }

        return config;
    }

    @FunctionalInterface
    interface ConfigConsumer<T> {
        void apply(String providerName, ConfigNode config, T target);
    }
}
