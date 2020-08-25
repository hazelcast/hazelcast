/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.config.override.PropertiesToNodeConverter.propsToNode;
import static java.lang.String.format;

/**
 * A class used to process external configuration overrides coming from environment variables/system properties,
 * and applying them over existing {@link Config}/{@link ClientConfig} configuration
 */
public class ExternalConfigurationOverride {

    private static final ILogger LOGGER = Logger.getLogger(ExternalConfigurationOverride.class);

    public Config overwriteMemberConfig(Config config) {
        return overwrite(config, (provider, c) -> {
              try {
                  new YamlMemberDomConfigProcessor(true, c)
                    .buildConfig(new ConfigOverrideElementAdapter(propsToNode(provider.properties())));
              } catch (Exception e) {
                  throw new InvalidConfigurationException("failed to overwrite configuration coming from " + provider.name(), e);
              }
          },
          new EnvConfigProvider(EnvVariablesConfigParser.member()),
          new SystemPropertiesConfigProvider(SystemPropertiesConfigParser.member()));
    }

    public ClientConfig overwriteClientConfig(ClientConfig config) {
        return overwrite(config, (provider, c) -> {
              try {
                  new YamlClientDomConfigProcessor(true, c)
                    .buildConfig(new ConfigOverrideElementAdapter(propsToNode(provider.properties())));
              } catch (Exception e) {
                  throw new InvalidConfigurationException("failed to overwrite configuration coming from " + provider.name());
              }
          },
          new EnvConfigProvider(EnvVariablesConfigParser.client()),
          new SystemPropertiesConfigProvider(SystemPropertiesConfigParser.client()));
    }

    private <T> T overwrite(T config, BiConsumer<ConfigProvider, T> configProcessor, ConfigProvider... providers) {
        ConfigOverrideValidator.validate(new HashSet<>(Arrays.asList(providers)));

        for (ConfigProvider configProvider : providers) {
            Map<String, String> properties = configProvider.properties();
            properties.forEach((k, v) -> {
                LOGGER.info(format("Detected external configuration overrides in %s: [%s]",
                  configProvider.name(),
                  k + "=" + v));
            });

            if (!properties.isEmpty()) {
                configProcessor.accept(configProvider, config);
            }
        }

        return config;
    }
}
