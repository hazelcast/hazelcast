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

package com.hazelcast.client.config;

import com.hazelcast.client.config.impl.XmlClientFailoverConfigLocator;
import com.hazelcast.client.config.impl.YamlClientFailoverConfigLocator;
import com.hazelcast.core.HazelcastException;

import com.hazelcast.config.InvalidConfigurationException;

import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_FAILOVER_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.validateSuffixInSystemProperty;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;

/**
 * Config class to configure multiple client configs to be used by single client instance.
 * The client will try to connect them in given order. When the connected cluster fails
 * or the client blacklisted from the cluster via the Management Center, the client will
 * search for alternative clusters with given configs.
 * <p>
 * The client configurations must be exactly the same except the following configuration options:
 * <ul>
 * <li>{@code SecurityConfig}</li>
 * <li>{@code NetworkConfig.Addresses}</li>
 * <li>{@code NetworkConfig.SocketInterceptorConfig}</li>
 * <li>{@code NetworkConfig.SSLConfig}</li>
 * <li>{@code NetworkConfig.AwsConfig}</li>
 * <li>{@code NetworkConfig.GcpConfig}</li>
 * <li>{@code NetworkConfig.AzureConfig}</li>
 * <li>{@code NetworkConfig.KubernetesConfig}</li>
 * <li>{@code NetworkConfig.EurekaConfig}</li>
 * <li>{@code NetworkConfig.CloudConfig}</li>
 * <li>{@code NetworkConfig.DiscoveryConfig}</li>
 * </ul>
 */
public class ClientFailoverConfig {

    private int tryCount = Integer.MAX_VALUE;
    private List<ClientConfig> clientConfigs = new LinkedList<>();

    public ClientFailoverConfig() {
    }

    /**
     * Populates Hazelcast {@link ClientFailoverConfig} object from an external configuration file.
     * <p>
     * It tries to load Hazelcast Failover Client configuration from a list of well-known locations.
     * When no location contains Hazelcast Failover Client configuration then it returns default.
     * <p>
     * Note that the same mechanism is used when calling
     * {@link com.hazelcast.client.HazelcastClient#newHazelcastFailoverClient()}.
     *
     * @return ClientFailoverConfig created from a file when exists, otherwise default.
     */
    public static ClientFailoverConfig load() {
        validateSuffixInSystemProperty(SYSPROP_CLIENT_FAILOVER_CONFIG);

        XmlClientFailoverConfigLocator xmlConfigLocator = new XmlClientFailoverConfigLocator();
        YamlClientFailoverConfigLocator yamlConfigLocator = new YamlClientFailoverConfigLocator();

        if (xmlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading XML config from the configuration provided in system property
            return new XmlClientFailoverConfigBuilder(xmlConfigLocator).build();
        } else if (yamlConfigLocator.locateFromSystemProperty()) {
            // 2. Try loading YAML config from the configuration provided in system property
            return new YamlClientFailoverConfigBuilder(yamlConfigLocator).build();
        } else if (xmlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 3. Try loading XML config from the working directory or from the classpath
            return new XmlClientFailoverConfigBuilder(xmlConfigLocator).build();
        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 4. Try loading YAML config from the working directory or from the classpath
            return new YamlClientFailoverConfigBuilder(yamlConfigLocator).build();
        } else {
            throw new HazelcastException("Failed to load ClientFailoverConfig");
        }
    }

    /**
     * Adds the client config to the end of the alternative client configurations list.
     *
     * @param clientConfig the ClientConfig to add
     * @return this for chaining
     */
    public ClientFailoverConfig addClientConfig(ClientConfig clientConfig) {
        validateClientConfig(clientConfig);
        clientConfigs.add(clientConfig);
        return this;
    }

    /**
     * Sets the count of connection retries by the client to the alternative clusters.
     * <p>
     * When this value is reached and the client still could not connect to a cluster,
     * the client shuts down. Note that this value applies to the alternative clusters
     * whose configurations are provided with the client element.
     *
     * @param tryCount the number of attempts
     * @return this for chaining
     */
    public ClientFailoverConfig setTryCount(int tryCount) {
        this.tryCount = tryCount;
        return this;
    }

    /**
     * Gets the configured list of alternative client configurations.
     *
     * @return the list of configured ClientConfigs
     */
    public List<ClientConfig> getClientConfigs() {
        return clientConfigs;
    }

    /**
     * Sets the configured list of alternative client configurations.
     * <p>
     * Note: this method replaces previously configured alternative client
     * configurations with the given list.
     *
     * @param clientConfigs the list of ClientConfigs to be used
     * @return this for chaining
     */
    public ClientFailoverConfig setClientConfigs(List<ClientConfig> clientConfigs) {
        clientConfigs.forEach(this::validateClientConfig);
        this.clientConfigs = clientConfigs;
        return this;
    }

    /**
     * Returns the count of connection retries by the client to the alternative clusters.
     *
     * @return the count of connection retries
     */
    public int getTryCount() {
        return tryCount;
    }

    @Override
    public String toString() {
        return "ClientFailoverConfig{"
                + "tryCount=" + tryCount
                + ", clientConfigs=" + clientConfigs
                + '}';
    }

    private void validateClientConfig(ClientConfig clientConfig) {
        if (clientConfig.getConnectionStrategyConfig().getReconnectMode() == OFF) {
            throw new InvalidConfigurationException("Reconnect mode for ClientFailoverConfig must not be OFF");
        }
    }
}
