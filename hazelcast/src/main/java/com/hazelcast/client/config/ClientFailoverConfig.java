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

package com.hazelcast.client.config;

import com.hazelcast.client.config.impl.XmlClientFailoverConfigLocator;
import com.hazelcast.client.config.impl.YamlClientFailoverConfigLocator;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;

import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_FAILOVER_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.validateSuffixInSystemProperty;

/**
 * Config class to configure multiple client configs to be used by single client instance.
 * The client will try to connect them in given order. When the connected cluster fails
 * or the client blacklisted from the cluster via the Management Center, the client will
 * search for alternative clusters with given configs.
 * <p>
 * The client configurations must be exactly the same except the following configuration options:
 * <ul>
 * <li>{@code clusterName}</li>
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
     * Sets the count that the client iterates over the given list of alternative clusters.
     * For each alternative cluster, the client will try to connect to the cluster
     * respecting related ConnectionRetryConfig.
     * <p>
     * When the client can not connect a cluster, it will try to connect tryCount times going
     * over the alternative client configs in a round-robin fashion. This is triggered at the
     * start and also when the client disconnects from the cluster and can not connect back
     * to it by exhausting attempts described in ConnectionRetryConfig. In that case,
     * the client will continue from where it is left off in ClientConfig lists, and try
     * the next one again in round-robin tryCount X number of alternative cluster times.
     * <p>
     * Example:
     * Assume that 3 alternative clusters are given in the ClientConfig list.
     * Let's call them cluster A , B and C .
     * When the tryCount is set as 2, the maximum number of subsequent connection attempts done
     * by the client is 3 x 2 = 6.
     * They will be tried in the following order as long as the client can not connect to any of them:
     * A -> B -> C -> A -> B -> C -> Client Shutdown.
     * Trial 1     ->  Trial 2    -> Client Shutdown.
     * <p>
     * Assume that client was connected to cluster B, and then it is disconnected from it. In that case, the trial
     * oder will be as follows:
     * C -> A -> B -> C -> A -> B -> Client Shutdown.
     * Trial 1     ->  Trial 2    -> Client Shutdown.
     *
     * @param tryCount the count of attempts
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
     * Sets the count that the client iterates over the given list of alternative clusters.
     * For each alternative cluster, the client will try to connect to the cluster
     * respecting related ConnectionRetryConfig.
     * Default value is {@code Integer.MAX_VALUE}.
     *
     * @return the max try count
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
