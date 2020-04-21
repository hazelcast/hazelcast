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
 * Config class to configure multiple client configs to be used by single client instance
 * The client will try to connect them in given order.
 * When the connected cluster fails or the client blacklisted from the cluster via the management center, the client will
 * search for alternative clusters with given configs.
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

    public ClientFailoverConfig addClientConfig(ClientConfig clientConfig) {
        validateClientConfig(clientConfig);
        clientConfigs.add(clientConfig);
        return this;
    }

    public ClientFailoverConfig setTryCount(int tryCount) {
        this.tryCount = tryCount;
        return this;
    }

    public List<ClientConfig> getClientConfigs() {
        return clientConfigs;
    }

    public ClientFailoverConfig setClientConfigs(List<ClientConfig> clientConfigs) {
        clientConfigs.forEach(this::validateClientConfig);
        this.clientConfigs = clientConfigs;
        return this;
    }

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
