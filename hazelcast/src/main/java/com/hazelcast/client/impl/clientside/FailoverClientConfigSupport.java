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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;

import java.util.List;

/**
 * Static methods to resolve and validate multiple client configs for blue green feature
 */
public final class FailoverClientConfigSupport {

    private FailoverClientConfigSupport() {
    }

    /**
     * Loads failover configuration using the search logic described in
     * {@link HazelcastClient#newHazelcastFailoverClient()} and builds
     * {@link ClientFailoverConfig}.
     *
     * @return resolvedConfigs
     * @throws HazelcastException            if no failover configuration is found
     * @throws InvalidConfigurationException when given config is not valid
     */
    public static ClientFailoverConfig resolveClientFailoverConfig() {
        return resolveClientFailoverConfig(null);
    }

    /**
     * If {@code clientFailoverConfig} passed {@code null}, then loads
     * failover configuration using the search logic described in
     * {@link HazelcastClient#newHazelcastFailoverClient()} and builds
     * {@link ClientFailoverConfig}.
     *
     * @param clientFailoverConfig provided via {@link HazelcastClient#newHazelcastFailoverClient(ClientFailoverConfig)}
     * @return resolvedConfigs
     * @throws HazelcastException            if {@code clientFailoverConfig} is {@code null} and
     *                                       no failover configuration is found
     * @throws InvalidConfigurationException when given config is not valid
     */
    public static ClientFailoverConfig resolveClientFailoverConfig(ClientFailoverConfig clientFailoverConfig) {
        if (clientFailoverConfig == null) {
            clientFailoverConfig = ClientFailoverConfig.load();
        }
        checkValidAlternative(clientFailoverConfig.getClientConfigs());
        return clientFailoverConfig;
    }

    /**
     * If clientConfig is {@code null}, the client configuration is loaded
     * using the resolution logic as described in {@link HazelcastClient#newHazelcastClient()}
     * <p/>
     * Used with
     * {@link HazelcastClient#newHazelcastClient} or
     * {@link HazelcastClient#newHazelcastClient(ClientConfig)}
     *
     * @param config provided via {@link HazelcastClient#newHazelcastClient(ClientConfig)}
     * @return resolvedConfigs
     * @throws InvalidConfigurationException when given config is not valid
     */
    public static ClientConfig resolveClientConfig(ClientConfig config) {
        if (config == null) {
            return ClientConfig.load();
        }
        return config;
    }

    /**
     * For a client to be valid alternative, all configurations should be equal except
     * Cluster name
     * SecurityConfig
     * Discovery related parts of NetworkConfig
     * Credentials related configs
     *
     * @param alternativeClientConfigs to check if they are valid alternative for a single client two switch between clusters
     * @throws InvalidConfigurationException when given configs are not valid
     */
    private static void checkValidAlternative(List<ClientConfig> alternativeClientConfigs) {
        if (alternativeClientConfigs.isEmpty()) {
            throw new InvalidConfigurationException("ClientFailoverConfig should have at least one client config.");
        }
        ClientConfig mainConfig = alternativeClientConfigs.get(0);
        for (ClientConfig alternativeClientConfig : alternativeClientConfigs.subList(1, alternativeClientConfigs.size())) {
            checkValidAlternative(mainConfig, alternativeClientConfig);
        }

    }

    private static void throwInvalidConfigurationException(String rootClusterName, String clusterName, String configName) {
        throw new InvalidConfigurationException("Alternative config with cluster name " + clusterName
                + " has a different config than the initial config with cluster name " + rootClusterName + " for " + configName);
    }

    /**
     * @return false when both objects are null or they are equal, true otherwise
     */
    private static boolean notEqual(Object l, Object r) {
        return l != null ? !l.equals(r) : r != null;
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:methodlength"})
    private static void checkValidAlternative(ClientConfig mainConfig, ClientConfig alternativeConfig) {
        String mainClusterName = mainConfig.getClusterName();
        String alterNativeClusterName = alternativeConfig.getClusterName();

        checkValidAlternativeForNetwork(mainConfig, alternativeConfig);

        if (notEqual(mainConfig.getProperties(), alternativeConfig.getProperties())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "properties");
        }
        if (notEqual(mainConfig.getLoadBalancer(), alternativeConfig.getLoadBalancer())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "loadBalancer");
        }
        if (notEqual(mainConfig.getListenerConfigs(), alternativeConfig.getListenerConfigs())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "listeners");
        }
        if (notEqual(mainConfig.getInstanceName(), alternativeConfig.getInstanceName())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "instanceName");
        }
        if (notEqual(mainConfig.getConfigPatternMatcher(), alternativeConfig.getConfigPatternMatcher())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "configPatternMatcher");
        }
        if (notEqual(mainConfig.getNearCacheConfigMap(), alternativeConfig.getNearCacheConfigMap())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "nearCache");
        }
        if (notEqual(mainConfig.getReliableTopicConfigMap(), alternativeConfig.getReliableTopicConfigMap())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "reliableTopic");
        }
        if (notEqual(mainConfig.getQueryCacheConfigs(), alternativeConfig.getQueryCacheConfigs())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "queryCacheConfigs");
        }
        if (notEqual(mainConfig.getSerializationConfig(), alternativeConfig.getSerializationConfig())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "serializationConfig");
        }
        if (notEqual(mainConfig.getNativeMemoryConfig(), alternativeConfig.getNativeMemoryConfig())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "nativeMemory");
        }
        if (notEqual(mainConfig.getProxyFactoryConfigs(), alternativeConfig.getProxyFactoryConfigs())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "proxyFactory");
        }
        if (notEqual(mainConfig.getManagedContext(), alternativeConfig.getManagedContext())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "managedContext");
        }
        if (notEqual(mainConfig.getClassLoader(), alternativeConfig.getClassLoader())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "classLoader");
        }
        if (notEqual(mainConfig.getConnectionStrategyConfig(), alternativeConfig.getConnectionStrategyConfig())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "connectionStrategy");
        }
        if (notEqual(mainConfig.getUserCodeDeploymentConfig(), alternativeConfig.getUserCodeDeploymentConfig())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "userCodeDeployment");
        }
        if (notEqual(mainConfig.getFlakeIdGeneratorConfigMap(), alternativeConfig.getFlakeIdGeneratorConfigMap())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "flakeIdGenerator");
        }
        if (notEqual(mainConfig.getLabels(), alternativeConfig.getLabels())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "labels");
        }
        if (notEqual(mainConfig.getUserContext(), alternativeConfig.getUserContext())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "userContext");
        }
        if (notEqual(mainConfig.getMetricsConfig(), alternativeConfig.getMetricsConfig())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "metricsConfig");
        }
        if (mainConfig.isBackupAckToClientEnabled() != alternativeConfig.isBackupAckToClientEnabled()) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "isBackupAckToClientEnabled");
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:methodlength"})
    private static void checkValidAlternativeForNetwork(ClientConfig mainConfig, ClientConfig alternativeConfig) {
        String mainClusterName = mainConfig.getClusterName();
        String alterNativeClusterName = alternativeConfig.getClusterName();

        ClientNetworkConfig mainNetworkConfig = mainConfig.getNetworkConfig();
        ClientNetworkConfig alternativeNetworkConfig = alternativeConfig.getNetworkConfig();

        if (mainNetworkConfig == null && alternativeNetworkConfig == null) {
            return;
        }

        if (mainNetworkConfig == null || alternativeNetworkConfig == null) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "network");
        }

        if (mainNetworkConfig.isSmartRouting() != alternativeNetworkConfig.isSmartRouting()) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "network:smartRouting");
        }
        if (mainNetworkConfig.isRedoOperation() != alternativeNetworkConfig.isRedoOperation()) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "network:redoOperation");
        }
        if (mainNetworkConfig.getConnectionTimeout() != alternativeNetworkConfig.getConnectionTimeout()) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "network:connectionTimeout");
        }
        if (notEqual(mainNetworkConfig.getSocketOptions(), alternativeNetworkConfig.getSocketOptions())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "network:socketOptions");
        }
        if (notEqual(mainNetworkConfig.getOutboundPortDefinitions(), alternativeNetworkConfig.getOutboundPortDefinitions())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "network:outboundPortDefinitions");
        }
        if (notEqual(mainNetworkConfig.getOutboundPorts(), alternativeNetworkConfig.getOutboundPorts())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "network:smartRouting");
        }
        if (notEqual(mainNetworkConfig.getClientIcmpPingConfig(), alternativeNetworkConfig.getClientIcmpPingConfig())) {
            throwInvalidConfigurationException(mainClusterName, alterNativeClusterName, "network:clientIcmp");
        }
    }
}
