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

package com.hazelcast.config;

import java.util.Collection;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Contains the multiple different join configuration. Only one of them should be enabled!
 */
public class JoinConfig {

    private MulticastConfig multicastConfig = new MulticastConfig();

    private TcpIpConfig tcpIpConfig = new TcpIpConfig();

    private AwsConfig awsConfig = new AwsConfig();

    private GcpConfig gcpConfig = new GcpConfig();

    private AzureConfig azureConfig = new AzureConfig();

    private KubernetesConfig kubernetesConfig = new KubernetesConfig();

    private EurekaConfig eurekaConfig = new EurekaConfig();

    private DiscoveryConfig discoveryConfig = new DiscoveryConfig();

    private AutoDetectionConfig autoDetectionConfig = new AutoDetectionConfig();

    /**
     * @return the multicastConfig join configuration
     */
    public MulticastConfig getMulticastConfig() {
        return multicastConfig;
    }

    /**
     * @param multicastConfig the multicastConfig join configuration to set
     * @throws IllegalArgumentException if multicastConfig is null
     */
    public JoinConfig setMulticastConfig(final MulticastConfig multicastConfig) {
        this.multicastConfig = isNotNull(multicastConfig, "multicastConfig");
        return this;
    }

    /**
     * @return the tcpIpConfig
     */
    public TcpIpConfig getTcpIpConfig() {
        return tcpIpConfig;
    }

    /**
     * @param tcpIpConfig the tcpIpConfig join configuration to set
     * @throws IllegalArgumentException if tcpIpConfig is null
     */
    public JoinConfig setTcpIpConfig(final TcpIpConfig tcpIpConfig) {
        this.tcpIpConfig = isNotNull(tcpIpConfig, "tcpIpConfig");
        return this;
    }

    /**
     * @return the awsConfig join configuration
     */
    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    /**
     * @param awsConfig the AwsConfig join configuration to set
     * @throws IllegalArgumentException if awsConfig is null
     */
    public JoinConfig setAwsConfig(final AwsConfig awsConfig) {
        this.awsConfig = isNotNull(awsConfig, "awsConfig");
        return this;
    }

    /**
     * @return the gcpConfig join configuration
     */
    public GcpConfig getGcpConfig() {
        return gcpConfig;
    }

    /**
     * @param gcpConfig the GcpConfig join configuration to set
     * @throws IllegalArgumentException if gcpConfig is null
     */
    public JoinConfig setGcpConfig(final GcpConfig gcpConfig) {
        this.gcpConfig = isNotNull(gcpConfig, "gcpConfig");
        return this;
    }

    /**
     * @return the azureConfig join configuration
     */
    public AzureConfig getAzureConfig() {
        return azureConfig;
    }

    /**
     * @param azureConfig the AzureConfig join configuration to set
     * @throws IllegalArgumentException if azureConfig is null
     */
    public JoinConfig setAzureConfig(final AzureConfig azureConfig) {
        this.azureConfig = isNotNull(azureConfig, "azureConfig");
        return this;
    }

    /**
     * @return the kubernetesConfig join configuration
     */
    public KubernetesConfig getKubernetesConfig() {
        return kubernetesConfig;
    }

    /**
     * @param kubernetesConfig the KubernetesConfig join configuration to set
     * @throws IllegalArgumentException if kubernetesConfig is null
     */
    public JoinConfig setKubernetesConfig(final KubernetesConfig kubernetesConfig) {
        this.kubernetesConfig = isNotNull(kubernetesConfig, "kubernetesConfig");
        return this;
    }

    /**
     * @return the eurekaConfig join configuration
     */
    public EurekaConfig getEurekaConfig() {
        return eurekaConfig;
    }

    /**
     * @param eurekaConfig the EurekaConfig join configuration to set
     * @throws IllegalArgumentException if eurekaConfig is null
     */
    public JoinConfig setEurekaConfig(final EurekaConfig eurekaConfig) {
        this.eurekaConfig = isNotNull(eurekaConfig, "eurekaConfig");
        return this;
    }

    /**
     * Returns the currently defined {@link DiscoveryConfig}
     *
     * @return current DiscoveryProvidersConfig instance
     */
    public DiscoveryConfig getDiscoveryConfig() {
        return discoveryConfig;
    }

    /**
     * Sets a custom defined {@link DiscoveryConfig}
     *
     * @param discoveryConfig configuration to set
     * @throws java.lang.IllegalArgumentException if discoveryProvidersConfig is null
     */
    public JoinConfig setDiscoveryConfig(DiscoveryConfig discoveryConfig) {
        this.discoveryConfig = isNotNull(discoveryConfig, "discoveryProvidersConfig");
        return this;
    }

    /**
     * @return the autoDetectionConfig join configuration
     */
    public AutoDetectionConfig getAutoDetectionConfig() {
        return autoDetectionConfig;
    }

    /**
     * @param autoDetectionConfig the autoDetectionConfig join configuration to set
     * @throws IllegalArgumentException if autoDetectionConfig is null
     */
    public JoinConfig setAutoDetectionConfig(final AutoDetectionConfig autoDetectionConfig) {
        this.autoDetectionConfig = isNotNull(autoDetectionConfig, "autoDetectionConfig");
        return this;
    }

    /**
     * Any other join configuration takes precedence over auto-discovery, so auto-discovery is enabled only when no other join
     * strategy is enabled.
     *
     * @return true if auto-detection is enabled
     */
    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public boolean isAutoDetectionEnabled() {
        return autoDetectionConfig.isEnabled()
                && !multicastConfig.isEnabled()
                && !tcpIpConfig.isEnabled()
                && !awsConfig.isEnabled()
                && !gcpConfig.isEnabled()
                && !azureConfig.isEnabled()
                && !kubernetesConfig.isEnabled()
                && !eurekaConfig.isEnabled()
                && !discoveryConfig.isEnabled();
    }

    /**
     * Verifies this JoinConfig is valid. At most a single joiner should be active.
     *
     * @throws InvalidConfigurationException when the join config is not valid
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    public void verify() {
        int countEnabled = 0;
        if (getTcpIpConfig().isEnabled()) {
            countEnabled++;
        }
        if (getMulticastConfig().isEnabled()) {
            countEnabled++;
        }
        if (getAwsConfig().isEnabled()) {
            countEnabled++;
        }
        if (getGcpConfig().isEnabled()) {
            countEnabled++;
        }
        if (getAzureConfig().isEnabled()) {
            countEnabled++;
        }
        if (getKubernetesConfig().isEnabled()) {
            countEnabled++;
        }
        if (getEurekaConfig().isEnabled()) {
            countEnabled++;
        }

        Collection<DiscoveryStrategyConfig> discoveryStrategyConfigs = discoveryConfig.getDiscoveryStrategyConfigs();
        countEnabled += discoveryStrategyConfigs.size();

        if (countEnabled > 1) {
            throw new InvalidConfigurationException("Multiple join configuration cannot be enabled at the same time. Enable only "
                    + "one of: TCP/IP, Multicast, AWS, GCP, Azure, Kubernetes, Eureka, or Discovery Strategy");
        }
    }

    @Override
    public String toString() {
        return "JoinConfig{"
                + "multicastConfig=" + multicastConfig
                + ", tcpIpConfig=" + tcpIpConfig
                + ", awsConfig=" + awsConfig
                + ", gcpConfig=" + gcpConfig
                + ", azureConfig=" + azureConfig
                + ", kubernetesConfig=" + kubernetesConfig
                + ", eurekaConfig=" + eurekaConfig
                + ", discoveryConfig=" + discoveryConfig
                + ", autoDetectionConfig=" + autoDetectionConfig
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinConfig that = (JoinConfig) o;
        return Objects.equals(multicastConfig, that.multicastConfig)
                && Objects.equals(tcpIpConfig, that.tcpIpConfig) && Objects.equals(awsConfig, that.awsConfig)
                && Objects.equals(gcpConfig, that.gcpConfig) && Objects.equals(azureConfig, that.azureConfig)
                && Objects.equals(kubernetesConfig, that.kubernetesConfig)
                && Objects.equals(eurekaConfig, that.eurekaConfig)
                && Objects.equals(discoveryConfig, that.discoveryConfig)
                && Objects.equals(autoDetectionConfig, that.autoDetectionConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(multicastConfig, tcpIpConfig, awsConfig, gcpConfig, azureConfig, kubernetesConfig, eurekaConfig,
                discoveryConfig, autoDetectionConfig);
    }
}
