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

import com.hazelcast.config.AutoDetectionConfig;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.partition.PartitionService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Contains configuration parameters for client network related behaviour
 */
public class ClientNetworkConfig {

    private static final int CONNECTION_TIMEOUT = 5000;
    private final List<String> addressList;
    private boolean smartRouting = true;
    private boolean redoOperation;
    private int connectionTimeout = CONNECTION_TIMEOUT;
    private SocketInterceptorConfig socketInterceptorConfig = new SocketInterceptorConfig();
    private SocketOptions socketOptions = new SocketOptions();
    private SSLConfig sslConfig;
    private AwsConfig awsConfig = new AwsConfig();
    private GcpConfig gcpConfig = new GcpConfig();
    private AzureConfig azureConfig = new AzureConfig();
    private KubernetesConfig kubernetesConfig = new KubernetesConfig();
    private EurekaConfig eurekaConfig = new EurekaConfig();
    private ClientCloudConfig cloudConfig = new ClientCloudConfig();
    private DiscoveryConfig discoveryConfig = new DiscoveryConfig();
    private AutoDetectionConfig autoDetectionConfig = new AutoDetectionConfig();
    private Collection<String> outboundPortDefinitions;
    private Collection<Integer> outboundPorts;
    private ClientIcmpPingConfig clientIcmpPingConfig = new ClientIcmpPingConfig();

    public ClientNetworkConfig() {
        addressList = new ArrayList<String>();
    }

    public ClientNetworkConfig(ClientNetworkConfig networkConfig) {
        addressList = new ArrayList<String>(networkConfig.addressList);
        smartRouting = networkConfig.smartRouting;
        redoOperation = networkConfig.redoOperation;
        connectionTimeout = networkConfig.connectionTimeout;
        socketInterceptorConfig = new SocketInterceptorConfig(networkConfig.socketInterceptorConfig);
        socketOptions = new SocketOptions(networkConfig.socketOptions);
        sslConfig = networkConfig.sslConfig == null ? null : new SSLConfig(networkConfig.sslConfig);
        awsConfig = new AwsConfig(networkConfig.awsConfig);
        gcpConfig = new GcpConfig(networkConfig.gcpConfig);
        azureConfig = new AzureConfig(networkConfig.azureConfig);
        kubernetesConfig = new KubernetesConfig(networkConfig.kubernetesConfig);
        eurekaConfig = new EurekaConfig(networkConfig.eurekaConfig);
        cloudConfig = new ClientCloudConfig(networkConfig.cloudConfig);
        discoveryConfig = new DiscoveryConfig(networkConfig.discoveryConfig);
        outboundPortDefinitions = networkConfig.outboundPortDefinitions == null
                ? null : new HashSet<String>(networkConfig.outboundPortDefinitions);
        outboundPorts = networkConfig.outboundPorts == null ? null : new HashSet<Integer>(networkConfig.outboundPorts);
        clientIcmpPingConfig = new ClientIcmpPingConfig(networkConfig.clientIcmpPingConfig);
    }

    /**
     * Returns the configuration of the Hazelcast Discovery SPI and configured discovery providers
     *
     * @return Discovery Provider SPI configuration
     */
    public DiscoveryConfig getDiscoveryConfig() {
        return discoveryConfig;
    }

    /**
     * Defines the Discovery Provider SPI configuration
     *
     * @param discoveryConfig the Discovery Provider SPI configuration
     * @return this configuration
     * @throws java.lang.IllegalArgumentException if discoveryConfig is null
     */
    public ClientNetworkConfig setDiscoveryConfig(DiscoveryConfig discoveryConfig) {
        this.discoveryConfig = isNotNull(discoveryConfig, "discoveryConfig");
        return this;
    }


    /**
     * Returns the configuration of the Auto Detection discovery.
     *
     * @return Configuration of Auto Detection discovery
     */
    public AutoDetectionConfig getAutoDetectionConfig() {
        return autoDetectionConfig;
    }

    /**
     * Any other connect configuration takes precedence over auto-discovery, so auto-discovery is enabled only when no other
     * strategy is enabled.
     *
     * @return true if auto-detection is enabled
     */
    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public boolean isAutoDetectionEnabled() {
        return autoDetectionConfig.isEnabled()
                && addressList.isEmpty()
                && !cloudConfig.isEnabled()
                && !awsConfig.isEnabled()
                && !gcpConfig.isEnabled()
                && !azureConfig.isEnabled()
                && !kubernetesConfig.isEnabled()
                && !eurekaConfig.isEnabled()
                && !discoveryConfig.isEnabled();
    }

    /**
     * Defines the Auto Detection configuration.
     *
     * @param autoDetectionConfig Auto Detection configuration
     * @return this configuration
     * @throws java.lang.IllegalArgumentException if autoDetectionConfig is null
     */
    public ClientNetworkConfig setAutoDetectionConfig(AutoDetectionConfig autoDetectionConfig) {
        this.autoDetectionConfig = isNotNull(autoDetectionConfig, "autoDetectionConfig");
        return this;
    }

    /**
     * See {@link com.hazelcast.client.config.ClientNetworkConfig#setSmartRouting(boolean)}  for details
     *
     * @return true if client is smart
     */
    public boolean isSmartRouting() {
        return smartRouting;
    }

    /**
     * If {@code true}, client will route the key-based operations to owner of the key on best-effort basis.
     * Note that it uses a cached version of {@link PartitionService#getPartitions()} and doesn't
     * guarantee that the operation will always be executed on the owner. The cached table is updated every 10 seconds.
     * <p>
     * If {@code smartRouting == false}, all operations will be routed to single member. Operations will need two
     * hops if the chosen member is not owner of the key. Client will have only single open connection. Useful, if
     * there are many clients and we want to avoid each of them connecting to each member.
     * <p>
     * Default value is {@code true}.
     *
     * @param smartRouting true if smart routing should be enabled.
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    public ClientNetworkConfig setSmartRouting(boolean smartRouting) {
        this.smartRouting = smartRouting;
        return this;
    }

    /**
     * @return socket interceptor config that will be called with the Socket,
     * each time client creates a connection to any Member.
     */
    public SocketInterceptorConfig getSocketInterceptorConfig() {
        return socketInterceptorConfig;
    }

    /**
     * @param socketInterceptorConfig will be called with the Socket,
     *                                each time client creates a connection to any Member.
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    public ClientNetworkConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        this.socketInterceptorConfig = isNotNull(socketInterceptorConfig, "socketInterceptorConfig");
        return this;
    }

    /**
     * Timeout value in millis for nodes to accept client connection requests.
     *
     * @return connection timeout value in millis
     */
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @param connectionTimeoutInMillis Timeout value in millis for nodes to accept client connection requests.
     *                          A zero value means wait until connection established or an error occurs.
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    public ClientNetworkConfig setConnectionTimeout(int connectionTimeoutInMillis) {
        if (connectionTimeoutInMillis < 0) {
            throw new IllegalArgumentException("connectionTimeout cannot be negative");
        }
        this.connectionTimeout = connectionTimeoutInMillis;
        return this;
    }

    /**
     * Adds given addresses to candidate address list that client will use to establish initial connection
     *
     * @param addresses to be added to initial address list
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    public ClientNetworkConfig addAddress(String... addresses) {
        isNotNull(addresses, "addresses");
        for (String address : addresses) {
            isNotNull(address, "address");
            checkHasText(address.trim(), "member must contain text");
        }
        Collections.addAll(addressList, addresses);
        return this;
    }

    /**
     * Sets given addresses as candidate address list that client will use to establish initial connection
     *
     * @param addresses to be added to initial address list
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    // required for spring module
    public ClientNetworkConfig setAddresses(List<String> addresses) {
        isNotNull(addresses, "addresses");
        addressList.clear();
        addressList.addAll(addresses);
        return this;
    }

    /**
     * Returns the list of candidate addresses that client will use to establish initial connection
     *
     * @return list of addresses
     */
    public List<String> getAddresses() {
        return addressList;
    }

    /**
     * See {@link com.hazelcast.client.config.ClientNetworkConfig#setRedoOperation(boolean)} for details
     *
     * @return true if redo operations are enabled
     */
    public boolean isRedoOperation() {
        return redoOperation;
    }

    /**
     * If true, client will redo the operations that were executing on the server and client lost the connection.
     * This can be because of network, or simply because the member died. However it is not clear whether the
     * application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
     * retrying can cause to undesirable effects. Note that the redo can perform on any member.
     * <p>
     * If false, the operation will throw {@link RuntimeException} that is wrapping {@link java.io.IOException}.
     * TODO clear what is the exception here
     *
     * @param redoOperation true if redo operations are enabled
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    public ClientNetworkConfig setRedoOperation(boolean redoOperation) {
        this.redoOperation = redoOperation;
        return this;
    }

    /**
     * @return TCP Socket options
     */
    public SocketOptions getSocketOptions() {
        return socketOptions;
    }

    /**
     * @param socketOptions TCP Socket options
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    public ClientNetworkConfig setSocketOptions(SocketOptions socketOptions) {
        this.socketOptions = socketOptions;
        return this;
    }

    /**
     * Returns the current {@link SSLConfig}. It is possible that null is returned if no SSLConfig has been
     * set.
     *
     * @return the SSLConfig.
     * @see #setSSLConfig(SSLConfig)
     */
    public SSLConfig getSSLConfig() {
        return sslConfig;
    }

    /**
     * Sets the {@link SSLConfig}. null value indicates that no SSLConfig should be used.
     *
     * @param sslConfig the SSLConfig.
     * @return the updated ClientNetworkConfig.
     * @see #getSSLConfig()
     */
    public ClientNetworkConfig setSSLConfig(SSLConfig sslConfig) {
        this.sslConfig = sslConfig;
        return this;
    }

    /**
     * Sets configuration to connect nodes in AWS environment.
     *
     * @param clientAwsConfig the ClientAwsConfig
     * @see #getAwsConfig()
     */
    public ClientNetworkConfig setAwsConfig(AwsConfig clientAwsConfig) {
        isNotNull(clientAwsConfig, "clientAwsConfig");
        this.awsConfig = clientAwsConfig;
        return this;
    }

    /**
     * Returns the current {@link AwsConfig}.
     *
     * @return ClientAwsConfig
     * @see #setAwsConfig(AwsConfig)
     */
    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    /**
     * Sets configuration to connect nodes in GCP environment.
     *
     * @param gcpConfig the GcpConfig
     * @see #getGcpConfig()
     */
    public ClientNetworkConfig setGcpConfig(GcpConfig gcpConfig) {
        isNotNull(gcpConfig, "gcpConfig");
        this.gcpConfig = gcpConfig;
        return this;
    }

    /**
     * Returns the current {@link GcpConfig}.
     *
     * @return GcpConfig
     * @see #setGcpConfig(GcpConfig)
     */
    public GcpConfig getGcpConfig() {
        return gcpConfig;
    }

    /**
     * Sets configuration to connect nodes in Azure environment.
     *
     * @param azureConfig the AzureConfig
     * @see #getAzureConfig()
     */
    public ClientNetworkConfig setAzureConfig(AzureConfig azureConfig) {
        isNotNull(azureConfig, "azureConfig");
        this.azureConfig = azureConfig;
        return this;
    }

    /**
     * Returns the current {@link AzureConfig}.
     *
     * @return AzureConfig
     * @see #setAzureConfig(AzureConfig)
     */
    public AzureConfig getAzureConfig() {
        return azureConfig;
    }

    /**
     * Sets configuration to connect nodes in Kubernetes environment.
     *
     * @param kubernetesConfig the KubernetesConfig
     * @see #getKubernetesConfig()
     */
    public ClientNetworkConfig setKubernetesConfig(KubernetesConfig kubernetesConfig) {
        isNotNull(kubernetesConfig, "kubernetesConfig");
        this.kubernetesConfig = kubernetesConfig;
        return this;
    }

    /**
     * Returns the current {@link KubernetesConfig}.
     *
     * @return KubernetesConfig
     * @see #setKubernetesConfig(KubernetesConfig)
     */
    public KubernetesConfig getKubernetesConfig() {
        return kubernetesConfig;
    }

    /**
     * Sets configuration to connect nodes in Eureka environment.
     *
     * @param eurekaConfig the EurekaConfig
     * @see #getEurekaConfig()
     */
    public ClientNetworkConfig setEurekaConfig(EurekaConfig eurekaConfig) {
        isNotNull(eurekaConfig, "eurekaConfig");
        this.eurekaConfig = eurekaConfig;
        return this;
    }

    /**
     * Returns the current {@link EurekaConfig}.
     *
     * @return EurekaConfig
     * @see #setEurekaConfig(EurekaConfig)
     */
    public EurekaConfig getEurekaConfig() {
        return eurekaConfig;
    }

    public ClientCloudConfig getCloudConfig() {
        return cloudConfig;
    }

    public ClientNetworkConfig setCloudConfig(ClientCloudConfig cloudConfig) {
        isNotNull(cloudConfig, "cloudConfig");
        this.cloudConfig = cloudConfig;
        return this;
    }

    /**
     * Returns the outbound port definitions. It is possible that null is returned if not defined.
     *
     * @return list of outbound port definitions
     */
    public Collection<String> getOutboundPortDefinitions() {
        return outboundPortDefinitions;
    }

    /**
     * Returns the outbound ports. It is possible that null is returned if not defined.
     *
     * @return list of outbound ports
     */
    public Collection<Integer> getOutboundPorts() {
        return outboundPorts;
    }

    /**
     * Set outbound port definitions
     *
     * @param outboundPortDefinitions outbound port definitions
     * @return ClientNetworkConfig
     */
    public ClientNetworkConfig setOutboundPortDefinitions(final Collection<String> outboundPortDefinitions) {
        this.outboundPortDefinitions = outboundPortDefinitions;
        return this;
    }

    /**
     * Set outbond ports
     *
     * @param outboundPorts outbound ports
     * @return ClientNetworkConfig
     */
    public ClientNetworkConfig setOutboundPorts(final Collection<Integer> outboundPorts) {
        this.outboundPorts = outboundPorts;
        return this;
    }

    /**
     * Add outbound port to the outbound port list
     *
     * @param port outbound port
     * @return ClientNetworkConfig
     */
    public ClientNetworkConfig addOutboundPort(int port) {
        if (outboundPorts == null) {
            outboundPorts = new HashSet<Integer>();
        }
        outboundPorts.add(port);
        return this;
    }

    /**
     * Add outbound port definition to the outbound port definition list
     *
     * @param portDef outbound port definition
     * @return ClientNetworkConfig
     */
    public ClientNetworkConfig addOutboundPortDefinition(String portDef) {
        if (outboundPortDefinitions == null) {
            outboundPortDefinitions = new HashSet<String>();
        }
        outboundPortDefinitions.add(portDef);
        return this;
    }

    /**
     * ICMP ping is used to detect if machine that a remote hazelcast member runs on alive or not
     *
     * @return current configuration for client icmp ping, returns the default configuration if not set by user
     */
    public ClientIcmpPingConfig getClientIcmpPingConfig() {
        return clientIcmpPingConfig;
    }

    /**
     * ICMP ping is used to detect if machine that a remote hazelcast member runs on alive or not
     *
     * @param clientIcmpPingConfig configuration for client icmp ping
     * @return ClientNetworkConfig for chaining
     */
    public ClientNetworkConfig setClientIcmpPingConfig(ClientIcmpPingConfig clientIcmpPingConfig) {
        this.clientIcmpPingConfig = clientIcmpPingConfig;
        return this;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:methodlength"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientNetworkConfig that = (ClientNetworkConfig) o;

        if (smartRouting != that.smartRouting) {
            return false;
        }
        if (redoOperation != that.redoOperation) {
            return false;
        }
        if (connectionTimeout != that.connectionTimeout) {
            return false;
        }
        if (!addressList.equals(that.addressList)) {
            return false;
        }
        if (!socketInterceptorConfig.equals(that.socketInterceptorConfig)) {
            return false;
        }
        if (!socketOptions.equals(that.socketOptions)) {
            return false;
        }
        if (sslConfig != null ? !sslConfig.equals(that.sslConfig) : that.sslConfig != null) {
            return false;
        }
        if (!awsConfig.equals(that.awsConfig)) {
            return false;
        }
        if (!gcpConfig.equals(that.gcpConfig)) {
            return false;
        }
        if (!azureConfig.equals(that.azureConfig)) {
            return false;
        }
        if (!kubernetesConfig.equals(that.kubernetesConfig)) {
            return false;
        }
        if (!eurekaConfig.equals(that.eurekaConfig)) {
            return false;
        }
        if (!cloudConfig.equals(that.cloudConfig)) {
            return false;
        }
        if (!discoveryConfig.equals(that.discoveryConfig)) {
            return false;
        }
        if (outboundPortDefinitions != null
                ? !outboundPortDefinitions.equals(that.outboundPortDefinitions) : that.outboundPortDefinitions != null) {
            return false;
        }
        if (outboundPorts != null ? !outboundPorts.equals(that.outboundPorts) : that.outboundPorts != null) {
            return false;
        }
        return clientIcmpPingConfig.equals(that.clientIcmpPingConfig);
    }

    @Override
    public int hashCode() {
        int result = addressList.hashCode();
        result = 31 * result + (smartRouting ? 1 : 0);
        result = 31 * result + (redoOperation ? 1 : 0);
        result = 31 * result + connectionTimeout;
        result = 31 * result + socketInterceptorConfig.hashCode();
        result = 31 * result + socketOptions.hashCode();
        result = 31 * result + (sslConfig != null ? sslConfig.hashCode() : 0);
        result = 31 * result + awsConfig.hashCode();
        result = 31 * result + gcpConfig.hashCode();
        result = 31 * result + azureConfig.hashCode();
        result = 31 * result + kubernetesConfig.hashCode();
        result = 31 * result + eurekaConfig.hashCode();
        result = 31 * result + cloudConfig.hashCode();
        result = 31 * result + discoveryConfig.hashCode();
        result = 31 * result + (outboundPortDefinitions != null ? outboundPortDefinitions.hashCode() : 0);
        result = 31 * result + (outboundPorts != null ? outboundPorts.hashCode() : 0);
        result = 31 * result + clientIcmpPingConfig.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ClientNetworkConfig{"
                + "addressList=" + addressList
                + ", smartRouting=" + smartRouting
                + ", redoOperation=" + redoOperation
                + ", connectionTimeout=" + connectionTimeout
                + ", socketInterceptorConfig=" + socketInterceptorConfig
                + ", socketOptions=" + socketOptions
                + ", sslConfig=" + sslConfig
                + ", awsConfig=" + awsConfig
                + ", gcpConfig=" + gcpConfig
                + ", azureConfig=" + azureConfig
                + ", kubernetesConfig=" + kubernetesConfig
                + ", eurekaConfig=" + eurekaConfig
                + ", cloudConfig=" + cloudConfig
                + ", discoveryConfig=" + discoveryConfig
                + ", outboundPortDefinitions=" + outboundPortDefinitions
                + ", outboundPorts=" + outboundPorts
                + ", clientIcmpPingConfig=" + clientIcmpPingConfig
                + '}';
    }
}
