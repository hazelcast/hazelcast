/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains configuration parameters for client network related behaviour
 */
public class ClientNetworkConfig {

    private static final int CONNECTION_TIMEOUT = 5000;
    private static final int CONNECTION_ATTEMPT_PERIOD = 3000;
    private final List<String> addressList = new ArrayList<String>(10);
    private boolean smartRouting = true;
    private boolean redoOperation;
    private int connectionTimeout = CONNECTION_TIMEOUT;
    private int connectionAttemptLimit = -1;
    private int connectionAttemptPeriod = CONNECTION_ATTEMPT_PERIOD;
    private SocketInterceptorConfig socketInterceptorConfig;
    private SocketOptions socketOptions = new SocketOptions();
    private SSLConfig sslConfig;
    private ClientAwsConfig clientAwsConfig;
    private DiscoveryConfig discoveryConfig;
    private Collection<String> outboundPortDefinitions;
    private Collection<Integer> outboundPorts;

    /**
     * Returns the configuration of the Hazelcast Discovery SPI and configured discovery providers
     *
     * @return Discovery Provider SPI configuration
     */
    public DiscoveryConfig getDiscoveryConfig() {
        if (discoveryConfig == null) {
            discoveryConfig = new DiscoveryConfig();
        }
        return discoveryConfig;
    }

    /**
     * Defines the Discovery Provider SPI configuration
     *
     * @param discoveryConfig the Discovery Provider SPI configuration
     * @throws java.lang.IllegalArgumentException if discoveryConfig is null
     */
    public void setDiscoveryConfig(DiscoveryConfig discoveryConfig) {
        this.discoveryConfig = isNotNull(discoveryConfig, "discoveryConfig");
    }

    /**
     * @return true if client is smart
     * @see {@link com.hazelcast.client.config.ClientNetworkConfig#setSmartRouting(boolean)}  for details
     */
    public boolean isSmartRouting() {
        return smartRouting;
    }

    /**
     * If true, client will route the key based operations to owner of the key at the best effort.
     * Note that it uses a cached version of {@link com.hazelcast.core.PartitionService#getPartitions()} and doesn't
     * guarantee that the operation will always be executed on the owner. The cached table is updated every 10 seconds.
     * Default value is true.
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
        this.socketInterceptorConfig = socketInterceptorConfig;
        return this;
    }

    /**
     * Period for the next attempt to find a member to connect.
     *
     * @return connection attempt period in millis
     * @see {@link ClientNetworkConfig#connectionAttemptLimit}.
     */
    public int getConnectionAttemptPeriod() {
        return connectionAttemptPeriod;
    }

    /**
     * Period for the next attempt to find a member to connect.
     *
     * @param connectionAttemptPeriod time to wait before another attempt in millis
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    public ClientNetworkConfig setConnectionAttemptPeriod(int connectionAttemptPeriod) {
        if (connectionAttemptPeriod < 0) {
            throw new IllegalArgumentException("connectionAttemptPeriod cannot be negative");
        }
        this.connectionAttemptPeriod = connectionAttemptPeriod;
        return this;
    }

    /**
     * @return connection attempt Limit
     * @see {@link com.hazelcast.client.config.ClientNetworkConfig#setConnectionAttemptLimit(int)} for details
     */
    public int getConnectionAttemptLimit() {
        return connectionAttemptLimit;
    }

    /**
     * While client is trying to connect initially to one of the members in the {@link ClientNetworkConfig#addressList},
     * all might be not available. Instead of giving up, throwing Exception and stopping client, it will
     * attempt to retry as much as {@link ClientNetworkConfig#connectionAttemptLimit} times.
     *
     * @param connectionAttemptLimit number of times to attempt to connect
     *                               A zero value means try forever.
     *                               A negative value means default value
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    public ClientNetworkConfig setConnectionAttemptLimit(int connectionAttemptLimit) {
        if (connectionAttemptLimit < 0) {
            throw new IllegalArgumentException("connectionAttemptLimit cannot be negative");
        }
        this.connectionAttemptLimit = connectionAttemptLimit;
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
     * @param connectionTimeout Timeout value in millis for nodes to accept client connection requests.
     *                          A zero value means wait until connection established or an error occurs.
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    public ClientNetworkConfig setConnectionTimeout(int connectionTimeout) {
        if (connectionTimeout < 0) {
            throw new IllegalArgumentException("connectionTimeout cannot be negative");
        }
        this.connectionTimeout = connectionTimeout;
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
     * Adds given addresses to candidate address list that client will use to establish initial connection
     *
     * @param addresses to be added to initial address list
     * @return configured {@link com.hazelcast.client.config.ClientNetworkConfig} for chaining
     */
    // required for spring module
    public ClientNetworkConfig setAddresses(List<String> addresses) {
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
     * @return true if redo operations are enabled
     * @see {@link com.hazelcast.client.config.ClientNetworkConfig#setRedoOperation(boolean)} for details
     */
    public boolean isRedoOperation() {
        return redoOperation;
    }

    /**
     * If true, client will redo the operations that were executing on the server and client lost the connection.
     * This can be because of network, or simply because the member died. However it is not clear whether the
     * application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
     * retrying can cause to undesirable effects. Note that the redo can perform on any member.
     * <p/>
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
     * Sets configuration to connect nodes in aws environment.
     * null value indicates that no AwsConfig should be used.
     *
     * @param clientAwsConfig the ClientAwsConfig
     * @see #getAwsConfig()
     */
    public ClientNetworkConfig setAwsConfig(ClientAwsConfig clientAwsConfig) {
        this.clientAwsConfig = clientAwsConfig;
        return this;
    }

    /**
     * Returns the current {@link ClientAwsConfig}. It is possible that null is returned if no SSLConfig has been
     *
     * @return ClientAwsConfig
     * @see #setAwsConfig(ClientAwsConfig)
     */
    public ClientAwsConfig getAwsConfig() {
        return clientAwsConfig;
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
}
