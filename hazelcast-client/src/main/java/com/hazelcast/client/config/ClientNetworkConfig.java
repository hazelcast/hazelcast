/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClientNetworkConfig {

    /**
     * List of the initial set of addresses.
     * Client will use this list to find a running Member, connect to it.
     */
    private final List<String> addressList = new ArrayList<String>(10);

    /**
     * If true, client will route the key based operations to owner of the key at the best effort.
     * Note that it uses a cached version of {@link com.hazelcast.core.PartitionService#getPartitions()} and doesn't
     * guarantee that the operation will always be executed on the owner. The cached table is updated every 10 seconds.
     */
    private boolean smartRouting = true;

    /**
     * If true, client will redo the operations that were executing on the server and client lost the connection.
     * This can be because of network, or simply because the member died. However it is not clear whether the
     * application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
     * retrying can cause to undesirable effects. Note that the redo can perform on any member.
     * <p/>
     * If false, the operation will throw {@link RuntimeException} that is wrapping {@link java.io.IOException}.
     * TODO clear what is the exception here
     */
    private boolean redoOperation;

    /**
     * Client will be sending heartbeat messages to members and this is the timeout. If there is no any message
     * passing between client and member within the {@link ClientNetworkConfig#connectionTimeout} milliseconds the connection
     * will be closed.
     */
    private int connectionTimeout = 60000;
    //TODO heartbeat

    /**
     * While client is trying to connect initially to one of the members in the {@link ClientNetworkConfig#addressList},
     * all might be not available. Instead of giving up, throwing Exception and stopping client, it will
     * attempt to retry as much as {@link ClientNetworkConfig#connectionAttemptLimit} times.
     */
    private int connectionAttemptLimit = 2;

    /**
     * Period for the next attempt to find a member to connect. (see {@link ClientNetworkConfig#connectionAttemptLimit}).
     */
    private int connectionAttemptPeriod = 3000;


    /**
     * Will be called with the Socket, each time client creates a connection to any Member.
     */
    private SocketInterceptorConfig socketInterceptorConfig;

    /**
     * Options for creating socket
     */
    private SocketOptions socketOptions = new SocketOptions();


    /**
     * Enabling ssl for client
     */
    private SSLConfig sslConfig;

    /**
     * Configuration to connect nodes in aws environment
     */
    private ClientAwsConfig clientAwsConfig;


    public boolean isSmartRouting() {
        return smartRouting;
    }

    public ClientNetworkConfig setSmartRouting(boolean smartRouting) {
        this.smartRouting = smartRouting;
        return this;
    }

    public SocketInterceptorConfig getSocketInterceptorConfig() {
        return socketInterceptorConfig;
    }

    public ClientNetworkConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        this.socketInterceptorConfig = socketInterceptorConfig;
        return this;
    }

    public int getConnectionAttemptPeriod() {
        return connectionAttemptPeriod;
    }

    public ClientNetworkConfig setConnectionAttemptPeriod(int connectionAttemptPeriod) {
        this.connectionAttemptPeriod = connectionAttemptPeriod;
        return this;
    }

    public int getConnectionAttemptLimit() {
        return connectionAttemptLimit;
    }

    public ClientNetworkConfig setConnectionAttemptLimit(int connectionAttemptLimit) {
        this.connectionAttemptLimit = connectionAttemptLimit;
        return this;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public ClientNetworkConfig setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public ClientNetworkConfig addAddress(String... addresses) {
        Collections.addAll(addressList, addresses);
        return this;
    }

    // required for spring module
    public ClientNetworkConfig setAddresses(List<String> addresses) {
        addressList.clear();
        addressList.addAll(addresses);
        return this;
    }

    public List<String> getAddresses() {
        if (addressList.size() == 0) {
            addAddress("localhost");
        }
        return addressList;
    }

    public boolean isRedoOperation() {
        return redoOperation;
    }

    public ClientNetworkConfig setRedoOperation(boolean redoOperation) {
        this.redoOperation = redoOperation;
        return this;
    }

    public SocketOptions getSocketOptions() {
        return socketOptions;
    }

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
    public void setAwsConfig(ClientAwsConfig clientAwsConfig) {
        this.clientAwsConfig = clientAwsConfig;
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
}
