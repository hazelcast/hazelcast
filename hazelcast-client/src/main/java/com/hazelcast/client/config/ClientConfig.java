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

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.impl.RoundRobinLB;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// todo check the new attributes added on 3.0 working in spring configuration
public class ClientConfig {

    /**
     * The Group Configuration properties like:
     * Name and Password that is used to connect to the cluster.
     *
     */

    private GroupConfig groupConfig = new GroupConfig();



    /**
     * List of the initial set of addresses.
     * Client will use this list to find a running Member, connect to it.
     */
    private final List<InetSocketAddress> addressList = new ArrayList<InetSocketAddress>(10);

    /**
     * Used to distribute the operations to multiple Endpoints.
     */
    private LoadBalancer loadBalancer = new RoundRobinLB();

    /**
     * 
     * List of listeners that Hazelcast will automatically add as a part of initialization process. 
     * Currently only supports {@link com.hazelcast.core.LifecycleListener}. 
     * 
     */
    private final Collection<EventListener> listeners = new HashSet<EventListener>();

    /**
     * If true, client will route the key based operations to owner of the key at the best effort.
     * Note that it uses a cached version of {@link com.hazelcast.core.PartitionService#getPartitions()} and doesn't
     * guarantee that the operation will always be executed on the owner. The cached table is updated every second.
     * 
     */
    private boolean smart = true;

    /**
     * If true, client will redo the operations that were executing on the server and client lost the connection.
     * This can be because of network, or simply because the member died. However it is not clear whether the
     * application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
     * retrying can cause to undesirable effects. Note that the redo can perform on any member.
     * 
     * If false, the operation will throw {@link RuntimeException} that is wrapping {@link java.io.IOException}.
     *
     */
    private boolean redoOperation = true;

    /**
     * limit for the Pool size that is used to pool the connections to the members.
     *
     */
    private int poolSize = 500;

    /**
     *
     * Client will be sending heartbeat messages to members and this is the timeout. If there is no any message 
     * passing between client and member within the {@link ClientConfig#connectionTimeout} milliseconds the connection
     * will be closed.
     */
    private int connectionTimeout = 300000;

    /**
     *
     * While client is trying to connect initially to one of the members in the {@link ClientConfig#addressList}, 
     * all might be not available. Instead of giving up, throwing Exception and stopping client, it will
     * attempt to retry as much as {@link ClientConfig#initialConnectionAttemptLimit} times.
     * 
     */
    private int initialConnectionAttemptLimit = 1;

    /**
     * Period for the next attempt to find a member to connect. (see {@link ClientConfig#initialConnectionAttemptLimit}).
     */
    private int attemptPeriod = 5000;

    //Not used currently.
    private int reconnectionAttemptLimit = 1;

    /**
     * Will be called with the Socket, each time client creates a connection to any Member. 
     */
    private SocketInterceptor socketInterceptor = null;

    /**
     * Can be used instead of {@link GroupConfig} in Hazelcast EE. 
     * 
     */
    private Credentials credentials;

    /**
     * Contains Near Cache configuration for the {@link com.hazelcast.core.IMap} Proxies on client. Each Map should be
     * explicitly configured for the client to Cache the values. No configuration for a certain Map, means no
     * Near Cache on client side.
     */
    private Map<String, NearCacheConfig> mapNearCacheConfigs = new ConcurrentHashMap<String, NearCacheConfig>();

    public boolean isSmart() {
        return smart;
    }

    public void setSmart(boolean smart) {
        this.smart = smart;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public SocketInterceptor getSocketInterceptor() {
        return socketInterceptor;
    }

    public void setSocketInterceptor(SocketInterceptor socketInterceptor) {
        this.socketInterceptor = socketInterceptor;
    }

    public int getAttemptPeriod() {
        return attemptPeriod;
    }

    public ClientConfig setAttemptPeriod(int attemptPeriod) {
        this.attemptPeriod = attemptPeriod;
        return this;
    }

    public int getReconnectionAttemptLimit() {
        return reconnectionAttemptLimit;
    }

    public ClientConfig setReconnectionAttemptLimit(int reconnectionAttemptLimit) {
        this.reconnectionAttemptLimit = reconnectionAttemptLimit;
        return this;
    }

    public int getInitialConnectionAttemptLimit() {
        return initialConnectionAttemptLimit;
    }

    public ClientConfig setInitialConnectionAttemptLimit(int initialConnectionAttemptLimit) {
        this.initialConnectionAttemptLimit = initialConnectionAttemptLimit;
        return this;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public ClientConfig setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public Credentials getCredentials() {
        if (credentials == null) {
            setCredentials(new UsernamePasswordCredentials(getGroupConfig().getName(),
                    getGroupConfig().getPassword()));
        }
        return credentials;
    }

    public ClientConfig setCredentials(Credentials credentials) {
        this.credentials = credentials;
        return this;
    }

    public ClientConfig addInetSocketAddress(List<InetSocketAddress> inetSocketAddresses) {
        this.addressList.addAll(inetSocketAddresses);
        return this;
    }

    public ClientConfig addInetSocketAddress(InetSocketAddress... inetSocketAddresses) {
        Collections.addAll(this.addressList, inetSocketAddresses);
        return this;
    }

    public ClientConfig addAddress(String... addresses) {
        for (String address : addresses) {
            this.addressList.addAll(AddressHelper.getSocketAddresses(address));
        }
        return this;
    }

    // required for spring module
    public void setAddresses(List<String> addresses) {
        addressList.clear();
        for (String address : addresses) {
            addressList.addAll(AddressHelper.getSocketAddresses(address));
        }
    }

    public Collection<InetSocketAddress> getAddressList() {
        if (addressList.size() == 0) {
            addAddress("localhost");
        }
        return addressList;
    }

    public GroupConfig getGroupConfig() {
        return groupConfig;
    }

    public ClientConfig setGroupConfig(GroupConfig groupConfig) {
        this.groupConfig = groupConfig;
        return this;
    }

    public Collection<EventListener> getListeners() {
        return listeners;
    }

    /**
     * Adds a listener object to configuration to be registered when {@code HazelcastClient} starts.
     *
     * @param listener one of {@link com.hazelcast.core.LifecycleListener}, {@link com.hazelcast.core.DistributedObjectListener}
     *                 or {@link com.hazelcast.core.MembershipListener}
     * @return
     */
    public ClientConfig addListener(EventListener listener) {
        listeners.add(listener);
        return this;
    }

    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    public void setLoadBalancer(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
    }

    public boolean isRedoOperation() {
        return redoOperation;
    }

    public void setRedoOperation(boolean redoOperation) {
        this.redoOperation = redoOperation;
    }

    public NearCacheConfig getNearCacheConfig(final String name) {
        return mapNearCacheConfigs.get(name);
    }

    public ClientConfig addMapNearCacheConfig(String name, NearCacheConfig config) {
        mapNearCacheConfigs.put(name, config);
        return this;
    }
}
