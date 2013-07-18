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
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.util.*;

// todo check the new attributes added on 3.0 working in spring configuration
//TODO @ali reform clientConfig API so that it reflects the server-side config API
public class ClientConfig {

    /**
     * The Group Configuration properties like:
     * Name and Password that is used to connect to the cluster.
     */

    private GroupConfig groupConfig = new GroupConfig();

    /**
     * List of the initial set of addresses.
     * Client will use this list to find a running Member, connect to it.
     */
    private final List<String> addressList = new ArrayList<String>(10);

    /**
     * Used to distribute the operations to multiple Endpoints.
     */
    private LoadBalancer loadBalancer = new RoundRobinLB();

    /**
     * List of listeners that Hazelcast will automatically add as a part of initialization process.
     * Currently only supports {@link com.hazelcast.core.LifecycleListener}.
     */
    private final Collection<EventListener> listeners = new HashSet<EventListener>();

    /**
     * If true, client will route the key based operations to owner of the key at the best effort.
     * Note that it uses a cached version of {@link com.hazelcast.core.PartitionService#getPartitions()} and doesn't
     * guarantee that the operation will always be executed on the owner. The cached table is updated every second.
     */
    private boolean smart = false;

    /**
     * If true, client will redo the operations that were executing on the server and client lost the connection.
     * This can be because of network, or simply because the member died. However it is not clear whether the
     * application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
     * retrying can cause to undesirable effects. Note that the redo can perform on any member.
     * <p/>
     * If false, the operation will throw {@link RuntimeException} that is wrapping {@link java.io.IOException}.
     */
    private boolean redoOperation = false;

    /**
     * limit for the Pool size that is used to pool the connections to the members.
     */
    private int connectionPoolSize = 100;

    /**
     * Client will be sending heartbeat messages to members and this is the timeout. If there is no any message
     * passing between client and member within the {@link ClientConfig#connectionTimeout} milliseconds the connection
     * will be closed.
     */
    private int connectionTimeout = 60000;

    /**
     * While client is trying to connect initially to one of the members in the {@link ClientConfig#addressList},
     * all might be not available. Instead of giving up, throwing Exception and stopping client, it will
     * attempt to retry as much as {@link ClientConfig#connectionAttemptLimit} times.
     */
    private int connectionAttemptLimit = 2;

    /**
     * Period for the next attempt to find a member to connect. (see {@link ClientConfig#connectionAttemptLimit}).
     */
    private int connectionAttemptPeriod = 3000;

    /**
     * Period for the next attempt to find a member to connect. (see {@link ClientConfig#connectionAttemptLimit}).
     */
    private int executorPoolSize = -1;


    private final SocketOptions socketOptions = new SocketOptions();

    private final SerializationConfig serializationConfig = new SerializationConfig();
    
    private final ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig();

    /**
     * Will be called with the Socket, each time client creates a connection to any Member.
     */
    private SocketInterceptor socketInterceptor = null;

    private ManagedContext managedContext = null;
    
    private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    
    /**
     * Can be used instead of {@link GroupConfig} in Hazelcast EE.
     */
    private Credentials credentials;

    private Map<String, NearCacheConfig> cacheConfigMap = new HashMap<String, NearCacheConfig>();

    public NearCacheConfig getNearCacheConfig(String mapName){
        return cacheConfigMap.get(mapName);
    }

    public ClientConfig addNearCacheConfig(String mapName, NearCacheConfig nearCacheConfig){
        cacheConfigMap.put(mapName, nearCacheConfig);
        return this;
    }


    public boolean isSmart() {
        return smart;
    }

    public ClientConfig setSmart(boolean smart) {
        this.smart = smart;
        return this;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    public ClientConfig setConnectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    public SocketInterceptor getSocketInterceptor() {
        return socketInterceptor;
    }

    public ClientConfig setSocketInterceptor(SocketInterceptor socketInterceptor) {
        this.socketInterceptor = socketInterceptor;
        return this;
    }

    public int getConnectionAttemptPeriod() {
        return connectionAttemptPeriod;
    }

    public ClientConfig setConnectionAttemptPeriod(int connectionAttemptPeriod) {
        this.connectionAttemptPeriod = connectionAttemptPeriod;
        return this;
    }

    public int getConnectionAttemptLimit() {
        return connectionAttemptLimit;
    }

    public ClientConfig setConnectionAttemptLimit(int connectionAttemptLimit) {
        this.connectionAttemptLimit = connectionAttemptLimit;
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

    public ClientConfig addAddress(String... addresses) {
        Collections.addAll(addressList, addresses);
        return this;
    }

    // required for spring module
    public ClientConfig setAddresses(List<String> addresses) {
        addressList.clear();
        for (String address : addresses) {
            addressList.addAll(addresses);
        }
        return this;
    }

    public Collection<String> getAddressList() {
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

    public ClientConfig setLoadBalancer(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
        return this;
    }

    public boolean isRedoOperation() {
        return redoOperation;
    }

    public ClientConfig setRedoOperation(boolean redoOperation) {
        this.redoOperation = redoOperation;
        return this;
    }

    public SocketOptions getSocketOptions() {
        return socketOptions;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public ClientConfig setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }

    public ManagedContext getManagedContext() {
        return managedContext;
    }

    public ClientConfig setManagedContext(ManagedContext managedContext) {
        this.managedContext = managedContext;
        return this;
    }

    public int getExecutorPoolSize() {
        return executorPoolSize;
    }

    public ClientConfig setExecutorPoolSize(int executorPoolSize) {
        this.executorPoolSize = executorPoolSize;
        return this;
    }

    public ProxyFactoryConfig getProxyFactoryConfig() {
        return proxyFactoryConfig;
    }

    public SerializationConfig getSerializationConfig()
    {
        return serializationConfig;
    }
}
