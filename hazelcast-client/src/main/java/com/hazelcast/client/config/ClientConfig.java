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
import com.hazelcast.config.*;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.util.*;

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
    private List<ListenerConfig> listenerConfigs = new LinkedList<ListenerConfig>();

    /**
     * If true, client will route the key based operations to owner of the key at the best effort.
     * Note that it uses a cached version of {@link com.hazelcast.core.PartitionService#getPartitions()} and doesn't
     * guarantee that the operation will always be executed on the owner. The cached table is updated every second.
     */
    private boolean smartRouting = true;

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


    private SocketOptions socketOptions = new SocketOptions();

    private SerializationConfig serializationConfig = new SerializationConfig();
    
    private List<ProxyFactoryConfig> proxyFactoryConfigs = new LinkedList<ProxyFactoryConfig>();

    /**
     * Will be called with the Socket, each time client creates a connection to any Member.
     */
    private SocketInterceptorConfig socketInterceptorConfig = null;

    private ManagedContext managedContext = null;
    
    private ClassLoader classLoader = null;
    
    /**
     * Can be used instead of {@link GroupConfig} in Hazelcast EE.
     */
    private Credentials credentials;

    private Map<String, NearCacheConfig> nearCacheConfigMap = new HashMap<String, NearCacheConfig>();

    public ClientConfig addNearCacheConfig(String mapName, NearCacheConfig nearCacheConfig){
        nearCacheConfigMap.put(mapName, nearCacheConfig);
        return this;
    }

    public ClientConfig addListenerConfig(ListenerConfig listenerConfig) {
        getListenerConfigs().add(listenerConfig);
        return this;
    }

    public ClientConfig addProxyFactoryConfig(ProxyFactoryConfig proxyFactoryConfig) {
        this.proxyFactoryConfigs.add(proxyFactoryConfig);
        return this;
    }


    public NearCacheConfig getNearCacheConfig(String mapName) {
        return lookupByPattern(nearCacheConfigMap, mapName);
    }

    public Map<String, NearCacheConfig> getNearCacheConfigMap() {
        return nearCacheConfigMap;
    }

    public ClientConfig setNearCacheConfigMap(Map<String, NearCacheConfig> nearCacheConfigMap) {
        this.nearCacheConfigMap = nearCacheConfigMap;
        return this;
    }

    public boolean isSmartRouting() {
        return smartRouting;
    }

    public ClientConfig setSmartRouting(boolean smartRouting) {
        this.smartRouting = smartRouting;
        return this;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    public ClientConfig setConnectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    public SocketInterceptorConfig getSocketInterceptorConfig() {
        return socketInterceptorConfig;
    }

    public ClientConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        this.socketInterceptorConfig = socketInterceptorConfig;
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
        addressList.addAll(addresses);
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

    public List<ListenerConfig> getListenerConfigs() {
        return listenerConfigs;
    }

    public ClientConfig setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this ;
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

    public ClientConfig setSocketOptions(SocketOptions socketOptions) {
        this.socketOptions = socketOptions;
        return this;
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

    public List<ProxyFactoryConfig> getProxyFactoryConfigs() {
        return proxyFactoryConfigs;
    }

    public ClientConfig setProxyFactoryConfigs(List<ProxyFactoryConfig> proxyFactoryConfigs) {
        this.proxyFactoryConfigs = proxyFactoryConfigs;
        return this;
    }

    public SerializationConfig getSerializationConfig()
    {
        return serializationConfig;
    }

    public ClientConfig setSerializationConfig(SerializationConfig serializationConfig) {
        this.serializationConfig = serializationConfig;
        return this;
    }


    private static <T> T lookupByPattern(Map<String, T> map, String name) {
        T t = map.get(name);
        if (t == null) {
            final Set<String> tNames = map.keySet();
            for (final String pattern : tNames) {
                if (nameMatches(name, pattern)) {
                    return map.get(pattern);
                }
            }
        }
        return t;
    }

    private static boolean nameMatches(final String name, final String pattern) {
        final int index = pattern.indexOf('*');
        if (index == -1) {
            return name.equals(pattern);
        } else {
            final String firstPart = pattern.substring(0, index);
            final int indexFirstPart = name.indexOf(firstPart, 0);
            if (indexFirstPart == -1) {
                return false;
            }
            final String secondPart = pattern.substring(index + 1);
            final int indexSecondPart = name.indexOf(secondPart, index + 1);
            return indexSecondPart != -1;
        }
    }

}
