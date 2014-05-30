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
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.security.Credentials;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ClientConfig {


    /**
     * To pass properties
     */
    private Properties properties = new Properties();

    /**
     * The Group Configuration properties like:
     * Name and Password that is used to connect to the cluster.
     */
    private GroupConfig groupConfig = new GroupConfig();

    /**
     * The Security Configuration for custom Credentials:
     * Name and Password that is used to connect to the cluster.
     */
    private ClientSecurityConfig securityConfig = new ClientSecurityConfig();

    /**
     * The Network Configuration properties like:
     * addresses to connect, smart-routing, socket-options...
     */
    private ClientNetworkConfig networkConfig = new ClientNetworkConfig();

    /**
     * Used to distribute the operations to multiple Endpoints.
     */
    private LoadBalancer loadBalancer;

    /**
     * List of listeners that Hazelcast will automatically add as a part of initialization process.
     * Currently only supports {@link com.hazelcast.core.LifecycleListener}.
     */
    private List<ListenerConfig> listenerConfigs = new LinkedList<ListenerConfig>();

    /**
     * pool-size for internal ExecutorService which handles responses etc.
     */
    private int executorPoolSize = -1;

    private SerializationConfig serializationConfig = new SerializationConfig();

    private List<ProxyFactoryConfig> proxyFactoryConfigs = new LinkedList<ProxyFactoryConfig>();

    private ManagedContext managedContext;

    private ClassLoader classLoader;

    public String getProperty(String name) {
        String value = properties.getProperty(name);
        return value != null ? value : System.getProperty(name);
    }

    public ClientConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    private Map<String, NearCacheConfig> nearCacheConfigMap = new HashMap<String, NearCacheConfig>();

    public ClientConfig setProperties(final Properties properties) {
        this.properties = properties;
        return this;
    }

    public ClientSecurityConfig getSecurityConfig() {
        return securityConfig;
    }

    public ClientConfig setSecurityConfig(ClientSecurityConfig securityConfig) {
        this.securityConfig = securityConfig;
        return this;
    }

    public ClientNetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    public ClientConfig setNetworkConfig(ClientNetworkConfig networkConfig) {
        this.networkConfig = networkConfig;
        return this;
    }

    /**
     * please use {@link ClientConfig#addNearCacheConfig(NearCacheConfig)}
     *
     * @param mapName
     * @param nearCacheConfig
     * @return
     */
    @Deprecated
    public ClientConfig addNearCacheConfig(String mapName, NearCacheConfig nearCacheConfig) {
        nearCacheConfig.setName(mapName);
        return addNearCacheConfig(nearCacheConfig);
    }

    public ClientConfig addNearCacheConfig(NearCacheConfig nearCacheConfig) {
        nearCacheConfigMap.put(nearCacheConfig.getName(), nearCacheConfig);
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

    /**
     * Use {@link ClientNetworkConfig#isSmartRouting} instead
     */
    @Deprecated
    public boolean isSmartRouting() {
        return networkConfig.isSmartRouting();
    }

    /**
     * Use {@link ClientNetworkConfig#setSmartRouting} instead
     */
    @Deprecated
    public ClientConfig setSmartRouting(boolean smartRouting) {
        networkConfig.setSmartRouting(smartRouting);
        return this;
    }

    /**
     * Use {@link ClientNetworkConfig#getSocketInterceptorConfig} instead
     */
    @Deprecated
    public SocketInterceptorConfig getSocketInterceptorConfig() {
        return networkConfig.getSocketInterceptorConfig();
    }

    /**
     * Use {@link ClientNetworkConfig#setSocketInterceptorConfig} instead
     */
    @Deprecated
    public ClientConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        networkConfig.setSocketInterceptorConfig(socketInterceptorConfig);
        return this;
    }

    /**
     * Use {@link ClientNetworkConfig#getConnectionAttemptPeriod} instead
     */
    @Deprecated
    public int getConnectionAttemptPeriod() {
        return networkConfig.getConnectionAttemptPeriod();
    }

    /**
     * Use {@link ClientNetworkConfig#setConnectionAttemptPeriod} instead
     */
    @Deprecated
    public ClientConfig setConnectionAttemptPeriod(int connectionAttemptPeriod) {
        networkConfig.setConnectionAttemptPeriod(connectionAttemptPeriod);
        return this;
    }

    /**
     * Use {@link ClientNetworkConfig#getConnectionAttemptLimit} instead
     */
    @Deprecated
    public int getConnectionAttemptLimit() {
        return networkConfig.getConnectionAttemptLimit();
    }

    /**
     * Use {@link ClientNetworkConfig#setConnectionAttemptLimit} instead
     */
    @Deprecated
    public ClientConfig setConnectionAttemptLimit(int connectionAttemptLimit) {
        networkConfig.setConnectionAttemptLimit(connectionAttemptLimit);
        return this;
    }

    /**
     * Use {@link ClientNetworkConfig#getConnectionTimeout} instead
     */
    @Deprecated
    public int getConnectionTimeout() {
        return networkConfig.getConnectionTimeout();
    }

    /**
     * Use {@link ClientNetworkConfig#setConnectionTimeout} instead
     */
    @Deprecated
    public ClientConfig setConnectionTimeout(int connectionTimeout) {
        networkConfig.setConnectionTimeout(connectionTimeout);
        return this;
    }

    public Credentials getCredentials() {
        return securityConfig.getCredentials();
    }

    public ClientConfig setCredentials(Credentials credentials) {
        securityConfig.setCredentials(credentials);
        return this;
    }

    /**
     * Use {@link ClientNetworkConfig#addAddress} instead
     */
    @Deprecated
    public ClientConfig addAddress(String... addresses) {
        networkConfig.addAddress(addresses);
        return this;
    }

    /**
     * Use {@link ClientNetworkConfig#setAddresses} instead
     */
    @Deprecated
    public ClientConfig setAddresses(List<String> addresses) {
        networkConfig.setAddresses(addresses);
        return this;
    }

    /**
     * Use {@link ClientNetworkConfig#getAddresses} instead
     */
    @Deprecated
    public List<String> getAddresses() {
        return networkConfig.getAddresses();
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
        return this;
    }

    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    public ClientConfig setLoadBalancer(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
        return this;
    }

    /**
     * Use {@link ClientNetworkConfig#isRedoOperation} instead
     */
    @Deprecated
    public boolean isRedoOperation() {
        return networkConfig.isRedoOperation();
    }

    /**
     * Use {@link ClientNetworkConfig#setRedoOperation} instead
     */
    @Deprecated
    public ClientConfig setRedoOperation(boolean redoOperation) {
        networkConfig.setRedoOperation(redoOperation);
        return this;
    }

    /**
     * Use {@link ClientNetworkConfig#getSocketOptions} instead
     */
    @Deprecated
    public SocketOptions getSocketOptions() {
        return networkConfig.getSocketOptions();
    }

    /**
     * Use {@link ClientNetworkConfig#setSocketOptions} instead
     */
    @Deprecated
    public ClientConfig setSocketOptions(SocketOptions socketOptions) {
        networkConfig.setSocketOptions(socketOptions);
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

    public SerializationConfig getSerializationConfig() {
        return serializationConfig;
    }

    public ClientConfig setSerializationConfig(SerializationConfig serializationConfig) {
        this.serializationConfig = serializationConfig;
        return this;
    }


    private static <T> T lookupByPattern(Map<String, T> map, String name) {
        T t = map.get(name);
        if (t == null) {
            int lastMatchingPoint = -1;
            for (Map.Entry<String, T> entry : map.entrySet()) {
                String pattern = entry.getKey();
                T value = entry.getValue();
                final int matchingPoint = getMatchingPoint(name, pattern);
                if (matchingPoint > lastMatchingPoint) {
                    lastMatchingPoint = matchingPoint;
                    t = value;
                }
            }
        }
        return t;
    }

    /**
     * higher values means more specific matching
     *
     * @param name
     * @param pattern
     * @return -1 if name does not match at all, zero or positive otherwise
     */
    private static int getMatchingPoint(final String name, final String pattern) {
        final int index = pattern.indexOf('*');
        if (index == -1) {
            return -1;
        }
        final String firstPart = pattern.substring(0, index);
        final int indexFirstPart = name.indexOf(firstPart, 0);
        if (indexFirstPart == -1) {
            return -1;
        }
        final String secondPart = pattern.substring(index + 1);
        final int indexSecondPart = name.indexOf(secondPart, index + 1);
        if (indexSecondPart == -1) {
            return -1;
        }
        return firstPart.length() + secondPart.length();
    }
}
