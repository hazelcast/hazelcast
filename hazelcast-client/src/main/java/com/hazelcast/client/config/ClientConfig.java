/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.config.ConfigUtils;
import com.hazelcast.security.Credentials;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.function.BiConsumer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;
import static com.hazelcast.util.Preconditions.checkFalse;

/**
 * Main configuration to setup a Hazelcast Client
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
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
    private final List<ListenerConfig> listenerConfigs;

    /**
     * pool-size for internal ExecutorService which handles responses etc.
     */
    private int executorPoolSize = -1;
    private String instanceName;
    private ConfigPatternMatcher configPatternMatcher = new MatchingPointConfigPatternMatcher();
    private final Map<String, NearCacheConfig> nearCacheConfigMap;
    private final Map<String, ClientReliableTopicConfig> reliableTopicConfigMap;
    private final Map<String, Map<String, QueryCacheConfig>> queryCacheConfigs;
    private SerializationConfig serializationConfig = new SerializationConfig();
    private NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig();
    private final List<ProxyFactoryConfig> proxyFactoryConfigs;
    private ManagedContext managedContext;
    private ClassLoader classLoader;
    private String licenseKey;
    private ClientConnectionStrategyConfig connectionStrategyConfig = new ClientConnectionStrategyConfig();
    private ClientUserCodeDeploymentConfig userCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
    private final Map<String, ClientFlakeIdGeneratorConfig> flakeIdGeneratorConfigMap;
    private final Set<String> labels;
    private final ConcurrentMap<String, Object> userContext;

    public ClientConfig() {
        listenerConfigs = new LinkedList<ListenerConfig>();
        nearCacheConfigMap = new ConcurrentHashMap<String, NearCacheConfig>();
        reliableTopicConfigMap = new ConcurrentHashMap<String, ClientReliableTopicConfig>();
        proxyFactoryConfigs = new LinkedList<ProxyFactoryConfig>();
        flakeIdGeneratorConfigMap = new ConcurrentHashMap<String, ClientFlakeIdGeneratorConfig>();
        queryCacheConfigs = new ConcurrentHashMap<String, Map<String, QueryCacheConfig>>();
        labels = new HashSet<String>();
        userContext = new ConcurrentHashMap<String, Object>();
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:executablestatementcount"})
    public ClientConfig(ClientConfig config) {
        properties = new Properties();
        properties.putAll(config.properties);
        groupConfig = new GroupConfig(config.groupConfig);
        securityConfig = new ClientSecurityConfig(config.securityConfig);
        networkConfig = new ClientNetworkConfig(config.networkConfig);
        loadBalancer = config.loadBalancer;
        listenerConfigs = new LinkedList<ListenerConfig>();
        for (ListenerConfig listenerConfig : config.listenerConfigs) {
            listenerConfigs.add(new ListenerConfig(listenerConfig));
        }
        executorPoolSize = config.executorPoolSize;
        instanceName = config.instanceName;
        configPatternMatcher = config.configPatternMatcher;
        nearCacheConfigMap = new ConcurrentHashMap<String, NearCacheConfig>();
        for (Entry<String, NearCacheConfig> entry : config.nearCacheConfigMap.entrySet()) {
            nearCacheConfigMap.put(entry.getKey(), new NearCacheConfig(entry.getValue()));
        }
        reliableTopicConfigMap = new ConcurrentHashMap<String, ClientReliableTopicConfig>();
        for (Entry<String, ClientReliableTopicConfig> entry : config.reliableTopicConfigMap.entrySet()) {
            reliableTopicConfigMap.put(entry.getKey(), new ClientReliableTopicConfig(entry.getValue()));
        }
        queryCacheConfigs = new ConcurrentHashMap<String, Map<String, QueryCacheConfig>>();
        for (Entry<String, Map<String, QueryCacheConfig>> entry : config.queryCacheConfigs.entrySet()) {
            Map<String, QueryCacheConfig> value = entry.getValue();

            ConcurrentHashMap<String, QueryCacheConfig> map = new ConcurrentHashMap<String, QueryCacheConfig>();
            for (Entry<String, QueryCacheConfig> cacheConfigEntry : value.entrySet()) {
                map.put(cacheConfigEntry.getKey(), new QueryCacheConfig(cacheConfigEntry.getValue()));
            }
            queryCacheConfigs.put(entry.getKey(), map);
        }
        serializationConfig = new SerializationConfig(config.serializationConfig);
        nativeMemoryConfig = new NativeMemoryConfig(config.nativeMemoryConfig);
        proxyFactoryConfigs = new LinkedList<ProxyFactoryConfig>();
        for (ProxyFactoryConfig factoryConfig : config.proxyFactoryConfigs) {
            proxyFactoryConfigs.add(new ProxyFactoryConfig(factoryConfig));
        }
        managedContext = config.managedContext;
        classLoader = config.classLoader;
        licenseKey = config.licenseKey;
        connectionStrategyConfig = new ClientConnectionStrategyConfig(config.connectionStrategyConfig);
        userCodeDeploymentConfig = new ClientUserCodeDeploymentConfig(config.userCodeDeploymentConfig);
        flakeIdGeneratorConfigMap = new ConcurrentHashMap<String, ClientFlakeIdGeneratorConfig>();
        for (Entry<String, ClientFlakeIdGeneratorConfig> entry : config.flakeIdGeneratorConfigMap.entrySet()) {
            flakeIdGeneratorConfigMap.put(entry.getKey(), new ClientFlakeIdGeneratorConfig(entry.getValue()));
        }
        labels = new HashSet<String>(config.labels);
        userContext = new ConcurrentHashMap<String, Object>(config.userContext);
    }

    /**
     * Sets the pattern matcher which is used to match item names to
     * configuration objects.
     * By default the {@link MatchingPointConfigPatternMatcher} is used.
     *
     * @param configPatternMatcher the pattern matcher
     * @throws IllegalArgumentException if the pattern matcher is {@code null}
     */
    public void setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        Preconditions.isNotNull(configPatternMatcher, "configPatternMatcher");
        this.configPatternMatcher = configPatternMatcher;
    }

    /**
     * Returns the pattern matcher which is used to match item names to
     * configuration objects.
     * By default the {@link MatchingPointConfigPatternMatcher} is used.
     *
     * @return the pattern matcher
     */
    public ConfigPatternMatcher getConfigPatternMatcher() {
        return configPatternMatcher;
    }

    /**
     * Gets a named property already set or from system properties if not exists.
     *
     * @param name property name
     * @return value of the property
     */
    public String getProperty(String name) {
        String value = properties.getProperty(name);
        return value != null ? value : System.getProperty(name);
    }

    /**
     * Sets the value of a named property.
     *
     * @param name  property name
     * @param value value of the property
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    /**
     * Gets {@link java.util.Properties} object
     *
     * @return {@link java.util.Properties} object
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * sets all properties
     *
     * @param properties {@link java.util.Properties} object
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig setProperties(final Properties properties) {
        Preconditions.isNotNull(properties, "properties");
        this.properties = properties;
        return this;
    }

    /**
     * Gets {@link com.hazelcast.client.config.ClientSecurityConfig} object
     *
     * @return {@link com.hazelcast.client.config.ClientSecurityConfig}
     * @see com.hazelcast.client.config.ClientSecurityConfig
     */
    public ClientSecurityConfig getSecurityConfig() {
        return securityConfig;
    }

    /**
     * Sets {@link com.hazelcast.client.config.ClientSecurityConfig} object
     *
     * @param securityConfig {@link com.hazelcast.client.config.ClientSecurityConfig} to be set
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.client.config.ClientSecurityConfig
     */
    public ClientConfig setSecurityConfig(ClientSecurityConfig securityConfig) {
        Preconditions.isNotNull(securityConfig, "securityConfig");
        this.securityConfig = securityConfig;
        return this;
    }

    /**
     * Gets {@link com.hazelcast.client.config.ClientNetworkConfig}
     *
     * @return {@link com.hazelcast.client.config.ClientNetworkConfig}
     * @see com.hazelcast.client.config.ClientNetworkConfig
     */
    public ClientNetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    /**
     * Sets {@link com.hazelcast.client.config.ClientNetworkConfig}
     *
     * @param networkConfig {@link com.hazelcast.client.config.ClientNetworkConfig} to be set
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.client.config.ClientNetworkConfig
     */
    public ClientConfig setNetworkConfig(ClientNetworkConfig networkConfig) {
        Preconditions.isNotNull(networkConfig, "networkConfig");
        this.networkConfig = networkConfig;
        return this;
    }

    /**
     * Adds a ClientReliableTopicConfig.
     *
     * @param reliableTopicConfig the ClientReliableTopicConfig to add
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig addReliableTopicConfig(ClientReliableTopicConfig reliableTopicConfig) {
        reliableTopicConfigMap.put(reliableTopicConfig.getName(), reliableTopicConfig);
        return this;
    }

    /**
     * Gets the ClientReliableTopicConfig for a given reliable topic name.
     *
     * @param name the name of the reliable topic
     * @return the found config. If none is found, a default configured one is returned.
     */
    public ClientReliableTopicConfig getReliableTopicConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, reliableTopicConfigMap, name,
                ClientReliableTopicConfig.class, new BiConsumer<ClientReliableTopicConfig, String>() {
                    @Override
                    public void accept(ClientReliableTopicConfig clientReliableTopicConfig, String name) {
                        clientReliableTopicConfig.setName(name);
                    }
                });
    }

    /**
     * please use {@link ClientConfig#addNearCacheConfig(NearCacheConfig)}
     *
     * @param name            name of the IMap / ICache that Near Cache config will be applied to
     * @param nearCacheConfig nearCacheConfig
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    @Deprecated
    public ClientConfig addNearCacheConfig(String name, NearCacheConfig nearCacheConfig) {
        nearCacheConfig.setName(name);
        return addNearCacheConfig(nearCacheConfig);
    }

    /**
     * Helper method to add a new NearCacheConfig
     *
     * @param nearCacheConfig {@link com.hazelcast.config.NearCacheConfig}
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.config.NearCacheConfig
     */
    public ClientConfig addNearCacheConfig(NearCacheConfig nearCacheConfig) {
        nearCacheConfigMap.put(nearCacheConfig.getName(), nearCacheConfig);
        return this;
    }

    /**
     * Helper method to add a new ListenerConfig
     *
     * @param listenerConfig ListenerConfig
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.config.ListenerConfig
     */
    public ClientConfig addListenerConfig(ListenerConfig listenerConfig) {
        getListenerConfigs().add(listenerConfig);
        return this;
    }

    /**
     * Helper method to add a new {@link ProxyFactoryConfig}
     *
     * @param proxyFactoryConfig {@link ProxyFactoryConfig}
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.client.config.ProxyFactoryConfig
     */
    public ClientConfig addProxyFactoryConfig(ProxyFactoryConfig proxyFactoryConfig) {
        this.proxyFactoryConfigs.add(proxyFactoryConfig);
        return this;
    }

    /**
     * Gets the {@link NearCacheConfig} configured for the map / cache with name
     *
     * @param name name of the map / cache
     * @return Configured {@link NearCacheConfig}
     * @see com.hazelcast.config.NearCacheConfig
     */
    public NearCacheConfig getNearCacheConfig(String name) {
        NearCacheConfig nearCacheConfig = lookupByPattern(configPatternMatcher, nearCacheConfigMap, name);
        if (nearCacheConfig == null) {
            nearCacheConfig = nearCacheConfigMap.get("default");
            if (nearCacheConfig != null) {
                // if there is a default config we have to clone it,
                // otherwise you will modify the same instances via different Near Cache names
                nearCacheConfig = new NearCacheConfig(nearCacheConfig);
            }
        }
        return nearCacheConfig;
    }

    /**
     * Map of all configured NearCacheConfig's with the name key and configuration as the value
     *
     * @return map of NearCacheConfig
     * @see com.hazelcast.config.NearCacheConfig
     */
    public Map<String, NearCacheConfig> getNearCacheConfigMap() {
        return nearCacheConfigMap;
    }

    /**
     * Sets all {@link NearCacheConfig}'s with the provided map
     *
     * @param nearCacheConfigMap map of (name, {@link NearCacheConfig})
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig setNearCacheConfigMap(Map<String, NearCacheConfig> nearCacheConfigMap) {
        Preconditions.isNotNull(nearCacheConfigMap, "nearCacheConfigMap");
        this.nearCacheConfigMap.clear();
        this.nearCacheConfigMap.putAll(nearCacheConfigMap);
        for (Entry<String, NearCacheConfig> entry : this.nearCacheConfigMap.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the map of {@link FlakeIdGenerator} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the map configurations mapped by config name
     */
    public Map<String, ClientFlakeIdGeneratorConfig> getFlakeIdGeneratorConfigMap() {
        return flakeIdGeneratorConfigMap;
    }

    /**
     * Returns a {@link ClientFlakeIdGeneratorConfig} configuration for the given flake ID generator name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code "default"}.
     *
     * @param name name of the flake ID generator config
     * @return the flake ID generator configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see com.hazelcast.partition.strategy.StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ClientFlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String name) {
        String baseName = getBaseName(name);
        ClientFlakeIdGeneratorConfig config = lookupByPattern(configPatternMatcher, flakeIdGeneratorConfigMap, baseName);
        if (config != null) {
            return config;
        }
        return getFlakeIdGeneratorConfig("default");
    }

    /**
     * Returns the {@link ClientFlakeIdGeneratorConfig} for the given name, creating
     * one if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addFlakeIdGeneratorConfig(ClientFlakeIdGeneratorConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the flake ID generator config
     * @return the cache configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see com.hazelcast.partition.strategy.StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ClientFlakeIdGeneratorConfig getFlakeIdGeneratorConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, getFlakeIdGeneratorConfigMap(), name,
                ClientFlakeIdGeneratorConfig.class, new BiConsumer<ClientFlakeIdGeneratorConfig, String>() {
                    @Override
                    public void accept(ClientFlakeIdGeneratorConfig flakeIdGeneratorConfig, String name) {
                        flakeIdGeneratorConfig.setName(name);
                    }
                });
    }

    /**
     * Adds a flake ID generator configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param config the flake ID configuration
     * @return this config instance
     */
    public ClientConfig addFlakeIdGeneratorConfig(ClientFlakeIdGeneratorConfig config) {
        flakeIdGeneratorConfigMap.put(config.getName(), config);
        return this;
    }

    /**
     * Sets the map of {@link FlakeIdGenerator} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param map the FlakeIdGenerator configuration map to set
     * @return this config instance
     */
    public ClientConfig setFlakeIdGeneratorConfigMap(Map<String, ClientFlakeIdGeneratorConfig> map) {
        Preconditions.isNotNull(map, "flakeIdGeneratorConfigMap");
        flakeIdGeneratorConfigMap.clear();
        flakeIdGeneratorConfigMap.putAll(map);
        for (Entry<String, ClientFlakeIdGeneratorConfig> entry : map.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Sets the map of {@link ClientReliableTopicConfig},
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param map the FlakeIdGenerator configuration map to set
     * @return this config instance
     */
    public ClientConfig setReliableTopicConfigMap(Map<String, ClientReliableTopicConfig> map) {
        Preconditions.isNotNull(map, "reliableTopicConfigMap");
        reliableTopicConfigMap.clear();
        reliableTopicConfigMap.putAll(map);
        for (Entry<String, ClientReliableTopicConfig> entry : map.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the map of reliable topic configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the map configurations mapped by config name
     */
    public Map<String, ClientReliableTopicConfig> getReliableTopicConfigMap() {
        return reliableTopicConfigMap;
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

    /**
     * Gets {@link com.hazelcast.security.Credentials}
     *
     * @return {@link com.hazelcast.security.Credentials}
     * @see com.hazelcast.security.Credentials
     */
    public Credentials getCredentials() {
        return securityConfig.getCredentials();
    }

    /**
     * Sets {@link com.hazelcast.security.Credentials}
     *
     * @param credentials {@link com.hazelcast.security.Credentials}
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
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

    /**
     * gets {@link GroupConfig}
     *
     * @return {@link GroupConfig}
     * @see com.hazelcast.config.GroupConfig
     */
    public GroupConfig getGroupConfig() {
        return groupConfig;
    }

    /**
     * Sets {@link GroupConfig}
     *
     * @param groupConfig {@link GroupConfig}
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig setGroupConfig(GroupConfig groupConfig) {
        Preconditions.isNotNull(groupConfig, "groupConfig");
        this.groupConfig = groupConfig;
        return this;
    }

    /**
     * Gets list of all configured {@link ListenerConfig}'s
     *
     * @return {@link ListenerConfig}
     * @see com.hazelcast.config.ListenerConfig
     */
    public List<ListenerConfig> getListenerConfigs() {
        return listenerConfigs;
    }

    /**
     * Sets all {@link ListenerConfig}'s
     *
     * @param listenerConfigs list of {@link ListenerConfig}
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.config.ListenerConfig
     */
    public ClientConfig setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        Preconditions.isNotNull(listenerConfigs, "listenerConfigs");
        this.listenerConfigs.clear();
        this.listenerConfigs.addAll(listenerConfigs);
        return this;
    }

    /**
     * Gets LoadBalancer
     *
     * @return LoadBalancer
     * @see com.hazelcast.client.LoadBalancer
     */
    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * Sets the {@link LoadBalancer}
     *
     * @param loadBalancer {@link LoadBalancer}
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.client.LoadBalancer
     */
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

    /**
     * Gets the classLoader
     *
     * @return configured classLoader, null if not yet configured
     */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Sets the classLoader which is used by serialization and listener configuration
     *
     * @param classLoader
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }

    /**
     * Gets {@link ManagedContext}
     *
     * @return {@link ManagedContext}
     * @see com.hazelcast.core.ManagedContext
     */
    public ManagedContext getManagedContext() {
        return managedContext;
    }

    /**
     * Sets {@link ManagedContext}
     *
     * @param managedContext {@link ManagedContext}
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.core.ManagedContext
     */
    public ClientConfig setManagedContext(ManagedContext managedContext) {
        this.managedContext = managedContext;
        return this;
    }

    /**
     * Pool size for internal ExecutorService which handles responses etc.
     *
     * @return int Executor pool size.
     */
    public int getExecutorPoolSize() {
        return executorPoolSize;
    }

    /**
     * Sets Client side Executor pool size.
     *
     * @param executorPoolSize pool size
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig setExecutorPoolSize(int executorPoolSize) {
        this.executorPoolSize = executorPoolSize;
        return this;
    }

    /**
     * Gets list of {@link com.hazelcast.client.config.ProxyFactoryConfig}
     *
     * @return list of {@link com.hazelcast.client.config.ProxyFactoryConfig}
     * @see com.hazelcast.client.config.ProxyFactoryConfig
     */
    public List<ProxyFactoryConfig> getProxyFactoryConfigs() {
        return proxyFactoryConfigs;
    }

    /**
     * Sets the {@link ProxyFactoryConfig}
     *
     * @param proxyFactoryConfigs list to assign
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig setProxyFactoryConfigs(List<ProxyFactoryConfig> proxyFactoryConfigs) {
        Preconditions.isNotNull(proxyFactoryConfigs, "proxyFactoryConfigs");
        this.proxyFactoryConfigs.clear();
        this.proxyFactoryConfigs.addAll(proxyFactoryConfigs);
        return this;
    }

    /**
     * Gets {@link com.hazelcast.config.SerializationConfig}
     *
     * @return SerializationConfig
     * @see com.hazelcast.config.SerializationConfig
     */
    public SerializationConfig getSerializationConfig() {
        return serializationConfig;
    }

    /**
     * Sets {@link com.hazelcast.config.SerializationConfig}
     *
     * @param serializationConfig SerializationConfig
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.config.SerializationConfig
     */
    public ClientConfig setSerializationConfig(SerializationConfig serializationConfig) {
        Preconditions.isNotNull(serializationConfig, "serializationConfig");
        this.serializationConfig = serializationConfig;
        return this;
    }

    public NativeMemoryConfig getNativeMemoryConfig() {
        return nativeMemoryConfig;
    }

    public ClientConfig setNativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        Preconditions.isNotNull(nativeMemoryConfig, "nativeMemoryConfig");
        this.nativeMemoryConfig = nativeMemoryConfig;
        return this;
    }

    public String getLicenseKey() {
        return licenseKey;
    }

    /**
     * @deprecated As of Hazelcast 3.10.3, enterprise license keys are required only for members, and not for clients
     */
    public ClientConfig setLicenseKey(final String licenseKey) {
        this.licenseKey = licenseKey;
        return this;
    }

    public ClientConfig addQueryCacheConfig(String mapName, QueryCacheConfig queryCacheConfig) {
        Map<String, Map<String, QueryCacheConfig>> queryCacheConfigsPerMap = getQueryCacheConfigs();
        String queryCacheName = queryCacheConfig.getName();
        Map<String, QueryCacheConfig> queryCacheConfigs = queryCacheConfigsPerMap.get(mapName);
        if (queryCacheConfigs != null) {
            checkFalse(queryCacheConfigs.containsKey(queryCacheName),
                    "A query cache already exists with name = [" + queryCacheName + ']');
        } else {
            queryCacheConfigs = new ConcurrentHashMap<String, QueryCacheConfig>();
            queryCacheConfigsPerMap.put(mapName, queryCacheConfigs);
        }

        queryCacheConfigs.put(queryCacheName, queryCacheConfig);
        return this;

    }

    public Map<String, Map<String, QueryCacheConfig>> getQueryCacheConfigs() {
        return queryCacheConfigs;
    }

    public void setQueryCacheConfigs(Map<String, Map<String, QueryCacheConfig>> queryCacheConfigs) {
        Preconditions.isNotNull(queryCacheConfigs, "queryCacheConfigs");
        this.queryCacheConfigs.clear();
        this.queryCacheConfigs.putAll(queryCacheConfigs);
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public ClientConnectionStrategyConfig getConnectionStrategyConfig() {
        return connectionStrategyConfig;
    }

    public ClientConfig setConnectionStrategyConfig(ClientConnectionStrategyConfig connectionStrategyConfig) {
        Preconditions.isNotNull(connectionStrategyConfig, "connectionStrategyConfig");
        this.connectionStrategyConfig = connectionStrategyConfig;
        return this;
    }

    /**
     * Get current configuration of User Code Deployment.
     *
     * @return User Code Deployment configuration
     * @since 3.9
     */
    public ClientUserCodeDeploymentConfig getUserCodeDeploymentConfig() {
        return userCodeDeploymentConfig;
    }

    /**
     * Set User Code Deployment configuration
     *
     * @param userCodeDeploymentConfig
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @since 3.9
     */
    public ClientConfig setUserCodeDeploymentConfig(ClientUserCodeDeploymentConfig userCodeDeploymentConfig) {
        Preconditions.isNotNull(userCodeDeploymentConfig, "userCodeDeploymentConfig");
        this.userCodeDeploymentConfig = userCodeDeploymentConfig;
        return this;
    }

    /**
     * @param mapName   The name of the map for which the query cache config is to be returned.
     * @param cacheName The name of the query cache.
     * @return The query cache config. If the config does not exist, it is created.
     */
    public QueryCacheConfig getOrCreateQueryCacheConfig(String mapName, String cacheName) {
        Map<String, Map<String, QueryCacheConfig>> allQueryCacheConfig = getQueryCacheConfigs();

        Map<String, QueryCacheConfig> queryCacheConfigsForMap =
                lookupByPattern(configPatternMatcher, allQueryCacheConfig, mapName);
        if (queryCacheConfigsForMap == null) {
            queryCacheConfigsForMap = new HashMap<String, QueryCacheConfig>();
            allQueryCacheConfig.put(mapName, queryCacheConfigsForMap);
        }

        QueryCacheConfig queryCacheConfig = lookupByPattern(configPatternMatcher, queryCacheConfigsForMap, cacheName);
        if (queryCacheConfig == null) {
            queryCacheConfig = new QueryCacheConfig(cacheName);
            queryCacheConfigsForMap.put(cacheName, queryCacheConfig);
        }

        return queryCacheConfig;
    }

    /**
     * @param mapName   The name of the map for which the query cache config is to be returned.
     * @param cacheName The name of the query cache.
     * @return The query cache config. If no such config exist null is returned.
     */
    public QueryCacheConfig getOrNullQueryCacheConfig(String mapName, String cacheName) {
        if (queryCacheConfigs == null) {
            return null;
        }

        Map<String, QueryCacheConfig> queryCacheConfigsForMap = lookupByPattern(configPatternMatcher, queryCacheConfigs, mapName);
        if (queryCacheConfigsForMap == null) {
            return null;
        }

        return lookupByPattern(configPatternMatcher, queryCacheConfigsForMap, cacheName);
    }

    /**
     * Adds a label for this client {@link com.hazelcast.core.Client} available
     *
     * @param label The label to be added.
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig addLabel(String label) {
        Preconditions.isNotNull(label, "label");
        labels.add(label);
        return this;
    }

    /**
     * @return all the labels assigned to this client
     */
    public Set<String> getLabels() {
        return labels;
    }

    /**
     * Set labels for the client. Deletes old labels if added earlier.
     *
     * @param labels The labels to be set
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig setLabels(Set<String> labels) {
        Preconditions.isNotNull(labels, "labels");
        this.labels.clear();
        this.labels.addAll(labels);
        return this;
    }

    public ClientConfig setUserContext(ConcurrentMap<String, Object> userContext) {
        Preconditions.isNotNull(userContext, "userContext");
        this.userContext.clear();
        this.userContext.putAll(userContext);
        return this;
    }

    public ConcurrentMap<String, Object> getUserContext() {
        return userContext;
    }

    @Override
    @SuppressWarnings({"checkstyle:methodlength", "checkstyle:cyclomaticcomplexity",
            "checkstyle:npathcomplexity", "checkstyle:executablestatementcount"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientConfig that = (ClientConfig) o;

        if (executorPoolSize != that.executorPoolSize) {
            return false;
        }
        if (!properties.equals(that.properties)) {
            return false;
        }
        if (!groupConfig.equals(that.groupConfig)) {
            return false;
        }
        if (!securityConfig.equals(that.securityConfig)) {
            return false;
        }
        if (!networkConfig.equals(that.networkConfig)) {
            return false;
        }
        if (loadBalancer != null ? !loadBalancer.equals(that.loadBalancer) : that.loadBalancer != null) {
            return false;
        }
        if (!listenerConfigs.equals(that.listenerConfigs)) {
            return false;
        }
        if (instanceName != null ? !instanceName.equals(that.instanceName) : that.instanceName != null) {
            return false;
        }
        if (configPatternMatcher != null
                ? !configPatternMatcher.equals(that.configPatternMatcher) : that.configPatternMatcher != null) {
            return false;
        }
        if (!nearCacheConfigMap.equals(that.nearCacheConfigMap)) {
            return false;
        }
        if (!reliableTopicConfigMap.equals(that.reliableTopicConfigMap)) {
            return false;
        }
        if (!queryCacheConfigs.equals(that.queryCacheConfigs)) {
            return false;
        }
        if (!serializationConfig.equals(that.serializationConfig)) {
            return false;
        }
        if (!nativeMemoryConfig.equals(that.nativeMemoryConfig)) {
            return false;
        }
        if (!proxyFactoryConfigs.equals(that.proxyFactoryConfigs)) {
            return false;
        }
        if (managedContext != null ? !managedContext.equals(that.managedContext) : that.managedContext != null) {
            return false;
        }
        if (classLoader != null ? !classLoader.equals(that.classLoader) : that.classLoader != null) {
            return false;
        }
        if (licenseKey != null ? !licenseKey.equals(that.licenseKey) : that.licenseKey != null) {
            return false;
        }
        if (!connectionStrategyConfig.equals(that.connectionStrategyConfig)) {
            return false;
        }
        if (!userCodeDeploymentConfig.equals(that.userCodeDeploymentConfig)) {
            return false;
        }
        if (!flakeIdGeneratorConfigMap.equals(that.flakeIdGeneratorConfigMap)) {
            return false;
        }
        if (!labels.equals(that.labels)) {
            return false;
        }
        return userContext.equals(that.userContext);
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    public int hashCode() {
        int result = properties.hashCode();
        result = 31 * result + groupConfig.hashCode();
        result = 31 * result + securityConfig.hashCode();
        result = 31 * result + networkConfig.hashCode();
        result = 31 * result + (loadBalancer != null ? loadBalancer.hashCode() : 0);
        result = 31 * result + listenerConfigs.hashCode();
        result = 31 * result + executorPoolSize;
        result = 31 * result + (instanceName != null ? instanceName.hashCode() : 0);
        result = 31 * result + (configPatternMatcher != null ? configPatternMatcher.hashCode() : 0);
        result = 31 * result + nearCacheConfigMap.hashCode();
        result = 31 * result + reliableTopicConfigMap.hashCode();
        result = 31 * result + queryCacheConfigs.hashCode();
        result = 31 * result + serializationConfig.hashCode();
        result = 31 * result + nativeMemoryConfig.hashCode();
        result = 31 * result + proxyFactoryConfigs.hashCode();
        result = 31 * result + (managedContext != null ? managedContext.hashCode() : 0);
        result = 31 * result + (classLoader != null ? classLoader.hashCode() : 0);
        result = 31 * result + (licenseKey != null ? licenseKey.hashCode() : 0);
        result = 31 * result + connectionStrategyConfig.hashCode();
        result = 31 * result + userCodeDeploymentConfig.hashCode();
        result = 31 * result + flakeIdGeneratorConfigMap.hashCode();
        result = 31 * result + labels.hashCode();
        result = 31 * result + userContext.hashCode();
        return result;
    }
}
