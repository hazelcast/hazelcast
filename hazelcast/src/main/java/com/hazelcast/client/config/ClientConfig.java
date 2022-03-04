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

import com.hazelcast.client.Client;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.impl.XmlClientConfigLocator;
import com.hazelcast.client.config.impl.YamlClientConfigLocator;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.InstanceTrackingConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.config.ConfigUtils;
import com.hazelcast.internal.config.override.ExternalConfigurationOverride;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.security.Credentials;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.validateSuffixInSystemProperty;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.isNotNull;
import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;

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
     * Load balancer class name. Used internally with declarative configuration.
     */
    private String loadBalancerClassName;

    /**
     * List of listeners that Hazelcast will automatically add as a part of initialization process.
     * Currently only supports {@link com.hazelcast.core.LifecycleListener}.
     */
    private final List<ListenerConfig> listenerConfigs;

    private String instanceName;
    private String clusterName = Config.DEFAULT_CLUSTER_NAME;
    private ConfigPatternMatcher configPatternMatcher = new MatchingPointConfigPatternMatcher();
    private final Map<String, NearCacheConfig> nearCacheConfigMap;
    private final Map<String, ClientReliableTopicConfig> reliableTopicConfigMap;
    private final Map<String, Map<String, QueryCacheConfig>> queryCacheConfigs;
    private SerializationConfig serializationConfig = new SerializationConfig();
    private NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig();
    private final List<ProxyFactoryConfig> proxyFactoryConfigs;
    private ManagedContext managedContext;
    private ClassLoader classLoader;
    private ClientConnectionStrategyConfig connectionStrategyConfig = new ClientConnectionStrategyConfig();
    private ClientUserCodeDeploymentConfig userCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
    private boolean backupAckToClientEnabled = true;
    private final Map<String, ClientFlakeIdGeneratorConfig> flakeIdGeneratorConfigMap;
    private final Set<String> labels;
    private final ConcurrentMap<String, Object> userContext;
    private ClientMetricsConfig metricsConfig = new ClientMetricsConfig();
    private InstanceTrackingConfig instanceTrackingConfig = new InstanceTrackingConfig();

    public ClientConfig() {
        listenerConfigs = new LinkedList<>();
        nearCacheConfigMap = new ConcurrentHashMap<>();
        reliableTopicConfigMap = new ConcurrentHashMap<>();
        proxyFactoryConfigs = new LinkedList<>();
        flakeIdGeneratorConfigMap = new ConcurrentHashMap<>();
        queryCacheConfigs = new ConcurrentHashMap<>();
        labels = new HashSet<>();
        userContext = new ConcurrentHashMap<>();
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:executablestatementcount"})
    public ClientConfig(ClientConfig config) {
        properties = new Properties();
        properties.putAll(config.properties);
        clusterName = config.clusterName;
        securityConfig = new ClientSecurityConfig(config.securityConfig);
        networkConfig = new ClientNetworkConfig(config.networkConfig);
        loadBalancer = config.loadBalancer;
        loadBalancerClassName = config.loadBalancerClassName;
        listenerConfigs = new LinkedList<>();
        for (ListenerConfig listenerConfig : config.listenerConfigs) {
            listenerConfigs.add(new ListenerConfig(listenerConfig));
        }
        instanceName = config.instanceName;
        configPatternMatcher = config.configPatternMatcher;
        nearCacheConfigMap = new ConcurrentHashMap<>();
        for (Entry<String, NearCacheConfig> entry : config.nearCacheConfigMap.entrySet()) {
            nearCacheConfigMap.put(entry.getKey(), new NearCacheConfig(entry.getValue()));
        }
        reliableTopicConfigMap = new ConcurrentHashMap<>();
        for (Entry<String, ClientReliableTopicConfig> entry : config.reliableTopicConfigMap.entrySet()) {
            reliableTopicConfigMap.put(entry.getKey(), new ClientReliableTopicConfig(entry.getValue()));
        }
        queryCacheConfigs = new ConcurrentHashMap<>();
        for (Entry<String, Map<String, QueryCacheConfig>> entry : config.queryCacheConfigs.entrySet()) {
            Map<String, QueryCacheConfig> value = entry.getValue();

            ConcurrentHashMap<String, QueryCacheConfig> map = new ConcurrentHashMap<>();
            for (Entry<String, QueryCacheConfig> cacheConfigEntry : value.entrySet()) {
                map.put(cacheConfigEntry.getKey(), new QueryCacheConfig(cacheConfigEntry.getValue()));
            }
            queryCacheConfigs.put(entry.getKey(), map);
        }
        serializationConfig = new SerializationConfig(config.serializationConfig);
        nativeMemoryConfig = new NativeMemoryConfig(config.nativeMemoryConfig);
        proxyFactoryConfigs = new LinkedList<>();
        for (ProxyFactoryConfig factoryConfig : config.proxyFactoryConfigs) {
            proxyFactoryConfigs.add(new ProxyFactoryConfig(factoryConfig));
        }
        managedContext = config.managedContext;
        classLoader = config.classLoader;
        connectionStrategyConfig = new ClientConnectionStrategyConfig(config.connectionStrategyConfig);
        userCodeDeploymentConfig = new ClientUserCodeDeploymentConfig(config.userCodeDeploymentConfig);
        backupAckToClientEnabled = config.backupAckToClientEnabled;
        flakeIdGeneratorConfigMap = new ConcurrentHashMap<>();
        for (Entry<String, ClientFlakeIdGeneratorConfig> entry : config.flakeIdGeneratorConfigMap.entrySet()) {
            flakeIdGeneratorConfigMap.put(entry.getKey(), new ClientFlakeIdGeneratorConfig(entry.getValue()));
        }
        labels = new HashSet<>(config.labels);
        userContext = new ConcurrentHashMap<>(config.userContext);
        metricsConfig = new ClientMetricsConfig(config.metricsConfig);
        instanceTrackingConfig = new InstanceTrackingConfig(config.instanceTrackingConfig);
    }

    /**
     * Populates Hazelcast {@link ClientConfig} object from an external configuration file.
     * <p>
     * It tries to load Hazelcast Client configuration from a list of well-known locations,
     * and then applies overrides found in environment variables/system properties
     *
     * When no location contains Hazelcast Client configuration then it returns default.
     * <p>
     * Note that the same mechanism is used when calling
     * {@link com.hazelcast.client.HazelcastClient#newHazelcastClient()}.
     *
     * @return ClientConfig created from a file when exists, otherwise default.
     */
    public static ClientConfig load() {
        return new ExternalConfigurationOverride().overwriteClientConfig(loadFromFile());
    }

    private static ClientConfig loadFromFile() {
        validateSuffixInSystemProperty(SYSPROP_CLIENT_CONFIG);

        XmlClientConfigLocator xmlConfigLocator = new XmlClientConfigLocator();
        YamlClientConfigLocator yamlConfigLocator = new YamlClientConfigLocator();

        if (xmlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading XML config from the configuration provided in system property
            return new XmlClientConfigBuilder(xmlConfigLocator).build();
        } else if (yamlConfigLocator.locateFromSystemProperty()) {
            // 2. Try loading YAML config from the configuration provided in system property
            return new YamlClientConfigBuilder(yamlConfigLocator).build();
        } else if (xmlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 3. Try loading XML config from the working directory or from the classpath
            return new XmlClientConfigBuilder(xmlConfigLocator).build();
        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 4. Try loading YAML config from the working directory or from the classpath
            return new YamlClientConfigBuilder(yamlConfigLocator).build();
        } else {
            // 5. Loading the default XML configuration file
            xmlConfigLocator.locateDefault();
            return new XmlClientConfigBuilder(xmlConfigLocator).build();
        }
    }

    /**
     * Sets the pattern matcher which is used to match item names to
     * configuration objects.
     * By default the {@link MatchingPointConfigPatternMatcher} is used.
     *
     * @param configPatternMatcher the pattern matcher
     * @return this configuration
     * @throws IllegalArgumentException if the pattern matcher is {@code null}
     */
    public ClientConfig setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        isNotNull(configPatternMatcher, "configPatternMatcher");
        this.configPatternMatcher = configPatternMatcher;
        return this;
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
        isNotNull(properties, "properties");
        this.properties = properties;
        return this;
    }

    /**
     * Gets {@link ClientSecurityConfig} object which allows client credentials configuration.
     *
     * @return {@link ClientSecurityConfig} instance
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
        isNotNull(securityConfig, "securityConfig");
        this.securityConfig = securityConfig;
        return this;
    }

    /**
     * Shortcut for {@code getSecurityConfig().setCredentials()}
     * @param credentials Credentials instance to be set
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig setCredentials(Credentials credentials) {
        getSecurityConfig().setCredentials(credentials);
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
        isNotNull(networkConfig, "networkConfig");
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
                ClientReliableTopicConfig.class, ClientReliableTopicConfig::setName);
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
        isNotNull(nearCacheConfigMap, "nearCacheConfigMap");
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
     * @throws InvalidConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
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
     * @throws InvalidConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ClientFlakeIdGeneratorConfig getFlakeIdGeneratorConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, getFlakeIdGeneratorConfigMap(), name,
                ClientFlakeIdGeneratorConfig.class, ClientFlakeIdGeneratorConfig::setName);
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
        isNotNull(map, "flakeIdGeneratorConfigMap");
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
        isNotNull(map, "reliableTopicConfigMap");
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
        isNotNull(listenerConfigs, "listenerConfigs");
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
     * Sets the {@link LoadBalancer}.
     * <p>
     * If a load balancer class name was set, it will be removed.
     *
     * @param loadBalancer {@link LoadBalancer}
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.client.LoadBalancer
     */
    public ClientConfig setLoadBalancer(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
        this.loadBalancerClassName = null;
        return this;
    }

    /**
     * Gets load balancer class name
     *
     * @return load balancer class name
     * @see com.hazelcast.client.LoadBalancer
     */
    public String getLoadBalancerClassName() {
        return loadBalancerClassName;
    }

    /**
     * Sets load balancer class name.
     * <p>
     * If a load balancer implementation was set, it will be removed.
     *
     * @param loadBalancerClassName {@link LoadBalancer}
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @see com.hazelcast.client.LoadBalancer
     */
    public ClientConfig setLoadBalancerClassName(@Nonnull String loadBalancerClassName) {
        this.loadBalancerClassName = checkHasText(loadBalancerClassName, "Load balancer class name must contain text");
        this.loadBalancer = null;
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
     * @param classLoader the classLoader
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
        isNotNull(proxyFactoryConfigs, "proxyFactoryConfigs");
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
        isNotNull(serializationConfig, "serializationConfig");
        this.serializationConfig = serializationConfig;
        return this;
    }

    public NativeMemoryConfig getNativeMemoryConfig() {
        return nativeMemoryConfig;
    }

    public ClientConfig setNativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        isNotNull(nativeMemoryConfig, "nativeMemoryConfig");
        this.nativeMemoryConfig = nativeMemoryConfig;
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
            queryCacheConfigs = new ConcurrentHashMap<>();
            queryCacheConfigsPerMap.put(mapName, queryCacheConfigs);
        }

        queryCacheConfigs.put(queryCacheName, queryCacheConfig);
        return this;

    }

    public Map<String, Map<String, QueryCacheConfig>> getQueryCacheConfigs() {
        return queryCacheConfigs;
    }

    public ClientConfig setQueryCacheConfigs(Map<String, Map<String, QueryCacheConfig>> queryCacheConfigs) {
        isNotNull(queryCacheConfigs, "queryCacheConfigs");
        this.queryCacheConfigs.clear();
        this.queryCacheConfigs.putAll(queryCacheConfigs);
        return this;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public ClientConfig setInstanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    /**
     * Returns the configured cluster name. The name is sent as part of client authentication message and may be verified on the
     * member.
     *
     * @return the configured cluster name
     */
    public String getClusterName() {
        return clusterName;
    }

    public ClientConfig setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public ClientConnectionStrategyConfig getConnectionStrategyConfig() {
        return connectionStrategyConfig;
    }

    public ClientConfig setConnectionStrategyConfig(ClientConnectionStrategyConfig connectionStrategyConfig) {
        isNotNull(connectionStrategyConfig, "connectionStrategyConfig");
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
     * @param userCodeDeploymentConfig the configuration of User Code Deployment
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     * @since 3.9
     */
    public ClientConfig setUserCodeDeploymentConfig(ClientUserCodeDeploymentConfig userCodeDeploymentConfig) {
        isNotNull(userCodeDeploymentConfig, "userCodeDeploymentConfig");
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
            queryCacheConfigsForMap = new HashMap<>();
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
     * Adds a label for this client {@link Client} available
     *
     * @param label The label to be added.
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig addLabel(String label) {
        isNotNull(label, "label");
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
        isNotNull(labels, "labels");
        this.labels.clear();
        this.labels.addAll(labels);
        return this;
    }

    public ClientConfig setUserContext(ConcurrentMap<String, Object> userContext) {
        isNotNull(userContext, "userContext");
        this.userContext.clear();
        this.userContext.putAll(userContext);
        return this;
    }

    public ConcurrentMap<String, Object> getUserContext() {
        return userContext;
    }

    /**
     * This feature reduces number of hops and increase performance for smart clients.
     * It is enabled by default for smart clients.
     * This config has no effect for unisocket clients.
     *
     * @param backupAckToClientEnabled enables client to get backup acknowledgements directly from the member
     *                                 that backups are applied
     * @return configured {@link com.hazelcast.client.config.ClientConfig} for chaining
     */
    public ClientConfig setBackupAckToClientEnabled(boolean backupAckToClientEnabled) {
        this.backupAckToClientEnabled = backupAckToClientEnabled;
        return this;
    }

    /**
     * Note that backup acks to client can be enabled only for smart client.
     * This config has no effect for unisocket clients.
     *
     * @return true if backup acknowledgements comes to client
     */
    public boolean isBackupAckToClientEnabled() {
        return backupAckToClientEnabled;
    }

    /**
     * Returns the metrics collection config.
     */
    @Nonnull
    public ClientMetricsConfig getMetricsConfig() {
        return metricsConfig;
    }

    /**
     * Sets the metrics collection config.
     */
    @Nonnull
    public ClientConfig setMetricsConfig(@Nonnull ClientMetricsConfig metricsConfig) {
        Preconditions.checkNotNull(metricsConfig, "metricsConfig");
        this.metricsConfig = metricsConfig;
        return this;
    }

    /**
     * Returns the configuration for tracking use of this Hazelcast instance.
     */
    @Nonnull
    public InstanceTrackingConfig getInstanceTrackingConfig() {
        return instanceTrackingConfig;
    }

    /**
     * Returns the configuration for tracking use of this Hazelcast instance.
     */
    @Nonnull
    public ClientConfig setInstanceTrackingConfig(@Nonnull InstanceTrackingConfig instanceTrackingConfig) {
        Preconditions.checkNotNull(instanceTrackingConfig, "instanceTrackingConfig");
        this.instanceTrackingConfig = instanceTrackingConfig;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(backupAckToClientEnabled, classLoader, clusterName, configPatternMatcher, connectionStrategyConfig,
                flakeIdGeneratorConfigMap, instanceName, labels, listenerConfigs, loadBalancer, loadBalancerClassName,
                managedContext, metricsConfig, nativeMemoryConfig, nearCacheConfigMap, networkConfig, properties,
                proxyFactoryConfigs, queryCacheConfigs, reliableTopicConfigMap, securityConfig, serializationConfig,
                userCodeDeploymentConfig, userContext, instanceTrackingConfig);
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ClientConfig other = (ClientConfig) obj;
        return backupAckToClientEnabled == other.backupAckToClientEnabled && Objects.equals(classLoader, other.classLoader)
                && Objects.equals(clusterName, other.clusterName)
                && Objects.equals(configPatternMatcher, other.configPatternMatcher)
                && Objects.equals(connectionStrategyConfig, other.connectionStrategyConfig)
                && Objects.equals(flakeIdGeneratorConfigMap, other.flakeIdGeneratorConfigMap)
                && Objects.equals(instanceName, other.instanceName) && Objects.equals(labels, other.labels)
                && Objects.equals(listenerConfigs, other.listenerConfigs) && Objects.equals(loadBalancer, other.loadBalancer)
                && Objects.equals(loadBalancerClassName, other.loadBalancerClassName)
                && Objects.equals(managedContext, other.managedContext) && Objects.equals(metricsConfig, other.metricsConfig)
                && Objects.equals(nativeMemoryConfig, other.nativeMemoryConfig)
                && Objects.equals(nearCacheConfigMap, other.nearCacheConfigMap)
                && Objects.equals(networkConfig, other.networkConfig) && Objects.equals(properties, other.properties)
                && Objects.equals(proxyFactoryConfigs, other.proxyFactoryConfigs)
                && Objects.equals(queryCacheConfigs, other.queryCacheConfigs)
                && Objects.equals(reliableTopicConfigMap, other.reliableTopicConfigMap)
                && Objects.equals(securityConfig, other.securityConfig)
                && Objects.equals(serializationConfig, other.serializationConfig)
                && Objects.equals(userCodeDeploymentConfig, other.userCodeDeploymentConfig)
                && Objects.equals(userContext, other.userContext)
                && Objects.equals(instanceTrackingConfig, other.instanceTrackingConfig);
    }

    @Override
    public String toString() {
        return "ClientConfig{"
                + "properties=" + properties
                + ", clusterName=" + clusterName
                + ", securityConfig=" + securityConfig
                + ", networkConfig=" + networkConfig
                + ", loadBalancer=" + loadBalancer
                + ", loadBalancerClassName=" + loadBalancerClassName
                + ", listenerConfigs=" + listenerConfigs
                + ", instanceName='" + instanceName + '\''
                + ", configPatternMatcher=" + configPatternMatcher
                + ", nearCacheConfigMap=" + nearCacheConfigMap
                + ", reliableTopicConfigMap=" + reliableTopicConfigMap
                + ", queryCacheConfigs=" + queryCacheConfigs
                + ", serializationConfig=" + serializationConfig
                + ", nativeMemoryConfig=" + nativeMemoryConfig
                + ", proxyFactoryConfigs=" + proxyFactoryConfigs
                + ", connectionStrategyConfig=" + connectionStrategyConfig
                + ", userCodeDeploymentConfig=" + userCodeDeploymentConfig
                + ", backupAckToClientEnabled=" + backupAckToClientEnabled
                + ", flakeIdGeneratorConfigMap=" + flakeIdGeneratorConfigMap
                + ", labels=" + labels
                + ", metricsConfig=" + metricsConfig
                + ", instanceTrackingConfig=" + instanceTrackingConfig
                + '}';
    }
}
