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

package com.hazelcast.config;

import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.config.AtomicLongConfigReadOnly;
import com.hazelcast.internal.config.CacheSimpleConfigReadOnly;
import com.hazelcast.internal.config.CardinalityEstimatorConfigReadOnly;
import com.hazelcast.internal.config.ConfigUtils;
import com.hazelcast.internal.config.DurableExecutorConfigReadOnly;
import com.hazelcast.internal.config.ExecutorConfigReadOnly;
import com.hazelcast.internal.config.ListConfigReadOnly;
import com.hazelcast.internal.config.LockConfigReadOnly;
import com.hazelcast.internal.config.MapConfigReadOnly;
import com.hazelcast.internal.config.MultiMapConfigReadOnly;
import com.hazelcast.internal.config.PNCounterConfigReadOnly;
import com.hazelcast.internal.config.QueueConfigReadOnly;
import com.hazelcast.internal.config.ReliableTopicConfigReadOnly;
import com.hazelcast.internal.config.ReplicatedMapConfigReadOnly;
import com.hazelcast.internal.config.RingbufferConfigReadOnly;
import com.hazelcast.internal.config.ScheduledExecutorConfigReadOnly;
import com.hazelcast.internal.config.SetConfigReadOnly;
import com.hazelcast.internal.config.TopicConfigReadOnly;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.security.jsm.HazelcastRuntimePermission;
import com.hazelcast.topic.ITopic;

import java.io.File;
import java.net.URL;
import java.util.EventListener;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static com.hazelcast.config.NearCacheConfigAccessor.initDefaultMaxSizeForOnHeapMaps;
import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;

/**
 * Contains all the configuration to start a
 * {@link com.hazelcast.core.HazelcastInstance}. A Config can be created
 * programmatically, but can also be configured using XML, see
 * {@link com.hazelcast.config.XmlConfigBuilder}.
 * <p>
 * Config instances can be shared between threads, but should not be
 * modified after they are used to create HazelcastInstances.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class Config {

    private URL configurationUrl;

    private File configurationFile;

    private ClassLoader classLoader;

    private Properties properties = new Properties();

    private String instanceName;

    private GroupConfig groupConfig = new GroupConfig();

    private NetworkConfig networkConfig = new NetworkConfig();

    private ConfigPatternMatcher configPatternMatcher = new MatchingPointConfigPatternMatcher();

    private final Map<String, MapConfig> mapConfigs = new ConcurrentHashMap<String, MapConfig>();

    private final Map<String, CacheSimpleConfig> cacheConfigs = new ConcurrentHashMap<String, CacheSimpleConfig>();

    private final Map<String, TopicConfig> topicConfigs = new ConcurrentHashMap<String, TopicConfig>();

    private final Map<String, ReliableTopicConfig> reliableTopicConfigs = new ConcurrentHashMap<String, ReliableTopicConfig>();

    private final Map<String, QueueConfig> queueConfigs = new ConcurrentHashMap<String, QueueConfig>();

    private final Map<String, LockConfig> lockConfigs = new ConcurrentHashMap<String, LockConfig>();

    private final Map<String, MultiMapConfig> multiMapConfigs = new ConcurrentHashMap<String, MultiMapConfig>();

    private final Map<String, ListConfig> listConfigs = new ConcurrentHashMap<String, ListConfig>();

    private final Map<String, SetConfig> setConfigs = new ConcurrentHashMap<String, SetConfig>();

    private final Map<String, ExecutorConfig> executorConfigs = new ConcurrentHashMap<String, ExecutorConfig>();

    private final Map<String, DurableExecutorConfig> durableExecutorConfigs
            = new ConcurrentHashMap<String, DurableExecutorConfig>();

    private final Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs
            = new ConcurrentHashMap<String, ScheduledExecutorConfig>();

    private final Map<String, ReplicatedMapConfig> replicatedMapConfigs = new ConcurrentHashMap<String, ReplicatedMapConfig>();

    private final Map<String, WanReplicationConfig> wanReplicationConfigs = new ConcurrentHashMap<String, WanReplicationConfig>();

    private final Map<String, SplitBrainProtectionConfig> splitBrainProtectionConfigs = new ConcurrentHashMap<>();

    private final Map<String, RingbufferConfig> ringbufferConfigs = new ConcurrentHashMap<String, RingbufferConfig>();

    private final Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs =
            new ConcurrentHashMap<String, CardinalityEstimatorConfig>();

    private final Map<String, FlakeIdGeneratorConfig> flakeIdGeneratorConfigMap =
            new ConcurrentHashMap<String, FlakeIdGeneratorConfig>();

    private final Map<String, AtomicLongConfig> atomicLongConfigs = new ConcurrentHashMap<String, AtomicLongConfig>();

    private final Map<String, PNCounterConfig> pnCounterConfigs = new ConcurrentHashMap<String, PNCounterConfig>();

    // @since 3.12
    private AdvancedNetworkConfig advancedNetworkConfig = new AdvancedNetworkConfig();

    private ServicesConfig servicesConfig = new ServicesConfig();

    private SecurityConfig securityConfig = new SecurityConfig();

    private final List<ListenerConfig> listenerConfigs = new LinkedList<ListenerConfig>();

    private PartitionGroupConfig partitionGroupConfig = new PartitionGroupConfig();

    private ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig();

    private SerializationConfig serializationConfig = new SerializationConfig();

    private ManagedContext managedContext;

    private ConcurrentMap<String, Object> userContext = new ConcurrentHashMap<String, Object>();

    private MemberAttributeConfig memberAttributeConfig = new MemberAttributeConfig();

    private NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig();

    private HotRestartPersistenceConfig hotRestartPersistenceConfig = new HotRestartPersistenceConfig();

    private UserCodeDeploymentConfig userCodeDeploymentConfig = new UserCodeDeploymentConfig();

    private CRDTReplicationConfig crdtReplicationConfig = new CRDTReplicationConfig();

    private String licenseKey;

    private boolean liteMember;

    private CPSubsystemConfig cpSubsystemConfig = new CPSubsystemConfig();

    public Config() {
    }

    public Config(String instanceName) {
        this.instanceName = instanceName;
    }

    /**
     * Returns the class-loader that will be used in serialization.
     * <p>
     * If {@code null}, then thread context class-loader will be used instead.
     *
     * @return the class-loader
     */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Sets the class-loader to be used during de-serialization
     * and as context class-loader of Hazelcast internal threads.
     * <p>
     * If not set (or set to {@code null}); thread context class-loader
     * will be used in required places.
     * <p>
     * Default value is {@code null}.
     *
     * @param classLoader class-loader to be used during de-serialization
     * @return Config instance
     */
    public Config setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
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
     * Sets the pattern matcher which is used to match item names to
     * configuration objects.
     * By default the {@link MatchingPointConfigPatternMatcher} is used.
     *
     * @param configPatternMatcher the pattern matcher
     * @throws IllegalArgumentException if the pattern matcher is {@code null}
     * @return this configuration
     */
    public Config setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        if (configPatternMatcher == null) {
            throw new IllegalArgumentException("ConfigPatternMatcher is not allowed to be null!");
        }
        this.configPatternMatcher = configPatternMatcher;
        return this;
    }

    /**
     * Returns the value for a named property. If it has not been previously
     * set, it will try to get the value from the system properties.
     *
     * @param name property name
     * @return property value
     * @see #setProperty(String, String)
     * @see <a href="http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#system-properties">
     * Hazelcast System Properties</a>
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
     * @return this config instance
     * @see <a href="http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#system-properties">
     * Hazelcast System Properties</a>
     */
    public Config setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    /**
     * Returns the member attribute configuration. Unlike the config
     * properties (see {@link #setProperties(Properties)}), member
     * attributes are exchanged with other members, e.g. on membership events.
     *
     * @return the member attribute configuration
     */
    public MemberAttributeConfig getMemberAttributeConfig() {
        return memberAttributeConfig;
    }

    /**
     * Sets the member attribute configuration. Unlike the config
     * properties (see {@link #setProperties(Properties)}), member
     * attributes are exchanged with other members, e.g. on membership events.
     *
     * @param memberAttributeConfig the member attribute configuration
     * @return this configuration
     */
    public Config setMemberAttributeConfig(MemberAttributeConfig memberAttributeConfig) {
        this.memberAttributeConfig = memberAttributeConfig;
        return this;
    }

    /**
     * Returns the properties set on this config instance. These properties
     * are specific to this config and this hazelcast instance.
     *
     * @return the config properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the properties for this config instance. These properties are
     * specific to this config and this hazelcast instance.
     *
     * @param properties the config properties
     * @return this config instance
     */
    public Config setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Returns the instance name uniquely identifying the hazelcast instance
     * created by this configuration. This name is used in different scenarios,
     * such as identifying the hazelcast instance when running multiple
     * instances in the same JVM.
     *
     * @return the hazelcast instance name
     */
    public String getInstanceName() {
        return instanceName;
    }

    /**
     * Sets the instance name uniquely identifying the hazelcast instance
     * created by this configuration. This name is used in different scenarios,
     * such as identifying the hazelcast instance when running multiple
     * instances in the same JVM.
     *
     * @param instanceName the hazelcast instance name
     * @return this config instance
     */
    public Config setInstanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    /**
     * Returns the configuration for hazelcast groups. Members of a cluster
     * must share the same group configuration. Other instances that are
     * reachable but don't have the same group configuration will form
     * independent clusters.
     *
     * @return the hazelcast group configuration
     */
    public GroupConfig getGroupConfig() {
        return groupConfig;
    }

    /**
     * Sets the configuration for hazelcast groups. Members of a cluster must
     * share the same group configuration. Other instances that are reachable
     * but don't have the same group configuration will form independent
     * clusters.
     *
     * @param groupConfig the hazelcast group configuration
     * @return this config instance
     */
    public Config setGroupConfig(GroupConfig groupConfig) {
        this.groupConfig = groupConfig;
        return this;
    }

    // TODO (TK) : Inspect usages of NetworkConfig to replace where needed with {@link Config#getActiveMemberNetworkConfig()}

    /**
     * Returns the network configuration for this hazelcast instance. The
     * network configuration defines how a member will interact with other
     * members or clients.
     *
     * @return the network configuration
     */
    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    /**
     * Sets the network configuration for this hazelcast instance. The
     * network configuration defines how a member will interact with other
     * members or clients.
     *
     * @param networkConfig the network configuration
     * @return this config instance
     */
    public Config setNetworkConfig(NetworkConfig networkConfig) {
        this.networkConfig = networkConfig;
        return this;
    }

    /**
     * Returns a read-only {@link IMap} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     * For non-default configurations and on-heap maps, it will also
     * initialise the Near Cache eviction if not previously set.
     *
     * @param name name of the map config
     * @return the map configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public MapConfig findMapConfig(String name) {
        name = getBaseName(name);
        MapConfig config = lookupByPattern(configPatternMatcher, mapConfigs, name);
        if (config != null) {
            initDefaultMaxSizeForOnHeapMaps(config.getNearCacheConfig());
            return new MapConfigReadOnly(config);
        }
        return new MapConfigReadOnly(getMapConfig("default"));
    }

    /**
     * Returns the map config with the given name or {@code null} if there is none.
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     *
     * @param name name of the map config
     * @return the map configuration or {@code null} if none was found
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public MapConfig getMapConfigOrNull(String name) {
        name = getBaseName(name);
        return lookupByPattern(configPatternMatcher, mapConfigs, name);
    }

    /**
     * Returns the MapConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addMapConfig(MapConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the map config
     * @return the map configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public MapConfig getMapConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, mapConfigs, name, MapConfig.class);
    }

    /**
     * Adds the map configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param mapConfig the map configuration
     * @return this config instance
     */
    public Config addMapConfig(MapConfig mapConfig) {
        mapConfigs.put(mapConfig.getName(), mapConfig);
        return this;
    }

    /**
     * Returns the map of {@link IMap} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the map configurations mapped by config name
     */
    public Map<String, MapConfig> getMapConfigs() {
        return mapConfigs;
    }

    /**
     * Sets the map of {@link IMap} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param mapConfigs the IMap configuration map to set
     * @return this config instance
     */
    public Config setMapConfigs(Map<String, MapConfig> mapConfigs) {
        this.mapConfigs.clear();
        this.mapConfigs.putAll(mapConfigs);
        for (final Entry<String, MapConfig> entry : this.mapConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only {@link CacheSimpleConfig} configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the cardinality estimator config
     * @return the cache configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public CacheSimpleConfig findCacheConfig(String name) {
        name = getBaseName(name);
        final CacheSimpleConfig config = lookupByPattern(configPatternMatcher, cacheConfigs, name);
        if (config != null) {
            return new CacheSimpleConfigReadOnly(config);
        }
        return new CacheSimpleConfigReadOnly(getCacheConfig("default"));
    }

    /**
     * Returns the cache config with the given name or {@code null} if there is none.
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     *
     * @param name name of the cache config
     * @return the cache configuration or {@code null} if none was found
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public CacheSimpleConfig findCacheConfigOrNull(String name) {
        name = getBaseName(name);
        return lookupByPattern(configPatternMatcher, cacheConfigs, name);
    }

    /**
     * Returns the CacheSimpleConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addCacheConfig(CacheSimpleConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the cache config
     * @return the cache configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public CacheSimpleConfig getCacheConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, cacheConfigs, name, CacheSimpleConfig.class);
    }

    /**
     * Adds the cache configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param cacheConfig the cache configuration
     * @return this config instance
     */
    public Config addCacheConfig(CacheSimpleConfig cacheConfig) {
        cacheConfigs.put(cacheConfig.getName(), cacheConfig);
        return this;
    }

    /**
     * Returns the map of cache configurations, mapped by config name. The
     * config name may be a pattern with which the configuration was initially
     * obtained.
     *
     * @return the cache configurations mapped by config name
     */
    public Map<String, CacheSimpleConfig> getCacheConfigs() {
        return cacheConfigs;
    }

    /**
     * Sets the map of cache configurations, mapped by config name. The config
     * name may be a pattern with which the configuration was initially
     * obtained.
     *
     * @param cacheConfigs the cacheConfigs to set
     * @return this config instance
     */
    public Config setCacheConfigs(Map<String, CacheSimpleConfig> cacheConfigs) {
        this.cacheConfigs.clear();
        this.cacheConfigs.putAll(cacheConfigs);
        for (final Entry<String, CacheSimpleConfig> entry : this.cacheConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only {@link IQueue} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the queue config
     * @return the queue configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public QueueConfig findQueueConfig(String name) {
        name = getBaseName(name);
        QueueConfig config = lookupByPattern(configPatternMatcher, queueConfigs, name);
        if (config != null) {
            return new QueueConfigReadOnly(config);
        }
        return new QueueConfigReadOnly(getQueueConfig("default"));
    }

    /**
     * Returns the QueueConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addQueueConfig(QueueConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the queue config
     * @return the queue configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public QueueConfig getQueueConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, queueConfigs, name, QueueConfig.class);
    }

    /**
     * Adds the queue configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param queueConfig the queue configuration
     * @return this config instance
     */
    public Config addQueueConfig(QueueConfig queueConfig) {
        queueConfigs.put(queueConfig.getName(), queueConfig);
        return this;
    }

    /**
     * Returns the map of {@link IQueue} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the queue configurations mapped by config name
     */
    public Map<String, QueueConfig> getQueueConfigs() {
        return queueConfigs;
    }

    /**
     * Sets the map of {@link IQueue} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param queueConfigs the queue configuration map to set
     * @return this config instance
     */
    public Config setQueueConfigs(Map<String, QueueConfig> queueConfigs) {
        this.queueConfigs.clear();
        this.queueConfigs.putAll(queueConfigs);
        for (Entry<String, QueueConfig> entry : queueConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only {@link ILock} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the lock config
     * @return the lock configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public LockConfig findLockConfig(String name) {
        name = getBaseName(name);
        final LockConfig config = lookupByPattern(configPatternMatcher, lockConfigs, name);
        if (config != null) {
            return new LockConfigReadOnly(config);
        }
        return new LockConfigReadOnly(getLockConfig("default"));
    }

    /**
     * Returns the LockConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addLockConfig(LockConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the lock config
     * @return the lock configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public LockConfig getLockConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, lockConfigs, name, LockConfig.class);
    }

    /**
     * Adds the lock configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param lockConfig the lock configuration
     * @return this config instance
     */
    public Config addLockConfig(LockConfig lockConfig) {
        lockConfigs.put(lockConfig.getName(), lockConfig);
        return this;
    }

    /**
     * Returns the map of {@link ILock} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the lock configurations mapped by config name
     */
    public Map<String, LockConfig> getLockConfigs() {
        return lockConfigs;
    }

    /**
     * Sets the map of {@link ILock} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param lockConfigs the ILock configuration map to set
     * @return this config instance
     */
    public Config setLockConfigs(Map<String, LockConfig> lockConfigs) {
        this.lockConfigs.clear();
        this.lockConfigs.putAll(lockConfigs);
        for (Entry<String, LockConfig> entry : lockConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only {@link IList} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the list config
     * @return the list configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public ListConfig findListConfig(String name) {
        name = getBaseName(name);
        ListConfig config = lookupByPattern(configPatternMatcher, listConfigs, name);
        if (config != null) {
            return new ListConfigReadOnly(config);
        }
        return new ListConfigReadOnly(getListConfig("default"));
    }

    /**
     * Returns the ListConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addListConfig(ListConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the list config
     * @return the list configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ListConfig getListConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, listConfigs, name, ListConfig.class);
    }

    /**
     * Adds the list configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param listConfig the list configuration
     * @return this config instance
     */
    public Config addListConfig(ListConfig listConfig) {
        listConfigs.put(listConfig.getName(), listConfig);
        return this;
    }

    /**
     * Returns the map of {@link IList} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the list configurations mapped by config name
     */
    public Map<String, ListConfig> getListConfigs() {
        return listConfigs;
    }

    /**
     * Sets the map of {@link IList} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param listConfigs the list configuration map to set
     * @return this config instance
     */
    public Config setListConfigs(Map<String, ListConfig> listConfigs) {
        this.listConfigs.clear();
        this.listConfigs.putAll(listConfigs);
        for (Entry<String, ListConfig> entry : listConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only {@link ISet} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the set config
     * @return the set configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public SetConfig findSetConfig(String name) {
        name = getBaseName(name);
        SetConfig config = lookupByPattern(configPatternMatcher, setConfigs, name);
        if (config != null) {
            return new SetConfigReadOnly(config);
        }
        return new SetConfigReadOnly(getSetConfig("default"));
    }

    /**
     * Returns the SetConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addSetConfig(SetConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the set config
     * @return the set configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public SetConfig getSetConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, setConfigs, name, SetConfig.class);
    }

    /**
     * Adds the set configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param setConfig the set configuration
     * @return this config instance
     */
    public Config addSetConfig(SetConfig setConfig) {
        setConfigs.put(setConfig.getName(), setConfig);
        return this;
    }

    /**
     * Returns the map of {@link ISet} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the set configurations mapped by config name
     */
    public Map<String, SetConfig> getSetConfigs() {
        return setConfigs;
    }

    /**
     * Sets the map of {@link ISet} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param setConfigs the set configuration map to set
     * @return this config instance
     */
    public Config setSetConfigs(Map<String, SetConfig> setConfigs) {
        this.setConfigs.clear();
        this.setConfigs.putAll(setConfigs);
        for (Entry<String, SetConfig> entry : setConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only {@link MultiMap} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the multimap config
     * @return the multimap configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public MultiMapConfig findMultiMapConfig(String name) {
        name = getBaseName(name);
        MultiMapConfig config = lookupByPattern(configPatternMatcher, multiMapConfigs, name);
        if (config != null) {
            return new MultiMapConfigReadOnly(config);
        }
        return new MultiMapConfigReadOnly(getMultiMapConfig("default"));
    }

    /**
     * Returns the MultiMapConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addMultiMapConfig(MultiMapConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the multimap config
     * @return the multimap configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public MultiMapConfig getMultiMapConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, multiMapConfigs, name, MultiMapConfig.class);
    }

    /**
     * Adds the multimap configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param multiMapConfig the multimap configuration
     * @return this config instance
     */
    public Config addMultiMapConfig(MultiMapConfig multiMapConfig) {
        multiMapConfigs.put(multiMapConfig.getName(), multiMapConfig);
        return this;
    }

    /**
     * Returns the map of {@link MultiMap} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the multimap configurations mapped by config name
     */
    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        return multiMapConfigs;
    }

    /**
     * Sets the map of {@link MultiMap} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param multiMapConfigs the multimap configuration map to set
     * @return this config instance
     */
    public Config setMultiMapConfigs(Map<String, MultiMapConfig> multiMapConfigs) {
        this.multiMapConfigs.clear();
        this.multiMapConfigs.putAll(multiMapConfigs);
        for (final Entry<String, MultiMapConfig> entry : this.multiMapConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only {@link ReplicatedMap} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the replicated map config
     * @return the replicated map configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        name = getBaseName(name);
        ReplicatedMapConfig config = lookupByPattern(configPatternMatcher, replicatedMapConfigs, name);
        if (config != null) {
            return new ReplicatedMapConfigReadOnly(config);
        }
        return new ReplicatedMapConfigReadOnly(getReplicatedMapConfig("default"));
    }

    /**
     * Returns the ReplicatedMapConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking
     * {@link #addReplicatedMapConfig(ReplicatedMapConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the replicated map config
     * @return the replicated map configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, replicatedMapConfigs, name, ReplicatedMapConfig.class);
    }

    /**
     * Adds the replicated map configuration. The configuration is saved under
     * the config name, which may be a pattern with which the configuration
     * will be obtained in the future.
     *
     * @param replicatedMapConfig the replicated map configuration
     * @return this config instance
     */
    public Config addReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        replicatedMapConfigs.put(replicatedMapConfig.getName(), replicatedMapConfig);
        return this;
    }

    /**
     * Returns the map of {@link ReplicatedMap}
     * configurations, mapped by config name. The config name may be a pattern
     * with which the configuration was initially obtained.
     *
     * @return the replicate map configurations mapped by config name
     */
    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return replicatedMapConfigs;
    }

    /**
     * Sets the map of {@link ReplicatedMap} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param replicatedMapConfigs the replicated map configuration map to set
     * @return this config instance
     */
    public Config setReplicatedMapConfigs(Map<String, ReplicatedMapConfig> replicatedMapConfigs) {
        this.replicatedMapConfigs.clear();
        this.replicatedMapConfigs.putAll(replicatedMapConfigs);
        for (final Entry<String, ReplicatedMapConfig> entry : this.replicatedMapConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only {@link com.hazelcast.ringbuffer.Ringbuffer}
     * configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the ringbuffer config
     * @return the ringbuffer configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public RingbufferConfig findRingbufferConfig(String name) {
        name = getBaseName(name);
        RingbufferConfig config = lookupByPattern(configPatternMatcher, ringbufferConfigs, name);
        if (config != null) {
            return new RingbufferConfigReadOnly(config);
        }
        return new RingbufferConfigReadOnly(getRingbufferConfig("default"));
    }

    /**
     * Returns the RingbufferConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking
     * {@link #addRingBufferConfig(RingbufferConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the ringbuffer config
     * @return the ringbuffer configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public RingbufferConfig getRingbufferConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, ringbufferConfigs, name, RingbufferConfig.class);
    }

    /**
     * Adds the ringbuffer configuration. The configuration is saved under
     * the config name, which may be a pattern with which the configuration
     * will be obtained in the future.
     *
     * @param ringbufferConfig the ringbuffer configuration
     * @return this config instance
     */
    public Config addRingBufferConfig(RingbufferConfig ringbufferConfig) {
        ringbufferConfigs.put(ringbufferConfig.getName(), ringbufferConfig);
        return this;
    }

    /**
     * Returns the map of {@link com.hazelcast.ringbuffer.Ringbuffer}
     * configurations, mapped by config name. The config name may be a pattern
     * with which the configuration was initially obtained.
     *
     * @return the ringbuffer configurations mapped by config name
     */
    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        return ringbufferConfigs;
    }

    /**
     * Sets the map of {@link com.hazelcast.ringbuffer.Ringbuffer} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param ringbufferConfigs the ringbuffer configuration map to set
     * @return this config instance
     */
    public Config setRingbufferConfigs(Map<String, RingbufferConfig> ringbufferConfigs) {
        this.ringbufferConfigs.clear();
        this.ringbufferConfigs.putAll(ringbufferConfigs);
        for (Entry<String, RingbufferConfig> entry : ringbufferConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only AtomicLong configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the AtomicLong config
     * @return the AtomicLong configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public AtomicLongConfig findAtomicLongConfig(String name) {
        name = getBaseName(name);
        AtomicLongConfig config = lookupByPattern(configPatternMatcher, atomicLongConfigs, name);
        if (config != null) {
            return new AtomicLongConfigReadOnly(config);
        }
        return new AtomicLongConfigReadOnly(getAtomicLongConfig("default"));
    }

    /**
     * Returns the AtomicLongConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addAtomicLongConfig(AtomicLongConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the AtomicLong config
     * @return the AtomicLong configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public AtomicLongConfig getAtomicLongConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, atomicLongConfigs, name, AtomicLongConfig.class);
    }

    /**
     * Adds the AtomicLong configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param atomicLongConfig the AtomicLong configuration
     * @return this config instance
     */
    public Config addAtomicLongConfig(AtomicLongConfig atomicLongConfig) {
        atomicLongConfigs.put(atomicLongConfig.getName(), atomicLongConfig);
        return this;
    }

    /**
     * Returns the map of AtomicLong configurations, mapped by config name.
     * The config name may be a pattern with which the configuration was initially obtained.
     *
     * @return the AtomicLong configurations mapped by config name
     */
    public Map<String, AtomicLongConfig> getAtomicLongConfigs() {
        return atomicLongConfigs;
    }

    /**
     * Sets the map of AtomicLong configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be obtained in the future.
     *
     * @param atomicLongConfigs the AtomicLong configuration map to set
     * @return this config instance
     */
    public Config setAtomicLongConfigs(Map<String, AtomicLongConfig> atomicLongConfigs) {
        this.atomicLongConfigs.clear();
        this.atomicLongConfigs.putAll(atomicLongConfigs);
        for (Entry<String, AtomicLongConfig> entry : atomicLongConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only {@link ITopic}
     * configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the topic config
     * @return the topic configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public TopicConfig findTopicConfig(String name) {
        name = getBaseName(name);
        TopicConfig config = lookupByPattern(configPatternMatcher, topicConfigs, name);
        if (config != null) {
            return new TopicConfigReadOnly(config);
        }
        return new TopicConfigReadOnly(getTopicConfig("default"));
    }

    /**
     * Returns the TopicConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addTopicConfig(TopicConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the topic config
     * @return the topic configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public TopicConfig getTopicConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, topicConfigs, name, TopicConfig.class);
    }

    /**
     * Adds the topic configuration. The configuration is saved under
     * the config name, which may be a pattern with which the configuration
     * will be obtained in the future.
     *
     * @param topicConfig the topic configuration
     * @return this config instance
     */
    public Config addTopicConfig(TopicConfig topicConfig) {
        topicConfigs.put(topicConfig.getName(), topicConfig);
        return this;
    }

    /**
     * Returns a read-only reliable topic configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the reliable topic config
     * @return the reliable topic configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        name = getBaseName(name);
        ReliableTopicConfig config = lookupByPattern(configPatternMatcher, reliableTopicConfigs, name);
        if (config != null) {
            return new ReliableTopicConfigReadOnly(config);
        }
        return new ReliableTopicConfigReadOnly(getReliableTopicConfig("default"));
    }

    /**
     * Returns the ReliableTopicConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking
     * {@link #addReliableTopicConfig(ReliableTopicConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the reliable topic config
     * @return the reliable topic configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ReliableTopicConfig getReliableTopicConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, reliableTopicConfigs, name, ReliableTopicConfig.class);
    }

    /**
     * Returns the map of reliable topic configurations, mapped by config name.
     * The config name may be a pattern with which the configuration was
     * initially obtained.
     *
     * @return the reliable topic configurations mapped by config name
     */
    public Map<String, ReliableTopicConfig> getReliableTopicConfigs() {
        return reliableTopicConfigs;
    }

    /**
     * Adds the reliable topic configuration. The configuration is saved under
     * the config name, which may be a pattern with which the configuration
     * will be obtained in the future.
     *
     * @param topicConfig the reliable topic configuration
     * @return this config instance
     */
    public Config addReliableTopicConfig(ReliableTopicConfig topicConfig) {
        reliableTopicConfigs.put(topicConfig.getName(), topicConfig);
        return this;
    }

    /**
     * Sets the map of reliable topic configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param reliableTopicConfigs the reliable topic configuration map to set
     * @return this config instance
     */
    public Config setReliableTopicConfigs(Map<String, ReliableTopicConfig> reliableTopicConfigs) {
        this.reliableTopicConfigs.clear();
        this.reliableTopicConfigs.putAll(reliableTopicConfigs);
        for (Entry<String, ReliableTopicConfig> entry : reliableTopicConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the map of reliable topic configurations, mapped by config name.
     * The config name may be a pattern with which the configuration was
     * initially obtained.
     *
     * @return the reliable topic configurations mapped by config name
     */
    public Map<String, TopicConfig> getTopicConfigs() {
        return topicConfigs;
    }

    /**
     * Sets the map of {@link ITopic} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param topicConfigs the topic configuration map to set
     * @return this config instance
     */
    public Config setTopicConfigs(Map<String, TopicConfig> topicConfigs) {
        this.topicConfigs.clear();
        this.topicConfigs.putAll(topicConfigs);
        for (final Entry<String, TopicConfig> entry : this.topicConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only executor configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the executor config
     * @return the executor configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public ExecutorConfig findExecutorConfig(String name) {
        name = getBaseName(name);
        ExecutorConfig config = lookupByPattern(configPatternMatcher, executorConfigs, name);
        if (config != null) {
            return new ExecutorConfigReadOnly(config);
        }
        return new ExecutorConfigReadOnly(getExecutorConfig("default"));
    }

    /**
     * Returns a read-only durable executor configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the durable executor config
     * @return the durable executor configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        name = getBaseName(name);
        DurableExecutorConfig config = lookupByPattern(configPatternMatcher, durableExecutorConfigs, name);
        if (config != null) {
            return new DurableExecutorConfigReadOnly(config);
        }
        return new DurableExecutorConfigReadOnly(getDurableExecutorConfig("default"));
    }

    /**
     * Returns a read-only scheduled executor configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the scheduled executor config
     * @return the scheduled executor configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        name = getBaseName(name);
        ScheduledExecutorConfig config = lookupByPattern(configPatternMatcher, scheduledExecutorConfigs, name);
        if (config != null) {
            return new ScheduledExecutorConfigReadOnly(config);
        }
        return new ScheduledExecutorConfigReadOnly(getScheduledExecutorConfig("default"));
    }

    /**
     * Returns a read-only {@link com.hazelcast.cardinality.CardinalityEstimator}
     * configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the cardinality estimator config
     * @return the cardinality estimator configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        name = getBaseName(name);
        CardinalityEstimatorConfig config = lookupByPattern(configPatternMatcher, cardinalityEstimatorConfigs, name);
        if (config != null) {
            return new CardinalityEstimatorConfigReadOnly(config);
        }
        return new CardinalityEstimatorConfigReadOnly(getCardinalityEstimatorConfig("default"));
    }

    /**
     * Returns a read-only {@link PNCounterConfig}
     * configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the PN counter config
     * @return the PN counter configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public PNCounterConfig findPNCounterConfig(String name) {
        name = getBaseName(name);
        PNCounterConfig config = lookupByPattern(configPatternMatcher, pnCounterConfigs, name);
        if (config != null) {
            return new PNCounterConfigReadOnly(config);
        }
        return new PNCounterConfigReadOnly(getPNCounterConfig("default"));
    }

    /**
     * Returns the ExecutorConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addExecutorConfig(ExecutorConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the executor config
     * @return the executor configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ExecutorConfig getExecutorConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, executorConfigs, name, ExecutorConfig.class);
    }

    /**
     * Returns the DurableExecutorConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking
     * {@link #addDurableExecutorConfig(DurableExecutorConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the durable executor config
     * @return the durable executor configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, durableExecutorConfigs, name, DurableExecutorConfig.class);
    }

    /**
     * Returns the ScheduledExecutorConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking
     * {@link #addScheduledExecutorConfig(ScheduledExecutorConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the scheduled executor config
     * @return the scheduled executor configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, scheduledExecutorConfigs, name, ScheduledExecutorConfig.class);
    }

    /**
     * Returns the CardinalityEstimatorConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking
     * {@link #addCardinalityEstimatorConfig(CardinalityEstimatorConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the cardinality estimator config
     * @return the cardinality estimator configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, cardinalityEstimatorConfigs, name, CardinalityEstimatorConfig.class);
    }

    /**
     * Returns the {@link PNCounterConfig} for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking
     * {@link #addPNCounterConfig(PNCounterConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the PN counter config
     * @return the PN counter configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public PNCounterConfig getPNCounterConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, pnCounterConfigs, name, PNCounterConfig.class);
    }

    /**
     * Adds the executor configuration. The configuration is saved under
     * the config name, which may be a pattern with which the configuration
     * will be obtained in the future.
     *
     * @param executorConfig executor config to add
     * @return this config instance
     */
    public Config addExecutorConfig(ExecutorConfig executorConfig) {
        this.executorConfigs.put(executorConfig.getName(), executorConfig);
        return this;
    }

    /**
     * Adds the durable executor configuration. The configuration is saved under
     * the config name, which may be a pattern with which the configuration
     * will be obtained in the future.
     *
     * @param durableExecutorConfig durable executor config to add
     * @return this config instance
     */
    public Config addDurableExecutorConfig(DurableExecutorConfig durableExecutorConfig) {
        this.durableExecutorConfigs.put(durableExecutorConfig.getName(), durableExecutorConfig);
        return this;
    }

    /**
     * Adds the scheduled executor configuration. The configuration is saved under
     * the config name, which may be a pattern with which the configuration
     * will be obtained in the future.
     *
     * @param scheduledExecutorConfig scheduled executor config to add
     * @return this config instance
     */
    public Config addScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig) {
        this.scheduledExecutorConfigs.put(scheduledExecutorConfig.getName(), scheduledExecutorConfig);
        return this;
    }

    /**
     * Adds the cardinality estimator configuration. The configuration is
     * saved under the config name, which may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param cardinalityEstimatorConfig cardinality estimator config to add
     * @return this config instance
     */
    public Config addCardinalityEstimatorConfig(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        this.cardinalityEstimatorConfigs.put(cardinalityEstimatorConfig.getName(), cardinalityEstimatorConfig);
        return this;
    }

    /**
     * Adds the PN counter configuration. The configuration is
     * saved under the config name, which may be a pattern with which the
     * configuration will be obtained in the future.
     *
     * @param pnCounterConfig PN counter config to add
     * @return this config instance
     */
    public Config addPNCounterConfig(PNCounterConfig pnCounterConfig) {
        this.pnCounterConfigs.put(pnCounterConfig.getName(), pnCounterConfig);
        return this;
    }

    /**
     * Returns the map of executor configurations, mapped by config name.
     * The config name may be a pattern with which the configuration was
     * initially obtained.
     *
     * @return the executor configurations mapped by config name
     */
    public Map<String, ExecutorConfig> getExecutorConfigs() {
        return executorConfigs;
    }

    /**
     * Sets the map of executor configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param executorConfigs the executor configuration map to set
     * @return this config instance
     */
    public Config setExecutorConfigs(Map<String, ExecutorConfig> executorConfigs) {
        this.executorConfigs.clear();
        this.executorConfigs.putAll(executorConfigs);
        for (Entry<String, ExecutorConfig> entry : executorConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the map of durable executor configurations, mapped by config
     * name. The config name may be a pattern with which the configuration was
     * initially obtained.
     *
     * @return the durable executor configurations mapped by config name
     */
    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        return durableExecutorConfigs;
    }

    /**
     * Sets the map of durable executor configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param durableExecutorConfigs the durable executor configuration map to set
     * @return this config instance
     */
    public Config setDurableExecutorConfigs(Map<String, DurableExecutorConfig> durableExecutorConfigs) {
        this.durableExecutorConfigs.clear();
        this.durableExecutorConfigs.putAll(durableExecutorConfigs);
        for (Entry<String, DurableExecutorConfig> entry : durableExecutorConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the map of scheduled executor configurations, mapped by config
     * name. The config name may be a pattern with which the configuration was
     * initially obtained.
     *
     * @return the scheduled executor configurations mapped by config name
     */
    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        return scheduledExecutorConfigs;
    }

    /**
     * Sets the map of scheduled executor configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param scheduledExecutorConfigs the scheduled executor configuration
     *                                 map to set
     * @return this config instance
     */
    public Config setScheduledExecutorConfigs(Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs) {
        this.scheduledExecutorConfigs.clear();
        this.scheduledExecutorConfigs.putAll(scheduledExecutorConfigs);
        for (Entry<String, ScheduledExecutorConfig> entry : scheduledExecutorConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the map of cardinality estimator configurations, mapped by config
     * name. The config name may be a pattern with which the configuration was
     * initially obtained.
     *
     * @return the cardinality estimator configurations mapped by config name
     */
    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        return cardinalityEstimatorConfigs;
    }

    /**
     * Sets the map of cardinality estimator configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param cardinalityEstimatorConfigs the cardinality estimator
     *                                    configuration map to set
     * @return this config instance
     */
    public Config setCardinalityEstimatorConfigs(Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs) {
        this.cardinalityEstimatorConfigs.clear();
        this.cardinalityEstimatorConfigs.putAll(cardinalityEstimatorConfigs);
        for (Entry<String, CardinalityEstimatorConfig> entry : cardinalityEstimatorConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the map of PN counter configurations, mapped by config
     * name. The config name may be a pattern with which the configuration was
     * initially obtained.
     *
     * @return the PN counter configurations mapped by config name
     */
    public Map<String, PNCounterConfig> getPNCounterConfigs() {
        return pnCounterConfigs;
    }

    /**
     * Sets the map of PN counter configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param pnCounterConfigs the PN counter configuration map to set
     * @return this config instance
     */
    public Config setPNCounterConfigs(Map<String, PNCounterConfig> pnCounterConfigs) {
        this.pnCounterConfigs.clear();
        this.pnCounterConfigs.putAll(pnCounterConfigs);
        for (Entry<String, PNCounterConfig> entry : pnCounterConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the WAN replication configuration with the given {@code name}.
     *
     * @param name the WAN replication config name
     * @return the WAN replication config
     */
    public WanReplicationConfig getWanReplicationConfig(String name) {
        return wanReplicationConfigs.get(name);
    }

    /**
     * Adds the WAN replication config under the name defined by
     * {@link WanReplicationConfig#getName()}.
     *
     * @param wanReplicationConfig the WAN replication config
     * @return this config instance
     */
    public Config addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        wanReplicationConfigs.put(wanReplicationConfig.getName(), wanReplicationConfig);
        return this;
    }

    /**
     * Returns the map of WAN replication configurations, mapped by config
     * name.
     *
     * @return the WAN replication configurations mapped by config name
     */
    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        return wanReplicationConfigs;
    }

    /**
     * Sets the map of WAN replication configurations, mapped by config name.
     *
     * @param wanReplicationConfigs the WAN replication configuration map to set
     * @return this config instance
     */
    public Config setWanReplicationConfigs(Map<String, WanReplicationConfig> wanReplicationConfigs) {
        this.wanReplicationConfigs.clear();
        this.wanReplicationConfigs.putAll(wanReplicationConfigs);
        for (final Entry<String, WanReplicationConfig> entry : this.wanReplicationConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the map of split brain protection configurations, mapped by
     * config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the split-brain protection configurations mapped by config name
     */
    public Map<String, SplitBrainProtectionConfig> getSplitBrainProtectionConfigs() {
        return splitBrainProtectionConfigs;
    }

    /**
     * Returns the {@link SplitBrainProtectionConfig} for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking
     * {@link #addSplitBrainProtectionConfig(SplitBrainProtectionConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the split-brain protection config
     * @return the split-brain protection configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public SplitBrainProtectionConfig getSplitBrainProtectionConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, splitBrainProtectionConfigs, name, SplitBrainProtectionConfig.class);
    }

    /**
     * Returns a read-only split-brain protection configuration for the given
     * name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the split-brain protection config
     * @return the split-brain protection configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public SplitBrainProtectionConfig findSplitBrainProtectionConfig(String name) {
        name = getBaseName(name);
        SplitBrainProtectionConfig config = lookupByPattern(configPatternMatcher, splitBrainProtectionConfigs, name);
        if (config != null) {
            return config;
        }
        return getSplitBrainProtectionConfig("default");
    }

    /**
     * Sets the map of split-brain protection configurations, mapped by config
     * name. The config name may be a pattern with which the configuration
     * will be obtained in the future.
     *
     * @param splitBrainProtectionConfigs the split-brain protection configuration map to set
     * @return this config instance
     */
    public Config setSplitBrainProtectionConfigs(Map<String, SplitBrainProtectionConfig> splitBrainProtectionConfigs) {
        this.splitBrainProtectionConfigs.clear();
        this.splitBrainProtectionConfigs.putAll(splitBrainProtectionConfigs);
        for (final Entry<String, SplitBrainProtectionConfig> entry : this.splitBrainProtectionConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Adds the split-brain protection configuration.
     * The configuration is saved under the config name defined by
     * {@link SplitBrainProtectionConfig#getName()}.
     *
     * @param splitBrainProtectionConfig split-brain protection config to add
     * @return this config instance
     */
    public Config addSplitBrainProtectionConfig(SplitBrainProtectionConfig splitBrainProtectionConfig) {
        splitBrainProtectionConfigs.put(splitBrainProtectionConfig.getName(), splitBrainProtectionConfig);
        return this;
    }

    /**
     * Returns the management center configuration for this hazelcast instance.
     *
     * @return the management center configuration
     */
    public ManagementCenterConfig getManagementCenterConfig() {
        return managementCenterConfig;
    }

    /**
     * Sets the management center configuration for this hazelcast instance.
     *
     * @param managementCenterConfig the management center configuration
     * @return this config instance
     */
    public Config setManagementCenterConfig(ManagementCenterConfig managementCenterConfig) {
        this.managementCenterConfig = managementCenterConfig;
        return this;
    }

    /**
     * Returns the configuration for the user services managed by this
     * hazelcast instance.
     *
     * @return the user services configuration
     */
    public ServicesConfig getServicesConfig() {
        return servicesConfig;
    }

    /**
     * Sets the configuration for the user services managed by this hazelcast
     * instance.
     *
     * @param servicesConfig the user services configuration
     * @return this config instance
     */
    public Config setServicesConfig(ServicesConfig servicesConfig) {
        this.servicesConfig = servicesConfig;
        return this;
    }

    /**
     * Returns the security configuration for this hazelcast instance.
     * This includes configuration for security interceptors, permissions, etc.
     *
     * @return the security configuration
     */
    public SecurityConfig getSecurityConfig() {
        return securityConfig;
    }

    /**
     * Sets the security configuration for this hazelcast instance.
     * This includes configuration for security interceptors, permissions, etc.
     *
     * @param securityConfig the security configuration
     * @return this config instance
     */
    public Config setSecurityConfig(SecurityConfig securityConfig) {
        this.securityConfig = securityConfig;
        return this;
    }

    /**
     * Adds a configuration for an {@link EventListener}. This includes
     * listeners for events related to this instance/member or the cluster,
     * such as partition, migration, cluster version listeners, etc. but not
     * listeners on specific distributed data structures.
     *
     * @param listenerConfig the listener configuration
     * @return this config instance
     */
    public Config addListenerConfig(ListenerConfig listenerConfig) {
        getListenerConfigs().add(listenerConfig);
        return this;
    }

    /**
     * Returns the list of {@link EventListener} configurations. This includes
     * listeners for events related to this instance/member or the cluster,
     * such as partition, migration, cluster version listeners, etc. but not
     * listeners on specific distributed data structures.
     *
     * @return the listener configurations
     */
    public List<ListenerConfig> getListenerConfigs() {
        return listenerConfigs;
    }

    /**
     * Sets the list of {@link EventListener} configurations. This includes
     * listeners for events related to this instance/member or the cluster,
     * such as partition, migration, cluster version listeners, etc. but not
     * listeners on specific distributed data structures.
     *
     * @param listenerConfigs the listener configurations
     * @return this config instance
     */
    public Config setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        this.listenerConfigs.clear();
        this.listenerConfigs.addAll(listenerConfigs);
        return this;
    }

    /**
     * Returns the map of {@link FlakeIdGenerator} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the map configurations mapped by config name
     */
    public Map<String, FlakeIdGeneratorConfig> getFlakeIdGeneratorConfigs() {
        return flakeIdGeneratorConfigMap;
    }

    /**
     * Returns a {@link FlakeIdGeneratorConfig} configuration for the given flake ID generator name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code "default"}.
     *
     * @param name name of the flake ID generator config
     * @return the flake ID generator configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see com.hazelcast.partition.strategy.StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public FlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String name) {
        String baseName = getBaseName(name);
        FlakeIdGeneratorConfig config = lookupByPattern(configPatternMatcher, flakeIdGeneratorConfigMap, baseName);
        if (config != null) {
            return config;
        }
        return getFlakeIdGeneratorConfig("default");
    }

    /**
     * Returns the {@link FlakeIdGeneratorConfig} for the given name, creating
     * one if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking {@link #addFlakeIdGeneratorConfig(FlakeIdGeneratorConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the flake ID generator config
     * @return the cache configuration
     * @throws InvalidConfigurationException if ambiguous configurations are
     *                                       found
     * @see com.hazelcast.partition.strategy.StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public FlakeIdGeneratorConfig getFlakeIdGeneratorConfig(String name) {
        return ConfigUtils.getConfig(configPatternMatcher, flakeIdGeneratorConfigMap, name,
                FlakeIdGeneratorConfig.class, new BiConsumer<FlakeIdGeneratorConfig, String>() {
                    @Override
                    public void accept(FlakeIdGeneratorConfig flakeIdGeneratorConfig, String name) {
                        flakeIdGeneratorConfig.setName(name);
                    }
                });
    }

    /**
     * Adds a flake ID generator configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param config the flake ID generator configuration
     * @return this config instance
     */
    public Config addFlakeIdGeneratorConfig(FlakeIdGeneratorConfig config) {
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
    public Config setFlakeIdGeneratorConfigs(Map<String, FlakeIdGeneratorConfig> map) {
        flakeIdGeneratorConfigMap.clear();
        flakeIdGeneratorConfigMap.putAll(map);
        for (Entry<String, FlakeIdGeneratorConfig> entry : map.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the serialization configuration for this hazelcast instance. The
     * serialization configuration defines how objects are serialized and
     * deserialized on this instance.
     *
     * @return the serialization configuration
     */
    public SerializationConfig getSerializationConfig() {
        return serializationConfig;
    }

    /**
     * Sets the serialization configuration for this hazelcast instance. The
     * serialization configuration defines how objects are serialized and
     * deserialized on this instance.
     *
     * @param serializationConfig the serialization configuration
     * @return this config instance
     */
    public Config setSerializationConfig(SerializationConfig serializationConfig) {
        this.serializationConfig = serializationConfig;
        return this;
    }

    /**
     * Returns the partition group configuration for this hazelcast instance.
     * The partition group configuration defines how partitions are mapped to
     * members.
     *
     * @return the partition group configuration
     */
    public PartitionGroupConfig getPartitionGroupConfig() {
        return partitionGroupConfig;
    }

    /**
     * Sets the partition group configuration for this hazelcast instance.
     * The partition group configuration defines how partitions are mapped to
     * members.
     *
     * @param partitionGroupConfig the partition group configuration
     * @return this config instance
     */
    public Config setPartitionGroupConfig(PartitionGroupConfig partitionGroupConfig) {
        this.partitionGroupConfig = partitionGroupConfig;
        return this;
    }

    /**
     * Returns the Hot Restart configuration for this hazelcast instance
     *
     * @return hot restart configuration
     */
    public HotRestartPersistenceConfig getHotRestartPersistenceConfig() {
        return hotRestartPersistenceConfig;
    }

    /**
     * Sets the Hot Restart configuration.
     *
     * @param hrConfig Hot Restart configuration
     * @return this config instance
     * @throws NullPointerException if the {@code hrConfig} parameter is {@code null}
     */
    public Config setHotRestartPersistenceConfig(HotRestartPersistenceConfig hrConfig) {
        checkNotNull(hrConfig, "Hot restart config cannot be null!");
        this.hotRestartPersistenceConfig = hrConfig;
        return this;
    }

    public CRDTReplicationConfig getCRDTReplicationConfig() {
        return crdtReplicationConfig;
    }

    /**
     * Sets the replication configuration for {@link com.hazelcast.internal.crdt.CRDT}
     * implementations.
     *
     * @param crdtReplicationConfig the replication configuration
     * @return this config instance
     * @throws NullPointerException if the {@code crdtReplicationConfig} parameter is {@code null}
     */
    public Config setCRDTReplicationConfig(CRDTReplicationConfig crdtReplicationConfig) {
        checkNotNull(crdtReplicationConfig, "The CRDT replication config cannot be null!");
        this.crdtReplicationConfig = crdtReplicationConfig;
        return this;
    }

    /**
     * Returns the external managed context. This context is used to
     * initialize user supplied objects.
     *
     * @return the managed context
     */
    public ManagedContext getManagedContext() {
        return managedContext;
    }

    /**
     * Sets the external managed context. This context is used to
     * initialize user supplied objects.
     *
     * @param managedContext the managed context
     * @return this config instance
     */
    public Config setManagedContext(final ManagedContext managedContext) {
        this.managedContext = managedContext;
        return this;
    }

    /**
     * Returns the user supplied context. This context can then be obtained
     * from an instance of {@link com.hazelcast.core.HazelcastInstance}.
     *
     * @return the user supplied context
     * @see HazelcastInstance#getUserContext()
     */
    public ConcurrentMap<String, Object> getUserContext() {
        return userContext;
    }

    /**
     * Sets the user supplied context. This context can then be obtained
     * from an instance of {@link com.hazelcast.core.HazelcastInstance}.
     *
     * @param userContext the user supplied context
     * @return this config instance
     * @see HazelcastInstance#getUserContext()
     */
    public Config setUserContext(ConcurrentMap<String, Object> userContext) {
        if (userContext == null) {
            throw new IllegalArgumentException("userContext can't be null");
        }
        this.userContext = userContext;
        return this;
    }

    /**
     * Returns the native memory configuration for this hazelcast instance.
     * The native memory configuration defines the how native memory
     * is used and the limits on its usage.
     *
     * @return the native memory configuration
     */
    public NativeMemoryConfig getNativeMemoryConfig() {
        return nativeMemoryConfig;
    }

    /**
     * Sets the native memory configuration for this hazelcast instance.
     * The native memory configuration defines the how native memory
     * is used and the limits on its usage.
     *
     * @param nativeMemoryConfig the native memory configuration
     * @return this config instance
     */
    public Config setNativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        this.nativeMemoryConfig = nativeMemoryConfig;
        return this;
    }

    /**
     * Returns the {@link URL} to the declarative configuration, which has been parsed
     * to create this {@link Config} instance.
     *
     * @return the configuration URL if the configuration loaded from a URL
     * or {@code null} otherwise
     */
    public URL getConfigurationUrl() {
        return configurationUrl;
    }

    /**
     * Sets the {@link URL} from which this configuration has been retrieved
     * and created.
     * <p>
     * Is set by the {@link XmlConfigBuilder}, when the XML configuration was
     * retrieved from a URL.
     *
     * @param configurationUrl the configuration URL to set
     * @return this config instance
     */
    public Config setConfigurationUrl(URL configurationUrl) {
        this.configurationUrl = configurationUrl;
        return this;
    }

    /**
     * Returns the {@link File} to the declarative configuration, which has been
     * parsed to create this {@link Config} instance.
     *
     * @return the configuration file if the configuration loaded from a file
     * or {@code null} otherwise
     */
    public File getConfigurationFile() {
        return configurationFile;
    }

    /**
     * Sets the {@link File} from which this configuration has been retrieved
     * and created.
     * <p>
     * Is set by the {@link XmlConfigBuilder}, when the XML configuration was
     * retrieved from a file.
     *
     * @param configurationFile the configuration file to set
     */
    public Config setConfigurationFile(File configurationFile) {
        this.configurationFile = configurationFile;
        return this;
    }

    /**
     * Returns the license key for this hazelcast instance. The license key
     * is used to enable enterprise features.
     *
     * @return the license key
     * @throws SecurityException If a security manager exists and the calling method doesn't have corresponding
     *                           {@link HazelcastRuntimePermission}
     */
    public String getLicenseKey() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new HazelcastRuntimePermission("com.hazelcast.config.Config.getLicenseKey"));
        }
        return licenseKey;
    }

    /**
     * Sets the license key for this hazelcast instance. The license key
     * is used to enable enterprise features.
     *
     * @param licenseKey the license key
     * @return this config instance
     */
    public Config setLicenseKey(final String licenseKey) {
        this.licenseKey = licenseKey;
        return this;
    }

    /**
     * Returns {@code true} if this member is a lite member. A lite member
     * does not own any partitions.
     *
     * @return {@code true} if this member is a lite member
     */
    public boolean isLiteMember() {
        return liteMember;
    }

    /**
     * Sets the flag to indicate if this member is a lite member. A lite member
     * does not own any partitions.
     *
     * @param liteMember if this member is a lite member
     * @return this config instance
     */
    public Config setLiteMember(boolean liteMember) {
        this.liteMember = liteMember;
        return this;
    }

    /**
     * Get current configuration of User Code Deployment.
     *
     * @return User Code Deployment configuration
     * @since 3.8
     */
    public UserCodeDeploymentConfig getUserCodeDeploymentConfig() {
        return userCodeDeploymentConfig;
    }

    /**
     * Set User Code Deployment configuration
     *
     * @param userCodeDeploymentConfig the user code deployment configuration
     * @return this config instance
     * @since 3.8
     */
    public Config setUserCodeDeploymentConfig(UserCodeDeploymentConfig userCodeDeploymentConfig) {
        this.userCodeDeploymentConfig = userCodeDeploymentConfig;
        return this;
    }

    public AdvancedNetworkConfig getAdvancedNetworkConfig() {
        return advancedNetworkConfig;
    }

    public Config setAdvancedNetworkConfig(AdvancedNetworkConfig advancedNetworkConfig) {
        this.advancedNetworkConfig = advancedNetworkConfig;
        return this;
    }

    /**
     * Get current configuration for the CP subsystem
     *
     * @return CP subsystem configuration
     * @since 3.12
     */
    public CPSubsystemConfig getCPSubsystemConfig() {
        return cpSubsystemConfig;
    }

    /**
     * Set CP subsystem configuration
     *
     * @param cpSubsystemConfig the CP subsystem configuration
     * @return this config instance
     * @since 3.12
     */
    public Config setCPSubsystemConfig(CPSubsystemConfig cpSubsystemConfig) {
        this.cpSubsystemConfig = cpSubsystemConfig;
        return this;
    }

    @Override
    public String toString() {
        return "Config{"
                + "groupConfig=" + groupConfig
                + ", properties=" + properties
                + ", networkConfig=" + networkConfig
                + ", mapConfigs=" + mapConfigs
                + ", topicConfigs=" + topicConfigs
                + ", reliableTopicConfigs=" + reliableTopicConfigs
                + ", queueConfigs=" + queueConfigs
                + ", multiMapConfigs=" + multiMapConfigs
                + ", executorConfigs=" + executorConfigs
                + ", ringbufferConfigs=" + ringbufferConfigs
                + ", atomicLongConfigs=" + atomicLongConfigs
                + ", wanReplicationConfigs=" + wanReplicationConfigs
                + ", listenerConfigs=" + listenerConfigs
                + ", partitionGroupConfig=" + partitionGroupConfig
                + ", managementCenterConfig=" + managementCenterConfig
                + ", securityConfig=" + securityConfig
                + ", liteMember=" + liteMember
                + ", crdtReplicationConfig=" + crdtReplicationConfig
                + ", advancedNetworkConfig=" + advancedNetworkConfig
                + ", cpSubsystemConfig=" + cpSubsystemConfig
                + '}';
    }
}
