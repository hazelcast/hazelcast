/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.journal.EventJournal;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.util.StringUtil;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.EventListener;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.config.NearCacheConfigAccessor.initDefaultMaxSizeForOnHeapMaps;
import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;
import static com.hazelcast.util.Preconditions.checkNotNull;

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

    private static final ILogger LOGGER = Logger.getLogger(Config.class);

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

    private final Map<String, SemaphoreConfig> semaphoreConfigs = new ConcurrentHashMap<String, SemaphoreConfig>();

    private final Map<String, CountDownLatchConfig> countDownLatchConfigs =
            new ConcurrentHashMap<String, CountDownLatchConfig>();

    private final Map<String, ReplicatedMapConfig> replicatedMapConfigs = new ConcurrentHashMap<String, ReplicatedMapConfig>();

    private final Map<String, WanReplicationConfig> wanReplicationConfigs = new ConcurrentHashMap<String, WanReplicationConfig>();

    private final Map<String, JobTrackerConfig> jobTrackerConfigs = new ConcurrentHashMap<String, JobTrackerConfig>();

    private final Map<String, QuorumConfig> quorumConfigs = new ConcurrentHashMap<String, QuorumConfig>();

    private final Map<String, RingbufferConfig> ringbufferConfigs = new ConcurrentHashMap<String, RingbufferConfig>();

    private final Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs =
            new ConcurrentHashMap<String, CardinalityEstimatorConfig>();

    private final Map<String, EventJournalConfig> mapEventJournalConfigs = new ConcurrentHashMap<String, EventJournalConfig>();

    private final Map<String, EventJournalConfig> cacheEventJournalConfigs = new ConcurrentHashMap<String, EventJournalConfig>();

    private final Map<String, FlakeIdGeneratorConfig> flakeIdGeneratorConfigMap =
            new ConcurrentHashMap<String, FlakeIdGeneratorConfig>();

    private final Map<String, AtomicLongConfig> atomicLongConfigs = new ConcurrentHashMap<String, AtomicLongConfig>();

    private final Map<String, AtomicReferenceConfig> atomicReferenceConfigs
            = new ConcurrentHashMap<String, AtomicReferenceConfig>();

    private final Map<String, PNCounterConfig> pnCounterConfigs = new ConcurrentHashMap<String, PNCounterConfig>();

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
     */
    public void setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        if (configPatternMatcher == null) {
            throw new IllegalArgumentException("ConfigPatternMatcher is not allowed to be null!");
        }
        this.configPatternMatcher = configPatternMatcher;
    }

    /**
     * Returns the value for a named property. If it has not been previously
     * set, it will try to get the value from the system properties.
     *
     * @param name property name
     * @return property value
     * @see #setProperty(String, String)
     * @see <a href="http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#system-properties">
     *     Hazelcast System Properties</a>
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
     *     Hazelcast System Properties</a>
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
     */
    public void setMemberAttributeConfig(MemberAttributeConfig memberAttributeConfig) {
        this.memberAttributeConfig = memberAttributeConfig;
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
     * Returns a read-only {@link com.hazelcast.core.IMap} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     * For non-default configurations and on-heap maps, it will also
     * initialise the the Near Cache eviction if not previously set.
     *
     * @param name name of the map config
     * @return the map configuration
     * @throws ConfigurationException if ambiguous configurations are found
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
            return config.getAsReadOnly();
        }
        return getMapConfig("default").getAsReadOnly();
    }

    /**
     * Returns the map config with the given name or {@code null} if there is none.
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     *
     * @param name name of the map config
     * @return the map configuration or {@code null} if none was found
     * @throws ConfigurationException if ambiguous configurations are found
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
     * The configuration is found by matching the the configuration name
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public MapConfig getMapConfig(String name) {
        name = getBaseName(name);
        MapConfig config = lookupByPattern(configPatternMatcher, mapConfigs, name);
        if (config != null) {
            return config;
        }
        MapConfig defConfig = mapConfigs.get("default");
        if (defConfig == null) {
            defConfig = new MapConfig();
            defConfig.setName("default");
            initDefaultMaxSizeForOnHeapMaps(defConfig.getNearCacheConfig());
            mapConfigs.put(defConfig.getName(), defConfig);
        }
        config = new MapConfig(defConfig);
        config.setName(name);
        mapConfigs.put(config.getName(), config);
        return config;
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
     * Returns the map of {@link com.hazelcast.core.IMap} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the map configurations mapped by config name
     */
    public Map<String, MapConfig> getMapConfigs() {
        return mapConfigs;
    }

    /**
     * Sets the map of {@link com.hazelcast.core.IMap} configurations,
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public CacheSimpleConfig findCacheConfig(String name) {
        name = getBaseName(name);
        final CacheSimpleConfig config = lookupByPattern(configPatternMatcher, cacheConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getCacheConfig("default").getAsReadOnly();
    }

    /**
     * Returns the cache config with the given name or {@code null} if there is none.
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     *
     * @param name name of the cache config
     * @return the cache configuration or {@code null} if none was found
     * @throws ConfigurationException if ambiguous configurations are found
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
     * The configuration is found by matching the the configuration name
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public CacheSimpleConfig getCacheConfig(String name) {
        name = getBaseName(name);
        CacheSimpleConfig config = lookupByPattern(configPatternMatcher, cacheConfigs, name);
        if (config != null) {
            return config;
        }
        CacheSimpleConfig defConfig = cacheConfigs.get("default");
        if (defConfig == null) {
            defConfig = new CacheSimpleConfig();
            defConfig.setName("default");
            addCacheConfig(defConfig);
        }
        config = new CacheSimpleConfig(defConfig);
        config.setName(name);
        addCacheConfig(config);
        return config;
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
     * Returns a read-only {@link com.hazelcast.core.IQueue} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the queue config
     * @return the queue configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public QueueConfig findQueueConfig(String name) {
        name = getBaseName(name);
        QueueConfig config = lookupByPattern(configPatternMatcher, queueConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getQueueConfig("default").getAsReadOnly();
    }

    /**
     * Returns the QueueConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking {@link #addQueueConfig(QueueConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the queue config
     * @return the queue configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public QueueConfig getQueueConfig(String name) {
        name = getBaseName(name);
        QueueConfig config = lookupByPattern(configPatternMatcher, queueConfigs, name);
        if (config != null) {
            return config;
        }
        QueueConfig defConfig = queueConfigs.get("default");
        if (defConfig == null) {
            defConfig = new QueueConfig();
            defConfig.setName("default");
            addQueueConfig(defConfig);
        }
        config = new QueueConfig(defConfig);
        config.setName(name);
        addQueueConfig(config);
        return config;
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
     * Returns the map of {@link com.hazelcast.core.IQueue} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the queue configurations mapped by config name
     */
    public Map<String, QueueConfig> getQueueConfigs() {
        return queueConfigs;
    }

    /**
     * Sets the map of {@link com.hazelcast.core.IQueue} configurations,
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
     * Returns a read-only {@link com.hazelcast.core.ILock} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the lock config
     * @return the lock configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public LockConfig findLockConfig(String name) {
        name = getBaseName(name);
        final LockConfig config = lookupByPattern(configPatternMatcher, lockConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getLockConfig("default").getAsReadOnly();
    }

    /**
     * Returns the LockConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking {@link #addLockConfig(LockConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the lock config
     * @return the lock configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public LockConfig getLockConfig(String name) {
        name = getBaseName(name);
        LockConfig config = lookupByPattern(configPatternMatcher, lockConfigs, name);
        if (config != null) {
            return config;
        }
        LockConfig defConfig = lockConfigs.get("default");
        if (defConfig == null) {
            defConfig = new LockConfig();
            defConfig.setName("default");
            addLockConfig(defConfig);
        }
        config = new LockConfig(defConfig);
        config.setName(name);
        addLockConfig(config);
        return config;
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
     * Returns the map of {@link com.hazelcast.core.ILock} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the lock configurations mapped by config name
     */
    public Map<String, LockConfig> getLockConfigs() {
        return lockConfigs;
    }

    /**
     * Sets the map of {@link com.hazelcast.core.ILock} configurations,
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
     * Returns a read-only {@link com.hazelcast.core.IList} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the list config
     * @return the list configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public ListConfig findListConfig(String name) {
        name = getBaseName(name);
        ListConfig config = lookupByPattern(configPatternMatcher, listConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getListConfig("default").getAsReadOnly();
    }

    /**
     * Returns the ListConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking {@link #addListConfig(ListConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the list config
     * @return the list configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ListConfig getListConfig(String name) {
        name = getBaseName(name);
        ListConfig config = lookupByPattern(configPatternMatcher, listConfigs, name);
        if (config != null) {
            return config;
        }
        ListConfig defConfig = listConfigs.get("default");
        if (defConfig == null) {
            defConfig = new ListConfig();
            defConfig.setName("default");
            addListConfig(defConfig);
        }
        config = new ListConfig(defConfig);
        config.setName(name);
        addListConfig(config);
        return config;
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
     * Returns the map of {@link com.hazelcast.core.IList} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the list configurations mapped by config name
     */
    public Map<String, ListConfig> getListConfigs() {
        return listConfigs;
    }

    /**
     * Sets the map of {@link com.hazelcast.core.IList} configurations,
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
     * Returns a read-only {@link com.hazelcast.core.ISet} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the set config
     * @return the set configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public SetConfig findSetConfig(String name) {
        name = getBaseName(name);
        SetConfig config = lookupByPattern(configPatternMatcher, setConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getSetConfig("default").getAsReadOnly();
    }

    /**
     * Returns the SetConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking {@link #addSetConfig(SetConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the set config
     * @return the set configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public SetConfig getSetConfig(String name) {
        name = getBaseName(name);
        SetConfig config = lookupByPattern(configPatternMatcher, setConfigs, name);
        if (config != null) {
            return config;
        }
        SetConfig defConfig = setConfigs.get("default");
        if (defConfig == null) {
            defConfig = new SetConfig();
            defConfig.setName("default");
            addSetConfig(defConfig);
        }
        config = new SetConfig(defConfig);
        config.setName(name);
        addSetConfig(config);
        return config;
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
     * Returns the map of {@link com.hazelcast.core.ISet} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the set configurations mapped by config name
     */
    public Map<String, SetConfig> getSetConfigs() {
        return setConfigs;
    }

    /**
     * Sets the map of {@link com.hazelcast.core.ISet} configurations,
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
     * Returns a read-only {@link com.hazelcast.core.MultiMap} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the multimap config
     * @return the multimap configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public MultiMapConfig findMultiMapConfig(String name) {
        name = getBaseName(name);
        MultiMapConfig config = lookupByPattern(configPatternMatcher, multiMapConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getMultiMapConfig("default").getAsReadOnly();
    }

    /**
     * Returns the MultiMapConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking {@link #addMultiMapConfig(MultiMapConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the multimap config
     * @return the multimap configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public MultiMapConfig getMultiMapConfig(String name) {
        name = getBaseName(name);
        MultiMapConfig config = lookupByPattern(configPatternMatcher, multiMapConfigs, name);
        if (config != null) {
            return config;
        }
        MultiMapConfig defConfig = multiMapConfigs.get("default");
        if (defConfig == null) {
            defConfig = new MultiMapConfig();
            defConfig.setName("default");
            addMultiMapConfig(defConfig);
        }
        config = new MultiMapConfig(defConfig);
        config.setName(name);
        addMultiMapConfig(config);
        return config;
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
     * Returns the map of {@link com.hazelcast.core.MultiMap} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the multimap configurations mapped by config name
     */
    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        return multiMapConfigs;
    }

    /**
     * Sets the map of {@link com.hazelcast.core.MultiMap} configurations,
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
     * Returns a read-only {@link com.hazelcast.core.ReplicatedMap} configuration for
     * the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the replicated map config
     * @return the replicated map configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        name = getBaseName(name);
        ReplicatedMapConfig config = lookupByPattern(configPatternMatcher, replicatedMapConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getReplicatedMapConfig("default").getAsReadOnly();
    }

    /**
     * Returns the ReplicatedMapConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        name = getBaseName(name);
        ReplicatedMapConfig config = lookupByPattern(configPatternMatcher, replicatedMapConfigs, name);
        if (config != null) {
            return config;
        }
        ReplicatedMapConfig defConfig = replicatedMapConfigs.get("default");
        if (defConfig == null) {
            defConfig = new ReplicatedMapConfig();
            defConfig.setName("default");
            addReplicatedMapConfig(defConfig);
        }
        config = new ReplicatedMapConfig(defConfig);
        config.setName(name);
        addReplicatedMapConfig(config);
        return config;
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
     * Returns the map of {@link com.hazelcast.core.ReplicatedMap}
     * configurations, mapped by config name. The config name may be a pattern
     * with which the configuration was initially obtained.
     *
     * @return the replicate map configurations mapped by config name
     */
    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return replicatedMapConfigs;
    }

    /**
     * Sets the map of {@link com.hazelcast.core.ReplicatedMap} configurations,
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public RingbufferConfig findRingbufferConfig(String name) {
        name = getBaseName(name);
        RingbufferConfig config = lookupByPattern(configPatternMatcher, ringbufferConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getRingbufferConfig("default").getAsReadOnly();
    }

    /**
     * Returns the RingbufferConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public RingbufferConfig getRingbufferConfig(String name) {
        name = getBaseName(name);
        RingbufferConfig config = lookupByPattern(configPatternMatcher, ringbufferConfigs, name);
        if (config != null) {
            return config;
        }
        RingbufferConfig defConfig = ringbufferConfigs.get("default");
        if (defConfig == null) {
            defConfig = new RingbufferConfig("default");
            addRingBufferConfig(defConfig);
        }
        config = new RingbufferConfig(name, defConfig);
        addRingBufferConfig(config);
        return config;
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public AtomicLongConfig findAtomicLongConfig(String name) {
        name = getBaseName(name);
        AtomicLongConfig config = lookupByPattern(configPatternMatcher, atomicLongConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getAtomicLongConfig("default").getAsReadOnly();
    }

    /**
     * Returns the AtomicLongConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking {@link #addAtomicLongConfig(AtomicLongConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the AtomicLong config
     * @return the AtomicLong configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public AtomicLongConfig getAtomicLongConfig(String name) {
        name = getBaseName(name);
        AtomicLongConfig config = lookupByPattern(configPatternMatcher, atomicLongConfigs, name);
        if (config != null) {
            return config;
        }
        AtomicLongConfig defConfig = atomicLongConfigs.get("default");
        if (defConfig == null) {
            defConfig = new AtomicLongConfig();
            defConfig.setName("default");
            addAtomicLongConfig(defConfig);
        }
        config = new AtomicLongConfig(defConfig);
        config.setName(name);
        addAtomicLongConfig(config);
        return config;
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
     * Returns a read-only AtomicReference configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the AtomicReference config
     * @return the AtomicReference configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public AtomicReferenceConfig findAtomicReferenceConfig(String name) {
        name = getBaseName(name);
        AtomicReferenceConfig config = lookupByPattern(configPatternMatcher, atomicReferenceConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getAtomicReferenceConfig("default").getAsReadOnly();
    }

    /**
     * Returns the AtomicReferenceConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking {@link #addAtomicReferenceConfig(AtomicReferenceConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the AtomicReference config
     * @return the AtomicReference configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public AtomicReferenceConfig getAtomicReferenceConfig(String name) {
        name = getBaseName(name);
        AtomicReferenceConfig config = lookupByPattern(configPatternMatcher, atomicReferenceConfigs, name);
        if (config != null) {
            return config;
        }
        AtomicReferenceConfig defConfig = atomicReferenceConfigs.get("default");
        if (defConfig == null) {
            defConfig = new AtomicReferenceConfig();
            defConfig.setName("default");
            addAtomicReferenceConfig(defConfig);
        }
        config = new AtomicReferenceConfig(defConfig);
        config.setName(name);
        addAtomicReferenceConfig(config);
        return config;
    }

    /**
     * Adds the AtomicReference configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param atomicReferenceConfig the AtomicReference configuration
     * @return this config instance
     */
    public Config addAtomicReferenceConfig(AtomicReferenceConfig atomicReferenceConfig) {
        atomicReferenceConfigs.put(atomicReferenceConfig.getName(), atomicReferenceConfig);
        return this;
    }

    /**
     * Returns the map of AtomicReference configurations, mapped by config name.
     * The config name may be a pattern with which the configuration was initially obtained.
     *
     * @return the AtomicReference configurations mapped by config name
     */
    public Map<String, AtomicReferenceConfig> getAtomicReferenceConfigs() {
        return atomicReferenceConfigs;
    }

    /**
     * Sets the map of AtomicReference configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be obtained in the future.
     *
     * @param atomicReferenceConfigs the AtomicReference configuration map to set
     * @return this config instance
     */
    public Config setAtomicReferenceConfigs(Map<String, AtomicReferenceConfig> atomicReferenceConfigs) {
        this.atomicReferenceConfigs.clear();
        this.atomicReferenceConfigs.putAll(atomicReferenceConfigs);
        for (Entry<String, AtomicReferenceConfig> entry : atomicReferenceConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only CountDownLatch configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the CountDownLatch config
     * @return the CountDownLatch configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public CountDownLatchConfig findCountDownLatchConfig(String name) {
        name = getBaseName(name);
        CountDownLatchConfig config = lookupByPattern(configPatternMatcher, countDownLatchConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getCountDownLatchConfig("default").getAsReadOnly();
    }

    /**
     * Returns the CountDownLatchConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking {@link #addCountDownLatchConfig(CountDownLatchConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the CountDownLatch config
     * @return the CountDownLatch configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public CountDownLatchConfig getCountDownLatchConfig(String name) {
        name = getBaseName(name);
        CountDownLatchConfig config = lookupByPattern(configPatternMatcher, countDownLatchConfigs, name);
        if (config != null) {
            return config;
        }
        CountDownLatchConfig defConfig = countDownLatchConfigs.get("default");
        if (defConfig == null) {
            defConfig = new CountDownLatchConfig();
            defConfig.setName("default");
            addCountDownLatchConfig(defConfig);
        }
        config = new CountDownLatchConfig(defConfig);
        config.setName(name);
        addCountDownLatchConfig(config);
        return config;
    }

    /**
     * Adds the CountDownLatch configuration. The configuration is saved under the config
     * name, which may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param countDownLatchConfig the CountDownLatch configuration
     * @return this config instance
     */
    public Config addCountDownLatchConfig(CountDownLatchConfig countDownLatchConfig) {
        countDownLatchConfigs.put(countDownLatchConfig.getName(), countDownLatchConfig);
        return this;
    }

    /**
     * Returns the map of CountDownLatch configurations, mapped by config name.
     * The config name may be a pattern with which the configuration was initially obtained.
     *
     * @return the CountDownLatch configurations mapped by config name
     */
    public Map<String, CountDownLatchConfig> getCountDownLatchConfigs() {
        return countDownLatchConfigs;
    }

    /**
     * Sets the map of CountDownLatch configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be obtained in the future.
     *
     * @param countDownLatchConfigs the CountDownLatch configuration map to set
     * @return this config instance
     */
    public Config setCountDownLatchConfigs(Map<String, CountDownLatchConfig> countDownLatchConfigs) {
        this.countDownLatchConfigs.clear();
        this.countDownLatchConfigs.putAll(countDownLatchConfigs);
        for (Entry<String, CountDownLatchConfig> entry : countDownLatchConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns a read-only {@link com.hazelcast.core.ITopic}
     * configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the topic config
     * @return the topic configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public TopicConfig findTopicConfig(String name) {
        name = getBaseName(name);
        TopicConfig config = lookupByPattern(configPatternMatcher, topicConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getTopicConfig("default").getAsReadOnly();
    }

    /**
     * Returns the TopicConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking {@link #addTopicConfig(TopicConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the topic config
     * @return the topic configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public TopicConfig getTopicConfig(String name) {
        name = getBaseName(name);
        TopicConfig config = lookupByPattern(configPatternMatcher, topicConfigs, name);
        if (config != null) {
            return config;
        }
        TopicConfig defConfig = topicConfigs.get("default");
        if (defConfig == null) {
            defConfig = new TopicConfig();
            defConfig.setName("default");
            addTopicConfig(defConfig);
        }
        config = new TopicConfig(defConfig);
        config.setName(name);
        addTopicConfig(config);
        return config;
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        name = getBaseName(name);
        ReliableTopicConfig config = lookupByPattern(configPatternMatcher, reliableTopicConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getReliableTopicConfig("default").getAsReadOnly();
    }

    /**
     * Returns the ReliableTopicConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ReliableTopicConfig getReliableTopicConfig(String name) {
        name = getBaseName(name);
        ReliableTopicConfig config = lookupByPattern(configPatternMatcher, reliableTopicConfigs, name);
        if (config != null) {
            return config;
        }
        ReliableTopicConfig defConfig = reliableTopicConfigs.get("default");
        if (defConfig == null) {
            defConfig = new ReliableTopicConfig("default");
            addReliableTopicConfig(defConfig);
        }
        config = new ReliableTopicConfig(defConfig, name);
        addReliableTopicConfig(config);
        return config;
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
     * Sets the map of {@link com.hazelcast.core.ITopic} configurations,
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public ExecutorConfig findExecutorConfig(String name) {
        name = getBaseName(name);
        ExecutorConfig config = lookupByPattern(configPatternMatcher, executorConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getExecutorConfig("default").getAsReadOnly();
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        name = getBaseName(name);
        DurableExecutorConfig config = lookupByPattern(configPatternMatcher, durableExecutorConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getDurableExecutorConfig("default").getAsReadOnly();
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        name = getBaseName(name);
        ScheduledExecutorConfig config = lookupByPattern(configPatternMatcher, scheduledExecutorConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getScheduledExecutorConfig("default").getAsReadOnly();
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        name = getBaseName(name);
        CardinalityEstimatorConfig config = lookupByPattern(configPatternMatcher, cardinalityEstimatorConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getCardinalityEstimatorConfig("default").getAsReadOnly();
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public PNCounterConfig findPNCounterConfig(String name) {
        name = getBaseName(name);
        PNCounterConfig config = lookupByPattern(configPatternMatcher, pnCounterConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getPNCounterConfig("default").getAsReadOnly();
    }

    /**
     * Returns the ExecutorConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking {@link #addExecutorConfig(ExecutorConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the executor config
     * @return the executor configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ExecutorConfig getExecutorConfig(String name) {
        name = getBaseName(name);
        ExecutorConfig config = lookupByPattern(configPatternMatcher, executorConfigs, name);
        if (config != null) {
            return config;
        }
        ExecutorConfig defConfig = executorConfigs.get("default");
        if (defConfig == null) {
            defConfig = new ExecutorConfig();
            defConfig.setName("default");
            addExecutorConfig(defConfig);
        }
        config = new ExecutorConfig(defConfig);
        config.setName(name);
        addExecutorConfig(config);
        return config;
    }

    /**
     * Returns the DurableExecutorConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        name = getBaseName(name);
        DurableExecutorConfig config = lookupByPattern(configPatternMatcher, durableExecutorConfigs, name);
        if (config != null) {
            return config;
        }
        DurableExecutorConfig defConfig = durableExecutorConfigs.get("default");
        if (defConfig == null) {
            defConfig = new DurableExecutorConfig();
            defConfig.setName("default");
            addDurableExecutorConfig(defConfig);
        }
        config = new DurableExecutorConfig(defConfig);
        config.setName(name);
        addDurableExecutorConfig(config);
        return config;
    }

    /**
     * Returns the ScheduledExecutorConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        name = getBaseName(name);
        ScheduledExecutorConfig config = lookupByPattern(configPatternMatcher, scheduledExecutorConfigs, name);
        if (config != null) {
            return config;
        }
        ScheduledExecutorConfig defConfig = scheduledExecutorConfigs.get("default");
        if (defConfig == null) {
            defConfig = new ScheduledExecutorConfig();
            defConfig.setName("default");
            addScheduledExecutorConfig(defConfig);
        }
        config = new ScheduledExecutorConfig(defConfig);
        config.setName(name);
        addScheduledExecutorConfig(config);
        return config;
    }

    /**
     * Returns the CardinalityEstimatorConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        name = getBaseName(name);
        CardinalityEstimatorConfig config = lookupByPattern(configPatternMatcher, cardinalityEstimatorConfigs, name);
        if (config != null) {
            return config;
        }
        CardinalityEstimatorConfig defConfig = cardinalityEstimatorConfigs.get("default");
        if (defConfig == null) {
            defConfig = new CardinalityEstimatorConfig();
            defConfig.setName("default");
            addCardinalityEstimatorConfig(defConfig);
        }
        config = new CardinalityEstimatorConfig(defConfig);
        config.setName(name);
        addCardinalityEstimatorConfig(config);
        return config;
    }

    /**
     * Returns the {@link PNCounterConfig} for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public PNCounterConfig getPNCounterConfig(String name) {
        name = getBaseName(name);
        PNCounterConfig config = lookupByPattern(configPatternMatcher, pnCounterConfigs, name);
        if (config != null) {
            return config;
        }
        PNCounterConfig defConfig = pnCounterConfigs.get("default");
        if (defConfig == null) {
            defConfig = new PNCounterConfig();
            defConfig.setName("default");
            addPNCounterConfig(defConfig);
        }
        config = new PNCounterConfig(defConfig);
        config.setName(name);
        addPNCounterConfig(config);
        return config;
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
     * Returns a read-only {@link com.hazelcast.core.ISemaphore}
     * configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the semaphore config
     * @return the semaphore configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public SemaphoreConfig findSemaphoreConfig(String name) {
        name = getBaseName(name);
        SemaphoreConfig config = lookupByPattern(configPatternMatcher, semaphoreConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getSemaphoreConfig("default").getAsReadOnly();
    }

    /**
     * Returns the SemaphoreConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking
     * {@link #addSemaphoreConfig(SemaphoreConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the semaphore config
     * @return the semaphore configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public SemaphoreConfig getSemaphoreConfig(String name) {
        name = getBaseName(name);
        SemaphoreConfig config = lookupByPattern(configPatternMatcher, semaphoreConfigs, name);
        if (config != null) {
            return config;
        }
        SemaphoreConfig defConfig = semaphoreConfigs.get("default");
        if (defConfig == null) {
            defConfig = new SemaphoreConfig();
            defConfig.setName("default");
            addSemaphoreConfig(defConfig);
        }
        config = new SemaphoreConfig(defConfig);
        config.setName(name);
        addSemaphoreConfig(config);
        return config;
    }

    /**
     * Adds the {@link com.hazelcast.core.ISemaphore} configuration.
     * The configuration is saved under the config name, which may be a
     * pattern with which the configuration will be obtained in the future.
     *
     * @param semaphoreConfig semaphoreConfig config to add
     * @return this config instance
     */
    public Config addSemaphoreConfig(SemaphoreConfig semaphoreConfig) {
        this.semaphoreConfigs.put(semaphoreConfig.getName(), semaphoreConfig);
        return this;
    }

    /**
     * Returns the collection of {@link com.hazelcast.core.ISemaphore} configs
     * added to this config object.
     *
     * @return semaphore configs
     */
    public Collection<SemaphoreConfig> getSemaphoreConfigs() {
        return semaphoreConfigs.values();
    }

    /**
     * Returns the map of {@link com.hazelcast.core.ISemaphore} configurations,
     * mapped by config name. The config name may be a pattern with which the
     * configuration was initially obtained.
     *
     * @return the semaphore configurations mapped by config name
     */
    public Map<String, SemaphoreConfig> getSemaphoreConfigsAsMap() {
        return semaphoreConfigs;
    }

    /**
     * Sets the map of semaphore configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param semaphoreConfigs the semaphore configuration map to set
     * @return this config instance
     */
    public Config setSemaphoreConfigs(Map<String, SemaphoreConfig> semaphoreConfigs) {
        this.semaphoreConfigs.clear();
        this.semaphoreConfigs.putAll(semaphoreConfigs);
        for (final Entry<String, SemaphoreConfig> entry : this.semaphoreConfigs.entrySet()) {
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
     * Returns a read-only {@link com.hazelcast.mapreduce.JobTracker}
     * configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the job tracker config
     * @return the job tracker configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public JobTrackerConfig findJobTrackerConfig(String name) {
        name = getBaseName(name);
        JobTrackerConfig config = lookupByPattern(configPatternMatcher, jobTrackerConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getJobTrackerConfig("default").getAsReadOnly();
    }

    /**
     * Returns the JobTrackerConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking
     * {@link #addJobTrackerConfig(JobTrackerConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the job tracker config
     * @return the job tracker configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public JobTrackerConfig getJobTrackerConfig(String name) {
        name = getBaseName(name);
        JobTrackerConfig config = lookupByPattern(configPatternMatcher, jobTrackerConfigs, name);
        if (config != null) {
            return config;
        }
        JobTrackerConfig defConfig = jobTrackerConfigs.get("default");
        if (defConfig == null) {
            defConfig = new JobTrackerConfig();
            defConfig.setName("default");
            addJobTrackerConfig(defConfig);
        }
        config = new JobTrackerConfig(defConfig);
        config.setName(name);
        addJobTrackerConfig(config);
        return config;
    }

    /**
     * Adds the {@link com.hazelcast.mapreduce.JobTracker} configuration.
     * The configuration is saved under the config name defined by
     * {@link JobTrackerConfig#getName()}.
     *
     * @param jobTrackerConfig semaphoreConfig config to add
     * @return this config instance
     */
    public Config addJobTrackerConfig(JobTrackerConfig jobTrackerConfig) {
        jobTrackerConfigs.put(jobTrackerConfig.getName(), jobTrackerConfig);
        return this;
    }

    /**
     * Returns the map of {@link com.hazelcast.mapreduce.JobTracker}
     * configurations, mapped by config name. The config name may be a pattern
     * with which the configuration was initially obtained.
     *
     * @return the WAN replication configurations mapped by config name
     */
    public Map<String, JobTrackerConfig> getJobTrackerConfigs() {
        return jobTrackerConfigs;
    }

    /**
     * Sets the map of job tracker configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param jobTrackerConfigs the job tracker configuration map to set
     * @return this config instance
     */
    public Config setJobTrackerConfigs(Map<String, JobTrackerConfig> jobTrackerConfigs) {
        this.jobTrackerConfigs.clear();
        this.jobTrackerConfigs.putAll(jobTrackerConfigs);
        for (final Entry<String, JobTrackerConfig> entry : this.jobTrackerConfigs.entrySet()) {
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
    public Map<String, QuorumConfig> getQuorumConfigs() {
        return quorumConfigs;
    }

    /**
     * Returns the QuorumConfig for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
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
     * explicitly adding it by invoking
     * {@link #addQuorumConfig(QuorumConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the split-brain protection config
     * @return the split-brain protection configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public QuorumConfig getQuorumConfig(String name) {
        name = getBaseName(name);
        QuorumConfig config = lookupByPattern(configPatternMatcher, quorumConfigs, name);
        if (config != null) {
            return config;
        }
        QuorumConfig defConfig = quorumConfigs.get("default");
        if (defConfig == null) {
            defConfig = new QuorumConfig();
            defConfig.setName("default");
            addQuorumConfig(defConfig);
        }
        config = new QuorumConfig(defConfig);
        config.setName(name);
        addQuorumConfig(config);
        return config;
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public QuorumConfig findQuorumConfig(String name) {
        name = getBaseName(name);
        QuorumConfig config = lookupByPattern(configPatternMatcher, quorumConfigs, name);
        if (config != null) {
            return config;
        }
        return getQuorumConfig("default");
    }

    /**
     * Sets the map of split-brain protection configurations, mapped by config
     * name. The config name may be a pattern with which the configuration
     * will be obtained in the future.
     *
     * @param quorumConfigs the split-brain protection configuration map to set
     * @return this config instance
     */
    public Config setQuorumConfigs(Map<String, QuorumConfig> quorumConfigs) {
        this.quorumConfigs.clear();
        this.quorumConfigs.putAll(quorumConfigs);
        for (final Entry<String, QuorumConfig> entry : this.quorumConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Adds the split-brain protection configuration.
     * The configuration is saved under the config name defined by
     * {@link QuorumConfig#getName()}.
     *
     * @param quorumConfig split-brain protection config to add
     * @return this config instance
     */
    public Config addQuorumConfig(QuorumConfig quorumConfig) {
        quorumConfigs.put(quorumConfig.getName(), quorumConfig);
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
     * Returns a read-only map {@link EventJournal}
     * configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the map event journal config
     * @return the map event journal configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public EventJournalConfig findMapEventJournalConfig(String name) {
        name = getBaseName(name);
        final EventJournalConfig config = lookupByPattern(configPatternMatcher, mapEventJournalConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getMapEventJournalConfig("default").getAsReadOnly();
    }

    /**
     * Returns a read-only cache {@link EventJournal}
     * configuration for the given name.
     * <p>
     * The name is matched by pattern to the configuration and by stripping the
     * partition ID qualifier from the given {@code name}.
     * If there is no config found by the name, it will return the configuration
     * with the name {@code default}.
     *
     * @param name name of the cache event journal config
     * @return the cache event journal configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     * @see EvictionConfig#setSize(int)
     */
    public EventJournalConfig findCacheEventJournalConfig(String name) {
        name = getBaseName(name);
        final EventJournalConfig config = lookupByPattern(configPatternMatcher, cacheEventJournalConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getCacheEventJournalConfig("default").getAsReadOnly();
    }

    /**
     * Returns the map event journal config for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * If there is no default config as well, it will create one and disable
     * the event journal by default.
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking
     * {@link #addEventJournalConfig(EventJournalConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the map event journal config
     * @return the map event journal configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public EventJournalConfig getMapEventJournalConfig(String name) {
        name = getBaseName(name);
        EventJournalConfig config = lookupByPattern(configPatternMatcher, mapEventJournalConfigs, name);
        if (config != null) {
            return config;
        }
        EventJournalConfig defConfig = mapEventJournalConfigs.get("default");
        if (defConfig == null) {
            defConfig = new EventJournalConfig().setMapName("default").setEnabled(false);
            addEventJournalConfig(defConfig);
        }
        config = new EventJournalConfig(defConfig).setMapName(name);
        addEventJournalConfig(config);
        return config;
    }

    /**
     * Returns the cache event journal config for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * If there is no default config as well, it will create one and disable
     * the event journal by default.
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking
     * {@link #addEventJournalConfig(EventJournalConfig)}.
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the cache event journal config
     * @return the cache event journal configuration
     * @throws ConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public EventJournalConfig getCacheEventJournalConfig(String name) {
        name = getBaseName(name);
        EventJournalConfig config = lookupByPattern(configPatternMatcher, cacheEventJournalConfigs, name);
        if (config != null) {
            return config;
        }
        EventJournalConfig defConfig = cacheEventJournalConfigs.get("default");
        if (defConfig == null) {
            defConfig = new EventJournalConfig().setCacheName("default").setEnabled(false);
            addEventJournalConfig(defConfig);
        }
        config = new EventJournalConfig(defConfig).setCacheName(name);
        addEventJournalConfig(config);
        return config;
    }

    /**
     * Adds the event journal configuration. The configuration may apply to a map
     * and/or cache. A non-empty value for {@link EventJournalConfig#getMapName()}
     * means the configuration applies to maps and a non-empty value for
     * {@link EventJournalConfig#getCacheName()} means the configuration
     * applies to caches.
     * The returned name may be a may be a pattern with which the configuration
     * will be obtained in the future.
     *
     * @param eventJournalConfig the event journal configuration
     * @return this config instance
     * @throws IllegalArgumentException if the
     *                                  {@link EventJournalConfig#getMapName()} and
     *                                  {@link EventJournalConfig#getCacheName()}
     *                                  are both empty
     */
    public Config addEventJournalConfig(EventJournalConfig eventJournalConfig) {
        final String mapName = eventJournalConfig.getMapName();
        final String cacheName = eventJournalConfig.getCacheName();
        if (StringUtil.isNullOrEmpty(mapName) && StringUtil.isNullOrEmpty(cacheName)) {
            throw new IllegalArgumentException("Event journal config should have either map name or cache name non-empty");
        }
        if (!StringUtil.isNullOrEmpty(mapName)) {
            mapEventJournalConfigs.put(mapName, eventJournalConfig);
        }
        if (!StringUtil.isNullOrEmpty(cacheName)) {
            cacheEventJournalConfigs.put(cacheName, eventJournalConfig);
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
     * @throws ConfigurationException if ambiguous configurations are found
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
     * The configuration is found by matching the the configuration name
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
     * @throws ConfigurationException if ambiguous configurations are found
     * @see com.hazelcast.partition.strategy.StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see #setConfigPatternMatcher(ConfigPatternMatcher)
     * @see #getConfigPatternMatcher()
     */
    public FlakeIdGeneratorConfig getFlakeIdGeneratorConfig(String name) {
        String baseName = getBaseName(name);
        FlakeIdGeneratorConfig config = lookupByPattern(configPatternMatcher, flakeIdGeneratorConfigMap, baseName);
        if (config != null) {
            return config;
        }
        FlakeIdGeneratorConfig defConfig = flakeIdGeneratorConfigMap.get("default");
        if (defConfig == null) {
            defConfig = new FlakeIdGeneratorConfig("default");
            flakeIdGeneratorConfigMap.put(defConfig.getName(), defConfig);
        }
        config = new FlakeIdGeneratorConfig(defConfig);
        config.setName(name);
        flakeIdGeneratorConfigMap.put(config.getName(), config);
        return config;
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
     * Returns the map of map event journal configurations, mapped by config
     * name. The config name may be a pattern with which the configuration was
     * initially obtained.
     *
     * @return the map event journal configurations mapped by config name
     */
    public Map<String, EventJournalConfig> getMapEventJournalConfigs() {
        return mapEventJournalConfigs;
    }

    /**
     * Returns the map of cache event journal configurations, mapped by config
     * name. The config name may be a pattern with which the configuration was
     * initially obtained.
     *
     * @return the cache event journal configurations mapped by config name
     */
    public Map<String, EventJournalConfig> getCacheEventJournalConfigs() {
        return cacheEventJournalConfigs;
    }

    /**
     * Sets the map of map event journal configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param eventJournalConfigs the map event journal configuration map to set
     * @return this config instance
     */
    public Config setMapEventJournalConfigs(Map<String, EventJournalConfig> eventJournalConfigs) {
        this.mapEventJournalConfigs.clear();
        this.mapEventJournalConfigs.putAll(eventJournalConfigs);
        for (Entry<String, EventJournalConfig> entry : eventJournalConfigs.entrySet()) {
            entry.getValue().setMapName(entry.getKey());
        }
        return this;
    }

    /**
     * Sets the map of cache event journal configurations, mapped by config name.
     * The config name may be a pattern with which the configuration will be
     * obtained in the future.
     *
     * @param eventJournalConfigs the cache event journal configuration map to set
     * @return this config instance
     */
    public Config setCacheEventJournalConfigs(Map<String, EventJournalConfig> eventJournalConfigs) {
        this.cacheEventJournalConfigs.clear();
        this.cacheEventJournalConfigs.putAll(eventJournalConfigs);
        for (Entry<String, EventJournalConfig> entry : eventJournalConfigs.entrySet()) {
            entry.getValue().setCacheName(entry.getKey());
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
     * Sets the replication configuration for {@link com.hazelcast.crdt.CRDT}
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
     * Returns the {@link URL} to the XML configuration, which has been parsed
     * to create this {@link Config} instance.
     *
     * @return the configuration URL
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
     * Returns the {@link File} to the XML configuration, which has been
     * parsed to create this {@link Config} instance.
     *
     * @return the configuration file
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
     */
    public String getLicenseKey() {
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
                + ", semaphoreConfigs=" + semaphoreConfigs
                + ", countDownLatchConfigs=" + countDownLatchConfigs
                + ", ringbufferConfigs=" + ringbufferConfigs
                + ", atomicLongConfigs=" + atomicLongConfigs
                + ", atomicReferenceConfigs=" + atomicReferenceConfigs
                + ", wanReplicationConfigs=" + wanReplicationConfigs
                + ", listenerConfigs=" + listenerConfigs
                + ", mapEventJournalConfigs=" + mapEventJournalConfigs
                + ", cacheEventJournalConfigs=" + cacheEventJournalConfigs
                + ", partitionGroupConfig=" + partitionGroupConfig
                + ", managementCenterConfig=" + managementCenterConfig
                + ", securityConfig=" + securityConfig
                + ", liteMember=" + liteMember
                + ", crdtReplicationConfig=" + crdtReplicationConfig
                + '}';
    }
}
