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

package com.hazelcast.config;

import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.StringUtil;

import java.io.File;
import java.net.URL;
import java.util.Collection;
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
 * Contains all the configuration to start a {@link com.hazelcast.core.HazelcastInstance}. A Config can be created
 * programmatically, but can also be configured using XML, see {@link com.hazelcast.config.XmlConfigBuilder}.
 * <p>
 * Config instances can be shared between threads, but should not be modified after they are used to create HazelcastInstances.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
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

    private final Map<String, ReplicatedMapConfig> replicatedMapConfigs = new ConcurrentHashMap<String, ReplicatedMapConfig>();

    private final Map<String, WanReplicationConfig> wanReplicationConfigs = new ConcurrentHashMap<String, WanReplicationConfig>();

    private final Map<String, JobTrackerConfig> jobTrackerConfigs = new ConcurrentHashMap<String, JobTrackerConfig>();

    private final Map<String, QuorumConfig> quorumConfigs = new ConcurrentHashMap<String, QuorumConfig>();

    private final Map<String, RingbufferConfig> ringbufferConfigs = new ConcurrentHashMap<String, RingbufferConfig>();

    private final Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs =
            new ConcurrentHashMap<String, CardinalityEstimatorConfig>();

    private final Map<String, EventJournalConfig> mapEventJournalConfigs = new ConcurrentHashMap<String, EventJournalConfig>();

    private final Map<String, EventJournalConfig> cacheEventJournalConfigs = new ConcurrentHashMap<String, EventJournalConfig>();

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

    public ConfigPatternMatcher getConfigPatternMatcher() {
        return configPatternMatcher;
    }

    public void setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        if (configPatternMatcher == null) {
            throw new IllegalArgumentException("ConfigPatternMatcher is not allowed to be null!");
        }
        this.configPatternMatcher = configPatternMatcher;
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
     * @return configured {@link Config} for chaining
     */
    public Config setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    public MemberAttributeConfig getMemberAttributeConfig() {
        return memberAttributeConfig;
    }

    public void setMemberAttributeConfig(MemberAttributeConfig memberAttributeConfig) {
        this.memberAttributeConfig = memberAttributeConfig;
    }

    public Properties getProperties() {
        return properties;
    }

    public Config setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public Config setInstanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    public GroupConfig getGroupConfig() {
        return groupConfig;
    }

    public Config setGroupConfig(GroupConfig groupConfig) {
        this.groupConfig = groupConfig;
        return this;
    }

    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    public Config setNetworkConfig(NetworkConfig networkConfig) {
        this.networkConfig = networkConfig;
        return this;
    }

    public MapConfig findMapConfig(String name) {
        String baseName = getBaseName(name);
        MapConfig config = lookupByPattern(configPatternMatcher, mapConfigs, baseName);
        if (config != null) {
            initDefaultMaxSizeForOnHeapMaps(config.getNearCacheConfig());
            return config.getAsReadOnly();
        }
        return getMapConfig("default").getAsReadOnly();
    }

    public MapConfig getMapConfig(String name) {
        String baseName = getBaseName(name);
        MapConfig config = lookupByPattern(configPatternMatcher, mapConfigs, baseName);
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

    public Config addMapConfig(MapConfig mapConfig) {
        mapConfigs.put(mapConfig.getName(), mapConfig);
        return this;
    }

    /**
     * @return the mapConfigs
     */
    public Map<String, MapConfig> getMapConfigs() {
        return mapConfigs;
    }

    /**
     * @param mapConfigs the mapConfigs to set
     */
    public Config setMapConfigs(Map<String, MapConfig> mapConfigs) {
        this.mapConfigs.clear();
        this.mapConfigs.putAll(mapConfigs);
        for (final Entry<String, MapConfig> entry : this.mapConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public CacheSimpleConfig findCacheConfig(String name) {
        name = getBaseName(name);
        return lookupByPattern(configPatternMatcher, cacheConfigs, name);
    }

    public CacheSimpleConfig getCacheConfig(String name) {
        String baseName = getBaseName(name);
        CacheSimpleConfig config = lookupByPattern(configPatternMatcher, cacheConfigs, baseName);
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

    public Config addCacheConfig(CacheSimpleConfig cacheConfig) {
        cacheConfigs.put(cacheConfig.getName(), cacheConfig);
        return this;
    }

    /**
     * @return the cacheConfigs
     */
    public Map<String, CacheSimpleConfig> getCacheConfigs() {
        return cacheConfigs;
    }

    /**
     * @param cacheConfigs the cacheConfigs to set
     */
    public Config setCacheConfigs(Map<String, CacheSimpleConfig> cacheConfigs) {
        this.cacheConfigs.clear();
        this.cacheConfigs.putAll(cacheConfigs);
        for (final Entry<String, CacheSimpleConfig> entry : this.cacheConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public QueueConfig findQueueConfig(String name) {
        String baseName = getBaseName(name);
        QueueConfig config = lookupByPattern(configPatternMatcher, queueConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getQueueConfig("default").getAsReadOnly();
    }

    public QueueConfig getQueueConfig(String name) {
        String baseName = getBaseName(name);
        QueueConfig config = lookupByPattern(configPatternMatcher, queueConfigs, baseName);
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

    public Config addQueueConfig(QueueConfig queueConfig) {
        queueConfigs.put(queueConfig.getName(), queueConfig);
        return this;
    }

    public Map<String, QueueConfig> getQueueConfigs() {
        return queueConfigs;
    }

    public Config setQueueConfigs(Map<String, QueueConfig> queueConfigs) {
        this.queueConfigs.clear();
        this.queueConfigs.putAll(queueConfigs);
        for (Entry<String, QueueConfig> entry : queueConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public LockConfig findLockConfig(String name) {
        final String baseName = getBaseName(name);
        final LockConfig config = lookupByPattern(configPatternMatcher, lockConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getLockConfig("default").getAsReadOnly();
    }

    public LockConfig getLockConfig(String name) {
        final String baseName = getBaseName(name);
        LockConfig config = lookupByPattern(configPatternMatcher, lockConfigs, baseName);
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

    public Config addLockConfig(LockConfig lockConfig) {
        lockConfigs.put(lockConfig.getName(), lockConfig);
        return this;
    }

    public Map<String, LockConfig> getLockConfigs() {
        return lockConfigs;
    }

    public Config setLockConfigs(Map<String, LockConfig> lockConfigs) {
        this.lockConfigs.clear();
        this.lockConfigs.putAll(lockConfigs);
        for (Entry<String, LockConfig> entry : lockConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public ListConfig findListConfig(String name) {
        String baseName = getBaseName(name);
        ListConfig config = lookupByPattern(configPatternMatcher, listConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getListConfig("default").getAsReadOnly();
    }

    public ListConfig getListConfig(String name) {
        String baseName = getBaseName(name);
        ListConfig config = lookupByPattern(configPatternMatcher, listConfigs, baseName);
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

    public Config addListConfig(ListConfig listConfig) {
        listConfigs.put(listConfig.getName(), listConfig);
        return this;
    }

    public Map<String, ListConfig> getListConfigs() {
        return listConfigs;
    }

    public Config setListConfigs(Map<String, ListConfig> listConfigs) {
        this.listConfigs.clear();
        this.listConfigs.putAll(listConfigs);
        for (Entry<String, ListConfig> entry : listConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public SetConfig findSetConfig(String name) {
        String baseName = getBaseName(name);
        SetConfig config = lookupByPattern(configPatternMatcher, setConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getSetConfig("default").getAsReadOnly();
    }

    public SetConfig getSetConfig(String name) {
        String baseName = getBaseName(name);
        SetConfig config = lookupByPattern(configPatternMatcher, setConfigs, baseName);
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

    public Config addSetConfig(SetConfig setConfig) {
        setConfigs.put(setConfig.getName(), setConfig);
        return this;
    }

    public Map<String, SetConfig> getSetConfigs() {
        return setConfigs;
    }

    public Config setSetConfigs(Map<String, SetConfig> setConfigs) {
        this.setConfigs.clear();
        this.setConfigs.putAll(setConfigs);
        for (Entry<String, SetConfig> entry : setConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public MultiMapConfig findMultiMapConfig(String name) {
        String baseName = getBaseName(name);
        MultiMapConfig config = lookupByPattern(configPatternMatcher, multiMapConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getMultiMapConfig("default").getAsReadOnly();
    }

    public MultiMapConfig getMultiMapConfig(String name) {
        String baseName = getBaseName(name);
        MultiMapConfig config = lookupByPattern(configPatternMatcher, multiMapConfigs, baseName);
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

    public Config addMultiMapConfig(MultiMapConfig multiMapConfig) {
        multiMapConfigs.put(multiMapConfig.getName(), multiMapConfig);
        return this;
    }

    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        return multiMapConfigs;
    }

    public Config setMultiMapConfigs(Map<String, MultiMapConfig> multiMapConfigs) {
        this.multiMapConfigs.clear();
        this.multiMapConfigs.putAll(multiMapConfigs);
        for (final Entry<String, MultiMapConfig> entry : this.multiMapConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        ReplicatedMapConfig config = lookupByPattern(configPatternMatcher, replicatedMapConfigs, name);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getReplicatedMapConfig("default").getAsReadOnly();
    }

    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
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

    public Config addReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        replicatedMapConfigs.put(replicatedMapConfig.getName(), replicatedMapConfig);
        return this;
    }

    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return replicatedMapConfigs;
    }

    public Config setReplicatedMapConfigs(Map<String, ReplicatedMapConfig> replicatedMapConfigs) {
        this.replicatedMapConfigs.clear();
        this.replicatedMapConfigs.putAll(replicatedMapConfigs);
        for (final Entry<String, ReplicatedMapConfig> entry : this.replicatedMapConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public RingbufferConfig findRingbufferConfig(String name) {
        String baseName = getBaseName(name);
        RingbufferConfig config = lookupByPattern(configPatternMatcher, ringbufferConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getRingbufferConfig("default").getAsReadOnly();
    }

    public RingbufferConfig getRingbufferConfig(String name) {
        String baseName = getBaseName(name);
        RingbufferConfig config = lookupByPattern(configPatternMatcher, ringbufferConfigs, baseName);
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

    public Config addRingBufferConfig(RingbufferConfig ringbufferConfig) {
        ringbufferConfigs.put(ringbufferConfig.getName(), ringbufferConfig);
        return this;
    }

    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        return ringbufferConfigs;
    }

    public Config setRingbufferConfigs(Map<String, RingbufferConfig> ringbufferConfigs) {
        this.ringbufferConfigs.clear();
        this.ringbufferConfigs.putAll(ringbufferConfigs);
        for (Entry<String, RingbufferConfig> entry : ringbufferConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public TopicConfig findTopicConfig(String name) {
        String baseName = getBaseName(name);
        TopicConfig config = lookupByPattern(configPatternMatcher, topicConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getTopicConfig("default").getAsReadOnly();
    }

    public TopicConfig getTopicConfig(String name) {
        String baseName = getBaseName(name);
        TopicConfig config = lookupByPattern(configPatternMatcher, topicConfigs, baseName);
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

    public Config addTopicConfig(TopicConfig topicConfig) {
        topicConfigs.put(topicConfig.getName(), topicConfig);
        return this;
    }

    public ReliableTopicConfig findReliableTopicConfig(String name) {
        String baseName = getBaseName(name);
        ReliableTopicConfig config = lookupByPattern(configPatternMatcher, reliableTopicConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getReliableTopicConfig("default").getAsReadOnly();
    }

    public ReliableTopicConfig getReliableTopicConfig(String name) {
        String baseName = getBaseName(name);
        ReliableTopicConfig config = lookupByPattern(configPatternMatcher, reliableTopicConfigs, baseName);
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
     * @return the reliable topic configs
     */
    public Map<String, ReliableTopicConfig> getReliableTopicConfigs() {
        return reliableTopicConfigs;
    }

    public Config addReliableTopicConfig(ReliableTopicConfig topicConfig) {
        reliableTopicConfigs.put(topicConfig.getName(), topicConfig);
        return this;
    }

    public Config setReliableTopicConfigs(Map<String, ReliableTopicConfig> reliableTopicConfigs) {
        this.reliableTopicConfigs.clear();
        this.reliableTopicConfigs.putAll(reliableTopicConfigs);
        for (Entry<String, ReliableTopicConfig> entry : reliableTopicConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * @return the topicConfigs
     */
    public Map<String, TopicConfig> getTopicConfigs() {
        return topicConfigs;
    }

    /**
     * @param mapTopicConfigs the topicConfigs to set
     */
    public Config setTopicConfigs(Map<String, TopicConfig> mapTopicConfigs) {
        this.topicConfigs.clear();
        this.topicConfigs.putAll(mapTopicConfigs);
        for (final Entry<String, TopicConfig> entry : this.topicConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public ExecutorConfig findExecutorConfig(String name) {
        String baseName = getBaseName(name);
        ExecutorConfig config = lookupByPattern(configPatternMatcher, executorConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getExecutorConfig("default").getAsReadOnly();
    }

    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        String baseName = getBaseName(name);
        DurableExecutorConfig config = lookupByPattern(configPatternMatcher, durableExecutorConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getDurableExecutorConfig("default").getAsReadOnly();
    }

    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        String baseName = getBaseName(name);
        ScheduledExecutorConfig config = lookupByPattern(configPatternMatcher, scheduledExecutorConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getScheduledExecutorConfig("default").getAsReadOnly();
    }

    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        String baseName = getBaseName(name);
        CardinalityEstimatorConfig config = lookupByPattern(configPatternMatcher, cardinalityEstimatorConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getCardinalityEstimatorConfig("default").getAsReadOnly();
    }

    /**
     * Returns the ExecutorConfig for the given name
     *
     * @param name name of the executor config
     * @return ExecutorConfig
     */
    public ExecutorConfig getExecutorConfig(String name) {
        String baseName = getBaseName(name);
        ExecutorConfig config = lookupByPattern(configPatternMatcher, executorConfigs, baseName);
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
     * Returns the DurableExecutorConfig for the given name
     *
     * @param name name of the durable executor config
     * @return DurableExecutorConfig
     */
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        String baseName = getBaseName(name);
        DurableExecutorConfig config = lookupByPattern(configPatternMatcher, durableExecutorConfigs, baseName);
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
     * Returns the ScheduledExecutorConfig for the given name
     *
     * @param name name of the scheduled executor config
     * @return ScheduledExecutorConfig
     */
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        String baseName = getBaseName(name);
        ScheduledExecutorConfig config = lookupByPattern(configPatternMatcher, scheduledExecutorConfigs, baseName);
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
     * Returns the CardinalityEstimatorConfig for the given name
     *
     * @param name name of the cardinality estimator config
     * @return CardinalityEstimatorConfig
     */
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        String baseName = getBaseName(name);
        CardinalityEstimatorConfig config = lookupByPattern(configPatternMatcher, cardinalityEstimatorConfigs, baseName);
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
     * Adds a new ExecutorConfig by name
     *
     * @param executorConfig executor config to add
     * @return this config instance
     */
    public Config addExecutorConfig(ExecutorConfig executorConfig) {
        this.executorConfigs.put(executorConfig.getName(), executorConfig);
        return this;
    }

    /**
     * Adds a new DurableExecutorConfig by name
     *
     * @param durableExecutorConfig executor config to add
     * @return this config instance
     */
    public Config addDurableExecutorConfig(DurableExecutorConfig durableExecutorConfig) {
        this.durableExecutorConfigs.put(durableExecutorConfig.getName(), durableExecutorConfig);
        return this;
    }

    /**
     * Adds a new ScheduledExecutorConfig by name
     *
     * @param scheduledExecutorConfig executor config to add
     * @return this config instance
     */
    public Config addScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig) {
        this.scheduledExecutorConfigs.put(scheduledExecutorConfig.getName(), scheduledExecutorConfig);
        return this;
    }

    /**
     * Adds a new CardinalityEstimatorConfig by name
     *
     * @param cardinalityEstimatorConfig estimator config to add
     * @return this config instance
     */
    public Config addCardinalityEstimatorConfig(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        this.cardinalityEstimatorConfigs.put(cardinalityEstimatorConfig.getName(), cardinalityEstimatorConfig);
        return this;
    }

    public Map<String, ExecutorConfig> getExecutorConfigs() {
        return executorConfigs;
    }

    public Config setExecutorConfigs(Map<String, ExecutorConfig> executorConfigs) {
        this.executorConfigs.clear();
        this.executorConfigs.putAll(executorConfigs);
        for (Entry<String, ExecutorConfig> entry : executorConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        return durableExecutorConfigs;
    }

    public Config setDurableExecutorConfigs(Map<String, DurableExecutorConfig> durableExecutorConfigs) {
        this.durableExecutorConfigs.clear();
        this.durableExecutorConfigs.putAll(durableExecutorConfigs);
        for (Entry<String, DurableExecutorConfig> entry : durableExecutorConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        return scheduledExecutorConfigs;
    }

    public Config setScheduledExecutorConfigs(Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs) {
        this.scheduledExecutorConfigs.clear();
        this.scheduledExecutorConfigs.putAll(scheduledExecutorConfigs);
        for (Entry<String, ScheduledExecutorConfig> entry : scheduledExecutorConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        return cardinalityEstimatorConfigs;
    }

    public Config setCardinalityEstimatorConfigs(Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs) {
        this.cardinalityEstimatorConfigs.clear();
        this.cardinalityEstimatorConfigs.putAll(cardinalityEstimatorConfigs);
        for (Entry<String, CardinalityEstimatorConfig> entry : cardinalityEstimatorConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public SemaphoreConfig findSemaphoreConfig(String name) {
        String baseName = getBaseName(name);
        SemaphoreConfig config = lookupByPattern(configPatternMatcher, semaphoreConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getSemaphoreConfig("default").getAsReadOnly();
    }

    /**
     * Returns the SemaphoreConfig for the given name
     *
     * @param name name of the semaphore config
     * @return SemaphoreConfig
     */
    public SemaphoreConfig getSemaphoreConfig(String name) {
        String baseName = getBaseName(name);
        SemaphoreConfig config = lookupByPattern(configPatternMatcher, semaphoreConfigs, baseName);
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
     * Adds a new SemaphoreConfig by name
     *
     * @param semaphoreConfig semaphore config to add
     * @return this config instance
     */
    public Config addSemaphoreConfig(SemaphoreConfig semaphoreConfig) {
        this.semaphoreConfigs.put(semaphoreConfig.getName(), semaphoreConfig);
        return this;
    }

    /**
     * Returns the collection of semaphore configs.
     *
     * @return collection of semaphore configs
     */
    public Collection<SemaphoreConfig> getSemaphoreConfigs() {
        return semaphoreConfigs.values();
    }

    public Map<String, SemaphoreConfig> getSemaphoreConfigsAsMap() {
        return semaphoreConfigs;
    }

    public Config setSemaphoreConfigs(Map<String, SemaphoreConfig> semaphoreConfigs) {
        this.semaphoreConfigs.clear();
        this.semaphoreConfigs.putAll(semaphoreConfigs);
        for (final Entry<String, SemaphoreConfig> entry : this.semaphoreConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public WanReplicationConfig getWanReplicationConfig(String name) {
        return wanReplicationConfigs.get(name);
    }

    public Config addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        wanReplicationConfigs.put(wanReplicationConfig.getName(), wanReplicationConfig);
        return this;
    }

    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        return wanReplicationConfigs;
    }

    public Config setWanReplicationConfigs(Map<String, WanReplicationConfig> wanReplicationConfigs) {
        this.wanReplicationConfigs.clear();
        this.wanReplicationConfigs.putAll(wanReplicationConfigs);
        return this;
    }

    public JobTrackerConfig findJobTrackerConfig(String name) {
        String baseName = getBaseName(name);
        JobTrackerConfig config = lookupByPattern(configPatternMatcher, jobTrackerConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getJobTrackerConfig(name);
    }

    public JobTrackerConfig getJobTrackerConfig(String name) {
        String baseName = getBaseName(name);
        JobTrackerConfig config = lookupByPattern(configPatternMatcher, jobTrackerConfigs, baseName);
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

    public Config addJobTrackerConfig(JobTrackerConfig jobTrackerConfig) {
        jobTrackerConfigs.put(jobTrackerConfig.getName(), jobTrackerConfig);
        return this;
    }

    public Map<String, JobTrackerConfig> getJobTrackerConfigs() {
        return jobTrackerConfigs;
    }

    public Config setJobTrackerConfigs(Map<String, JobTrackerConfig> jobTrackerConfigs) {
        this.jobTrackerConfigs.clear();
        this.jobTrackerConfigs.putAll(jobTrackerConfigs);
        for (final Entry<String, JobTrackerConfig> entry : this.jobTrackerConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public Map<String, QuorumConfig> getQuorumConfigs() {
        return quorumConfigs;
    }

    public QuorumConfig getQuorumConfig(String name) {
        String baseName = getBaseName(name);
        QuorumConfig config = lookupByPattern(configPatternMatcher, quorumConfigs, baseName);
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

    public QuorumConfig findQuorumConfig(String name) {
        String baseName = getBaseName(name);
        QuorumConfig config = lookupByPattern(configPatternMatcher, quorumConfigs, baseName);
        if (config != null) {
            return config;
        }
        return getQuorumConfig("default");
    }

    public Config setQuorumConfigs(Map<String, QuorumConfig> quorumConfigs) {
        this.quorumConfigs.clear();
        this.quorumConfigs.putAll(quorumConfigs);
        for (final Entry<String, QuorumConfig> entry : this.quorumConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    public Config addQuorumConfig(QuorumConfig quorumConfig) {
        quorumConfigs.put(quorumConfig.getName(), quorumConfig);
        return this;
    }

    public ManagementCenterConfig getManagementCenterConfig() {
        return managementCenterConfig;
    }

    public Config setManagementCenterConfig(ManagementCenterConfig managementCenterConfig) {
        this.managementCenterConfig = managementCenterConfig;
        return this;
    }

    public ServicesConfig getServicesConfig() {
        return servicesConfig;
    }

    public Config setServicesConfig(ServicesConfig servicesConfig) {
        this.servicesConfig = servicesConfig;
        return this;
    }

    public SecurityConfig getSecurityConfig() {
        return securityConfig;
    }

    public Config setSecurityConfig(SecurityConfig securityConfig) {
        this.securityConfig = securityConfig;
        return this;
    }

    public Config addListenerConfig(ListenerConfig listenerConfig) {
        getListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<ListenerConfig> getListenerConfigs() {
        return listenerConfigs;
    }

    public Config setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        this.listenerConfigs.clear();
        this.listenerConfigs.addAll(listenerConfigs);
        return this;
    }

    public EventJournalConfig findMapEventJournalConfig(String name) {
        final String baseName = getBaseName(name);
        final EventJournalConfig config = lookupByPattern(configPatternMatcher, mapEventJournalConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getMapEventJournalConfig("default").getAsReadOnly();
    }

    public EventJournalConfig findCacheEventJournalConfig(String name) {
        final String baseName = getBaseName(name);
        final EventJournalConfig config = lookupByPattern(configPatternMatcher, cacheEventJournalConfigs, baseName);
        if (config != null) {
            return config.getAsReadOnly();
        }
        return getCacheEventJournalConfig("default").getAsReadOnly();
    }

    public EventJournalConfig getMapEventJournalConfig(String name) {
        final String baseName = getBaseName(name);
        EventJournalConfig config = lookupByPattern(configPatternMatcher, mapEventJournalConfigs, baseName);
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

    public EventJournalConfig getCacheEventJournalConfig(String name) {
        final String baseName = getBaseName(name);
        EventJournalConfig config = lookupByPattern(configPatternMatcher, cacheEventJournalConfigs, baseName);
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

    public Config addEventJournalConfig(EventJournalConfig eventJournalConfig) {
        final String mapName = eventJournalConfig.getMapName();
        final String cacheName = eventJournalConfig.getCacheName();
        if (StringUtil.isNullOrEmpty(mapName) && StringUtil.isNullOrEmpty(cacheName)) {
            throw new IllegalArgumentException("Event journal config should have non-empty map name and/or cache name");
        }
        if (!StringUtil.isNullOrEmpty(mapName)) {
            mapEventJournalConfigs.put(eventJournalConfig.getMapName(), eventJournalConfig);
        }
        if (!StringUtil.isNullOrEmpty(cacheName)) {
            cacheEventJournalConfigs.put(eventJournalConfig.getCacheName(), eventJournalConfig);
        }
        return this;
    }

    public Map<String, EventJournalConfig> getMapEventJournalConfigs() {
        return mapEventJournalConfigs;
    }

    public Map<String, EventJournalConfig> getCacheEventJournalConfigs() {
        return cacheEventJournalConfigs;
    }

    public Config setMapEventJournalConfigs(Map<String, EventJournalConfig> eventJournalConfigs) {
        this.mapEventJournalConfigs.clear();
        this.mapEventJournalConfigs.putAll(eventJournalConfigs);
        for (Entry<String, EventJournalConfig> entry : eventJournalConfigs.entrySet()) {
            entry.getValue().setMapName(entry.getKey());
        }
        return this;
    }

    public Config setCacheEventJournalConfigs(Map<String, EventJournalConfig> eventJournalConfigs) {
        this.cacheEventJournalConfigs.clear();
        this.cacheEventJournalConfigs.putAll(eventJournalConfigs);
        for (Entry<String, EventJournalConfig> entry : eventJournalConfigs.entrySet()) {
            entry.getValue().setCacheName(entry.getKey());
        }
        return this;
    }

    public SerializationConfig getSerializationConfig() {
        return serializationConfig;
    }

    public Config setSerializationConfig(SerializationConfig serializationConfig) {
        this.serializationConfig = serializationConfig;
        return this;
    }

    public PartitionGroupConfig getPartitionGroupConfig() {
        return partitionGroupConfig;
    }

    public Config setPartitionGroupConfig(PartitionGroupConfig partitionGroupConfig) {
        this.partitionGroupConfig = partitionGroupConfig;
        return this;
    }

    /**
     * Returns hot restart configuration for this member
     *
     * @return hot restart configuration
     */
    public HotRestartPersistenceConfig getHotRestartPersistenceConfig() {
        return hotRestartPersistenceConfig;
    }

    /**
     * Sets hot restart configuration.
     *
     * @param hrConfig hot restart configuration
     * @return Config
     */
    public Config setHotRestartPersistenceConfig(HotRestartPersistenceConfig hrConfig) {
        checkNotNull(hrConfig, "Hot restart config cannot be null!");
        this.hotRestartPersistenceConfig = hrConfig;
        return this;
    }

    public ManagedContext getManagedContext() {
        return managedContext;
    }

    public Config setManagedContext(final ManagedContext managedContext) {
        this.managedContext = managedContext;
        return this;
    }

    public ConcurrentMap<String, Object> getUserContext() {
        return userContext;
    }

    public Config setUserContext(ConcurrentMap<String, Object> userContext) {
        if (userContext == null) {
            throw new IllegalArgumentException("userContext can't be null");
        }
        this.userContext = userContext;
        return this;
    }

    public NativeMemoryConfig getNativeMemoryConfig() {
        return nativeMemoryConfig;
    }

    public Config setNativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        this.nativeMemoryConfig = nativeMemoryConfig;
        return this;
    }

    /**
     * @return the configurationUrl
     */
    public URL getConfigurationUrl() {
        return configurationUrl;
    }

    /**
     * @param configurationUrl the configurationUrl to set
     */
    public Config setConfigurationUrl(URL configurationUrl) {
        this.configurationUrl = configurationUrl;
        return this;
    }

    /**
     * @return the configurationFile
     */
    public File getConfigurationFile() {
        return configurationFile;
    }

    /**
     * @param configurationFile the configurationFile to set
     */
    public Config setConfigurationFile(File configurationFile) {
        this.configurationFile = configurationFile;
        return this;
    }

    public String getLicenseKey() {
        return licenseKey;
    }

    public Config setLicenseKey(final String licenseKey) {
        this.licenseKey = licenseKey;
        return this;
    }

    /**
     * @return indicates if the node is a lite member or not. Lite members do not own any partition
     */
    public boolean isLiteMember() {
        return liteMember;
    }

    /**
     * @param liteMember sets if the node will be a lite member or not. Lite members do not own any partition
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
     * @param userCodeDeploymentConfig
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
                + ", ringbufferConfigs=" + ringbufferConfigs
                + ", wanReplicationConfigs=" + wanReplicationConfigs
                + ", listenerConfigs=" + listenerConfigs
                + ", mapEventJournalConfigs=" + mapEventJournalConfigs
                + ", cacheEventJournalConfigs=" + cacheEventJournalConfigs
                + ", partitionGroupConfig=" + partitionGroupConfig
                + ", managementCenterConfig=" + managementCenterConfig
                + ", securityConfig=" + securityConfig
                + ", liteMember=" + liteMember
                + '}';
    }
}
