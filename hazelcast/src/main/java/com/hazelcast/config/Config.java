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
import com.hazelcast.util.ConfigLookupUtil;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;
import static com.hazelcast.util.ConfigLookupUtil.CACHE_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.CARD_EST_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.DURABLE_EXECUTOR_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.EXECUTOR_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.JOB_TRACKER_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.LIST_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.LOCK_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.MAP_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.MULTI_MAP_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.QUEUE_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.QUORUM_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.RELIABLE_TOPIC_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.REP_MAP_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.RING_BUFFER_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.SCHEDULED_EXECUTOR_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.SEMAPHORE_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.SET_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.ConfigLookupUtil.TOPIC_CONFIG_CONSTRUCTOR;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains all the configuration to start a {@link com.hazelcast.core.HazelcastInstance}. A Config
 * can be created programmatically, but can also be configured using XML, see {@link com.hazelcast.config.XmlConfigBuilder}.
 * <p/>
 * Config instances can be shared between threads, but should not be modified after they are used to
 * create HazelcastInstances.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
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

    private final Map<String, SemaphoreConfig> semaphoreConfigs = new ConcurrentHashMap<String, SemaphoreConfig>();

    private final Map<String, ReplicatedMapConfig> replicatedMapConfigs = new ConcurrentHashMap<String, ReplicatedMapConfig>();

    private final Map<String, WanReplicationConfig> wanReplicationConfigs = new ConcurrentHashMap<String, WanReplicationConfig>();

    private final Map<String, JobTrackerConfig> jobTrackerConfigs = new ConcurrentHashMap<String, JobTrackerConfig>();

    private final Map<String, QuorumConfig> quorumConfigs = new ConcurrentHashMap<String, QuorumConfig>();

    private final Map<String, RingbufferConfig> ringbufferConfigs = new ConcurrentHashMap<String, RingbufferConfig>();

    private final Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs =
            new ConcurrentHashMap<String, CardinalityEstimatorConfig>();

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
     * <p> If null, then thread context class-loader will be used instead.
     *
     * @return the class-loader
     */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Sets the class-loader to be used during de-serialization
     * and as context class-loader of Hazelcast internal threads.
     * <p/>
     * <p/>
     * If not set (or set to null); thread context class-loader
     * will be used in required places.
     * <p/>
     * <p/>
     * Default value is null.
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
        return ConfigLookupUtil.findConfig(mapConfigs, name, MAP_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public MapConfig getMapConfig(String name) {
        return ConfigLookupUtil.getConfig(mapConfigs, name, MAP_CONFIG_CONSTRUCTOR, configPatternMatcher);
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
        ConfigLookupUtil.setConfig(this.mapConfigs, mapConfigs);
        return this;
    }

    public CacheSimpleConfig findCacheConfig(String name) {
        name = getBaseName(name);
        return lookupByPattern(cacheConfigs, name);
    }

    public CacheSimpleConfig getCacheConfig(String name) {
        return ConfigLookupUtil.getConfig(cacheConfigs, name, CACHE_CONFIG_CONSTRUCTOR, configPatternMatcher);
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
        ConfigLookupUtil.setConfig(this.cacheConfigs, cacheConfigs);
        return this;
    }

    public QueueConfig findQueueConfig(String name) {
        return ConfigLookupUtil.findConfig(queueConfigs, name, QUEUE_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public QueueConfig getQueueConfig(String name) {
        return ConfigLookupUtil.getConfig(queueConfigs, name, QUEUE_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public Config addQueueConfig(QueueConfig queueConfig) {
        queueConfigs.put(queueConfig.getName(), queueConfig);
        return this;
    }

    public Map<String, QueueConfig> getQueueConfigs() {
        return queueConfigs;
    }

    public Config setQueueConfigs(Map<String, QueueConfig> queueConfigs) {
        ConfigLookupUtil.setConfig(this.queueConfigs, queueConfigs);
        return this;
    }

    public LockConfig findLockConfig(String name) {
        return ConfigLookupUtil.findConfig(lockConfigs, name, LOCK_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public LockConfig getLockConfig(String name) {
        return ConfigLookupUtil.getConfig(lockConfigs, name, LOCK_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public Config addLockConfig(LockConfig lockConfig) {
        lockConfigs.put(lockConfig.getName(), lockConfig);
        return this;
    }

    public Map<String, LockConfig> getLockConfigs() {
        return lockConfigs;
    }

    public Config setLockConfigs(Map<String, LockConfig> lockConfigs) {
        ConfigLookupUtil.setConfig(this.lockConfigs, lockConfigs);
        return this;
    }

    public ListConfig findListConfig(String name) {
        return ConfigLookupUtil.findConfig(listConfigs, name, LIST_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public ListConfig getListConfig(String name) {
        return ConfigLookupUtil.getConfig(listConfigs, name, LIST_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public Config addListConfig(ListConfig listConfig) {
        listConfigs.put(listConfig.getName(), listConfig);
        return this;
    }

    public Map<String, ListConfig> getListConfigs() {
        return listConfigs;
    }

    public Config setListConfigs(Map<String, ListConfig> listConfigs) {
        ConfigLookupUtil.setConfig(this.listConfigs, listConfigs);
        return this;
    }

    public SetConfig findSetConfig(String name) {
        return ConfigLookupUtil.findConfig(setConfigs, name, SET_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public SetConfig getSetConfig(String name) {
        return ConfigLookupUtil.getConfig(setConfigs, name, SET_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public Config addSetConfig(SetConfig setConfig) {
        setConfigs.put(setConfig.getName(), setConfig);
        return this;
    }

    public Map<String, SetConfig> getSetConfigs() {
        return setConfigs;
    }

    public Config setSetConfigs(Map<String, SetConfig> setConfigs) {
        ConfigLookupUtil.setConfig(this.setConfigs, setConfigs);
        return this;
    }

    public MultiMapConfig findMultiMapConfig(String name) {
        return ConfigLookupUtil.findConfig(multiMapConfigs, name, MULTI_MAP_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public MultiMapConfig getMultiMapConfig(String name) {
        return ConfigLookupUtil.getConfig(multiMapConfigs, name, MULTI_MAP_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public Config addMultiMapConfig(MultiMapConfig multiMapConfig) {
        multiMapConfigs.put(multiMapConfig.getName(), multiMapConfig);
        return this;
    }

    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        return multiMapConfigs;
    }

    public Config setMultiMapConfigs(Map<String, MultiMapConfig> multiMapConfigs) {
        ConfigLookupUtil.setConfig(this.multiMapConfigs, multiMapConfigs);
        return this;
    }

    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        return ConfigLookupUtil.findConfig(replicatedMapConfigs, name, REP_MAP_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return ConfigLookupUtil.getConfig(replicatedMapConfigs, name, REP_MAP_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public Config addReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        replicatedMapConfigs.put(replicatedMapConfig.getName(), replicatedMapConfig);
        return this;
    }

    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return replicatedMapConfigs;
    }

    public Config setReplicatedMapConfigs(Map<String, ReplicatedMapConfig> replicatedMapConfigs) {
        ConfigLookupUtil.setConfig(this.replicatedMapConfigs, replicatedMapConfigs);
        return this;
    }

    public RingbufferConfig findRingbufferConfig(String name) {
        return ConfigLookupUtil.findConfig(ringbufferConfigs, name, RING_BUFFER_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public RingbufferConfig getRingbufferConfig(String name) {
        return ConfigLookupUtil.getConfig(ringbufferConfigs, name, RING_BUFFER_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public Config addRingBufferConfig(RingbufferConfig ringbufferConfig) {
        ringbufferConfigs.put(ringbufferConfig.getName(), ringbufferConfig);
        return this;
    }

    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        return ringbufferConfigs;
    }

    public Config setRingbufferConfigs(Map<String, RingbufferConfig> ringbufferConfigs) {
        ConfigLookupUtil.setConfig(this.ringbufferConfigs, ringbufferConfigs);
        return this;
    }

    public TopicConfig findTopicConfig(String name) {
        return ConfigLookupUtil.findConfig(topicConfigs, name, TOPIC_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public TopicConfig getTopicConfig(String name) {
        return ConfigLookupUtil.getConfig(topicConfigs, name, TOPIC_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public Config addTopicConfig(TopicConfig topicConfig) {
        topicConfigs.put(topicConfig.getName(), topicConfig);
        return this;
    }

    public ReliableTopicConfig findReliableTopicConfig(String name) {
        return ConfigLookupUtil.findConfig(reliableTopicConfigs, name, RELIABLE_TOPIC_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public ReliableTopicConfig getReliableTopicConfig(String name) {
        return ConfigLookupUtil.getConfig(reliableTopicConfigs, name, RELIABLE_TOPIC_CONFIG_CONSTRUCTOR, configPatternMatcher);
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
        ConfigLookupUtil.setConfig(this.reliableTopicConfigs, reliableTopicConfigs);
        return this;
    }


    /**
     * @return the topicConfigs
     */
    public Map<String, TopicConfig> getTopicConfigs() {
        return topicConfigs;
    }

    /**
     * @param topicConfigs the topicConfigs to set
     */
    public Config setTopicConfigs(Map<String, TopicConfig> topicConfigs) {
        ConfigLookupUtil.setConfig(this.topicConfigs, topicConfigs);
        return this;
    }

    public ExecutorConfig findExecutorConfig(String name) {
        return ConfigLookupUtil.findConfig(executorConfigs, name, EXECUTOR_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        return ConfigLookupUtil.findConfig(durableExecutorConfigs, name,
                DURABLE_EXECUTOR_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        return ConfigLookupUtil.findConfig(scheduledExecutorConfigs, name,
                SCHEDULED_EXECUTOR_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        return ConfigLookupUtil.findConfig(cardinalityEstimatorConfigs, name, CARD_EST_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    /**
     * Returns the ExecutorConfig for the given name
     *
     * @param name name of the executor config
     * @return ExecutorConfig
     */
    public ExecutorConfig getExecutorConfig(String name) {
        return ConfigLookupUtil.getConfig(executorConfigs, name, EXECUTOR_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    /**
     * Returns the DurableExecutorConfig for the given name
     *
     * @param name name of the durable executor config
     * @return DurableExecutorConfig
     */
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        return ConfigLookupUtil.getConfig(durableExecutorConfigs, name, DURABLE_EXECUTOR_CONFIG_CONSTRUCTOR,
                configPatternMatcher);
    }

    /**
     * Returns the ScheduledExecutorConfig for the given name
     *
     * @param name name of the scheduled executor config
     * @return ScheduledExecutorConfig
     */
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        return ConfigLookupUtil.getConfig(scheduledExecutorConfigs, name, SCHEDULED_EXECUTOR_CONFIG_CONSTRUCTOR,
                configPatternMatcher);
    }

    /**
     * Returns the CardinalityEstimatorConfig for the given name
     *
     * @param name name of the cardinality estimator config
     * @return CardinalityEstimatorConfig
     */
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        return ConfigLookupUtil.getConfig(cardinalityEstimatorConfigs, name, CARD_EST_CONFIG_CONSTRUCTOR, configPatternMatcher);
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
        ConfigLookupUtil.setConfig(this.executorConfigs, executorConfigs);
        return this;
    }

    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        return durableExecutorConfigs;
    }

    public Config setDurableExecutorConfigs(Map<String, DurableExecutorConfig> durableExecutorConfigs) {
        ConfigLookupUtil.setConfig(this.durableExecutorConfigs, durableExecutorConfigs);
        return this;
    }

    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        return scheduledExecutorConfigs;
    }

    public Config setScheduledExecutorConfigs(Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs) {
        ConfigLookupUtil.setConfig(this.scheduledExecutorConfigs, scheduledExecutorConfigs);
        return this;
    }

    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        return cardinalityEstimatorConfigs;
    }

    public Config setCardinalityEstimatorConfigs(Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs) {
        ConfigLookupUtil.setConfig(this.cardinalityEstimatorConfigs, cardinalityEstimatorConfigs);
        return this;
    }

    public SemaphoreConfig findSemaphoreConfig(String name) {
        return ConfigLookupUtil.findConfig(semaphoreConfigs, name, SEMAPHORE_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    /**
     * Returns the SemaphoreConfig for the given name
     *
     * @param name name of the semaphore config
     * @return SemaphoreConfig
     */
    public SemaphoreConfig getSemaphoreConfig(String name) {
        return ConfigLookupUtil.getConfig(semaphoreConfigs, name, SEMAPHORE_CONFIG_CONSTRUCTOR, configPatternMatcher);
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
     * @return collection of semaphore configs.
     */
    public Collection<SemaphoreConfig> getSemaphoreConfigs() {
        return semaphoreConfigs.values();
    }

    public Config setSemaphoreConfigs(Map<String, SemaphoreConfig> semaphoreConfigs) {
        ConfigLookupUtil.setConfig(this.semaphoreConfigs, semaphoreConfigs);
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
        return ConfigLookupUtil.findConfig(jobTrackerConfigs, name, JOB_TRACKER_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public JobTrackerConfig getJobTrackerConfig(String name) {
        return ConfigLookupUtil.getConfig(jobTrackerConfigs, name, JOB_TRACKER_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public Config addJobTrackerConfig(JobTrackerConfig jobTrackerConfig) {
        jobTrackerConfigs.put(jobTrackerConfig.getName(), jobTrackerConfig);
        return this;
    }

    public Map<String, JobTrackerConfig> getJobTrackerConfigs() {
        return jobTrackerConfigs;
    }

    public Config setJobTrackerConfigs(Map<String, JobTrackerConfig> jobTrackerConfigs) {
        ConfigLookupUtil.setConfig(this.jobTrackerConfigs, jobTrackerConfigs);
        return this;
    }

    public Map<String, QuorumConfig> getQuorumConfigs() {
        return quorumConfigs;
    }

    public QuorumConfig getQuorumConfig(String name) {
        return ConfigLookupUtil.getConfig(quorumConfigs, name, QUORUM_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public QuorumConfig findQuorumConfig(String name) {
        return ConfigLookupUtil.findConfig(quorumConfigs, name, QUORUM_CONFIG_CONSTRUCTOR, configPatternMatcher);
    }

    public Config setQuorumConfigs(Map<String, QuorumConfig> quorumConfigs) {
        ConfigLookupUtil.setConfig(this.quorumConfigs, quorumConfigs);
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
     * @return indicates if the node is a lite member or not. Lite members do not own any partition.
     */
    public boolean isLiteMember() {
        return liteMember;
    }

    /**
     * @param liteMember sets if the node will be a lite member or not. Lite members do not own any partition.
     */
    public Config setLiteMember(boolean liteMember) {
        this.liteMember = liteMember;
        return this;
    }

    /**
     * Get current configuration of User Code Deployment.
     *
     * @since 3.8
     */
    public UserCodeDeploymentConfig getUserCodeDeploymentConfig() {
        return userCodeDeploymentConfig;
    }

    /**
     * Set User Code Deployment configuration
     *
     * @param userCodeDeploymentConfig
     * @return User Code Deployment configuration
     * @since 3.8
     */
    public Config setUserCodeDeploymentConfig(UserCodeDeploymentConfig userCodeDeploymentConfig) {
        this.userCodeDeploymentConfig = userCodeDeploymentConfig;
        return this;
    }

    <T> T lookupByPattern(Map<String, T> configPatterns, String itemName) {
        return ConfigLookupUtil.lookupByPattern(configPatterns, itemName, configPatternMatcher);
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
                + ", partitionGroupConfig=" + partitionGroupConfig
                + ", managementCenterConfig=" + managementCenterConfig
                + ", securityConfig=" + securityConfig
                + ", liteMember=" + liteMember
                + '}';
    }
}
