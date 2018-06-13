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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.CountDownLatchConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.dynamicconfig.search.ConfigSearch;
import com.hazelcast.internal.dynamicconfig.search.ConfigSupplier;
import com.hazelcast.internal.dynamicconfig.search.Searcher;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.StringUtil;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.dynamicconfig.AggregatingMap.aggregate;
import static com.hazelcast.internal.dynamicconfig.search.ConfigSearch.supplierFor;
import static com.hazelcast.spi.properties.GroupProperty.SEARCH_DYNAMIC_CONFIG_FIRST;

@SuppressWarnings({"unchecked", "checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class DynamicConfigurationAwareConfig extends Config {

    private final ConfigSupplier<MapConfig> mapConfigOrNullConfigSupplier = new ConfigSupplier<MapConfig>() {
        @Override
        public MapConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
            return configurationService.findMapConfig(name);
        }

        @Override
        public MapConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
            return staticConfig.getMapConfigOrNull(name);
        }

        @Override
        public Map<String, MapConfig> getStaticConfigs(@Nonnull Config staticConfig) {
            return staticConfig.getMapConfigs();
        }
    };
    private final ConfigSupplier<EventJournalConfig> mapEventJournalConfigSupplier =
            new ConfigSupplier<EventJournalConfig>() {
                @Override
                public EventJournalConfig getDynamicConfig(@Nonnull ConfigurationService configurationService,
                                                           @Nonnull String name) {
                    return configurationService.findMapEventJournalConfig(name);
                }

                @Override
                public EventJournalConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                    return staticConfig.getMapEventJournalConfig(name);
                }

                @Override
                public Map<String, EventJournalConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                    return staticConfig.getMapEventJournalConfigs();
                }
            };
    private final ConfigSupplier<EventJournalConfig> cacheEventJournalConfigSupplier = new
            ConfigSupplier<EventJournalConfig>() {
                @Override
                public EventJournalConfig getDynamicConfig(@Nonnull ConfigurationService configurationService,
                                                           @Nonnull String name) {
                    return configurationService.findCacheEventJournalConfig(name);
                }

                @Override
                public EventJournalConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                    return staticConfig.getCacheEventJournalConfig(name);
                }

                @Override
                public Map<String, EventJournalConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                    return staticConfig.getCacheEventJournalConfigs();
                }
            };

    private final Config staticConfig;
    private final ConfigPatternMatcher configPatternMatcher;
    private final boolean isStaticFirst;

    private volatile ConfigurationService configurationService = new EmptyConfigurationService();
    private volatile DynamicSecurityConfig dynamicSecurityConfig;
    private volatile Searcher configSearcher;

    public DynamicConfigurationAwareConfig(Config staticConfig, HazelcastProperties properties) {
        assert !(staticConfig instanceof DynamicConfigurationAwareConfig) : "A static Config object is required";
        this.staticConfig = staticConfig;
        this.configPatternMatcher = staticConfig.getConfigPatternMatcher();
        this.isStaticFirst = !properties.getBoolean(SEARCH_DYNAMIC_CONFIG_FIRST);
        this.dynamicSecurityConfig = new DynamicSecurityConfig(staticConfig.getSecurityConfig(), null);
        this.configSearcher = initConfigSearcher();
    }

    @Override
    public ClassLoader getClassLoader() {
        return staticConfig.getClassLoader();
    }

    @Override
    public Config setClassLoader(ClassLoader classLoader) {
        return staticConfig.setClassLoader(classLoader);
    }

    @Override
    public ConfigPatternMatcher getConfigPatternMatcher() {
        return staticConfig.getConfigPatternMatcher();
    }

    @Override
    public void setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        staticConfig.setConfigPatternMatcher(configPatternMatcher);
    }

    @Override
    public String getProperty(String name) {
        return staticConfig.getProperty(name);
    }

    @Override
    public Config setProperty(String name, String value) {
        return staticConfig.setProperty(name, value);
    }

    @Override
    public MemberAttributeConfig getMemberAttributeConfig() {
        return staticConfig.getMemberAttributeConfig();
    }

    @Override
    public void setMemberAttributeConfig(MemberAttributeConfig memberAttributeConfig) {
        staticConfig.setMemberAttributeConfig(memberAttributeConfig);
    }

    @Override
    public Properties getProperties() {
        return staticConfig.getProperties();
    }

    @Override
    public Config setProperties(Properties properties) {
        return staticConfig.setProperties(properties);
    }

    @Override
    public String getInstanceName() {
        return staticConfig.getInstanceName();
    }

    @Override
    public Config setInstanceName(String instanceName) {
        return staticConfig.setInstanceName(instanceName);
    }

    @Override
    public GroupConfig getGroupConfig() {
        return staticConfig.getGroupConfig();
    }

    @Override
    public Config setGroupConfig(GroupConfig groupConfig) {
        return staticConfig.setGroupConfig(groupConfig);
    }

    @Override
    public NetworkConfig getNetworkConfig() {
        return staticConfig.getNetworkConfig();
    }

    @Override
    public Config setNetworkConfig(NetworkConfig networkConfig) {
        return staticConfig.setNetworkConfig(networkConfig);
    }

    @Override
    public MapConfig findMapConfig(String name) {
        return getMapConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public MapConfig getMapConfig(String name) {
        return getMapConfigInternal(name, name);
    }

    @Override
    public MapConfig getMapConfigOrNull(String name) {
        return getMapConfigOrNullInternal(name);
    }

    private MapConfig getMapConfigOrNullInternal(String name) {
        return getMapConfigOrNullInternal(name, name);
    }

    private MapConfig getMapConfigOrNullInternal(String name, String fallbackName) {
        return (MapConfig) configSearcher.getConfig(name, fallbackName, mapConfigOrNullConfigSupplier);
    }

    private MapConfig getMapConfigInternal(String name, String fallbackName) {
        return (MapConfig) configSearcher.getConfig(name, fallbackName, supplierFor(MapConfig.class));
    }

    @Override
    public Config addMapConfig(MapConfig mapConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getMapConfigs(), mapConfig.getName(), mapConfig);
        configurationService.broadcastConfig(mapConfig);
        return this;
    }

    private <T> void checkStaticConfigurationDoesNotExist(Map<String, T> staticConfigurations, String configName, T newConfig) {
        Object existingConfiguration = staticConfigurations.get(configName);
        if (existingConfiguration != null) {
            throw new ConfigurationException("Cannot add a new dynamic configuration " + newConfig
                    + " as static configuration already contains " + existingConfiguration);
        }
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        Map<String, MapConfig> staticMapConfigs = staticConfig.getMapConfigs();
        Map<String, MapConfig> dynamicMapConfigs = configurationService.getMapConfigs();
        return aggregate(staticMapConfigs, dynamicMapConfigs);
    }

    @Override
    public Config setMapConfigs(Map<String, MapConfig> mapConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public CacheSimpleConfig findCacheConfig(String name) {
        return getCacheConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public CacheSimpleConfig findCacheConfigOrNull(String name) {
        //intentional: as of Hazelcast 3.x we do not use default for JCache!
        CacheSimpleConfig cacheConfig = getCacheConfigInternal(name, null);
        if (cacheConfig == null) {
            return null;
        }
        return cacheConfig.getAsReadOnly();
    }

    @Override
    public CacheSimpleConfig getCacheConfig(String name) {
        return getCacheConfigInternal(name, name);
    }

    private CacheSimpleConfig getCacheConfigInternal(String name, String fallbackName) {
        return (CacheSimpleConfig) configSearcher.getConfig(name, fallbackName, supplierFor(CacheSimpleConfig.class));
    }

    @Override
    public Config addCacheConfig(CacheSimpleConfig cacheConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getCacheConfigs(), cacheConfig.getName(), cacheConfig);
        configurationService.broadcastConfig(cacheConfig);
        return this;
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheConfigs() {
        Map<String, CacheSimpleConfig> staticConfigs = staticConfig.getCacheConfigs();
        Map<String, CacheSimpleConfig> dynamicConfigs = configurationService.getCacheSimpleConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setCacheConfigs(Map<String, CacheSimpleConfig> cacheConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public QueueConfig findQueueConfig(String name) {
        return getQueueConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public QueueConfig getQueueConfig(String name) {
        return getQueueConfigInternal(name, name);
    }

    private QueueConfig getQueueConfigInternal(String name, String fallbackName) {
        return (QueueConfig) configSearcher.getConfig(name, fallbackName, supplierFor(QueueConfig.class));
    }

    @Override
    public Config addQueueConfig(QueueConfig queueConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getQueueConfigs(), queueConfig.getName(), queueConfig);
        configurationService.broadcastConfig(queueConfig);
        return this;
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        Map<String, QueueConfig> staticQueueConfigs = staticConfig.getQueueConfigs();
        Map<String, QueueConfig> dynamicQueueConfigs = configurationService.getQueueConfigs();
        return aggregate(staticQueueConfigs, dynamicQueueConfigs);
    }

    @Override
    public Config setQueueConfigs(Map<String, QueueConfig> queueConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public LockConfig findLockConfig(String name) {
        return getLockConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public LockConfig getLockConfig(String name) {
        return getLockConfigInternal(name, name);
    }

    private LockConfig getLockConfigInternal(String name, String fallbackName) {
        return (LockConfig) configSearcher.getConfig(name, fallbackName, supplierFor(LockConfig.class));
    }

    @Override
    public Config addLockConfig(LockConfig lockConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getLockConfigs(), lockConfig.getName(), lockConfig);
        configurationService.broadcastConfig(lockConfig);
        return this;
    }

    @Override
    public Map<String, LockConfig> getLockConfigs() {
        Map<String, LockConfig> staticLockConfigs = staticConfig.getLockConfigs();
        Map<String, LockConfig> dynamiclockConfigs = configurationService.getLockConfigs();
        return aggregate(staticLockConfigs, dynamiclockConfigs);
    }

    @Override
    public Config setLockConfigs(Map<String, LockConfig> lockConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ListConfig findListConfig(String name) {
        return getListConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public ListConfig getListConfig(String name) {
        return getListConfigInternal(name, name);
    }

    private ListConfig getListConfigInternal(String name, String fallbackName) {
        return (ListConfig) configSearcher.getConfig(name, fallbackName, supplierFor(ListConfig.class));
    }

    @Override
    public Config addListConfig(ListConfig listConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getListConfigs(), listConfig.getName(), listConfig);
        configurationService.broadcastConfig(listConfig);
        return this;
    }

    @Override
    public Map<String, ListConfig> getListConfigs() {
        Map<String, ListConfig> staticListConfigs = staticConfig.getListConfigs();
        Map<String, ListConfig> dynamicListConfigs = configurationService.getListConfigs();

        return aggregate(staticListConfigs, dynamicListConfigs);
    }

    @Override
    public Config setListConfigs(Map<String, ListConfig> listConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SetConfig findSetConfig(String name) {
        return getSetConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public SetConfig getSetConfig(String name) {
        return getSetConfigInternal(name, name);
    }

    private SetConfig getSetConfigInternal(String name, String fallbackName) {
        return (SetConfig) configSearcher.getConfig(name, fallbackName, supplierFor(SetConfig.class));
    }

    @Override
    public Config addSetConfig(SetConfig setConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getSetConfigs(), setConfig.getName(), setConfig);
        configurationService.broadcastConfig(setConfig);
        return this;
    }

    @Override
    public Map<String, SetConfig> getSetConfigs() {
        Map<String, SetConfig> staticSetConfigs = staticConfig.getSetConfigs();
        Map<String, SetConfig> dynamicSetConfigs = configurationService.getSetConfigs();
        return aggregate(staticSetConfigs, dynamicSetConfigs);
    }

    @Override
    public Config setSetConfigs(Map<String, SetConfig> setConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public MultiMapConfig findMultiMapConfig(String name) {
        return getMultiMapConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public MultiMapConfig getMultiMapConfig(String name) {
        return getMultiMapConfigInternal(name, name);
    }

    private MultiMapConfig getMultiMapConfigInternal(String name, String fallbackName) {
        return (MultiMapConfig) configSearcher.getConfig(name, fallbackName, supplierFor(MultiMapConfig.class));
    }

    @Override
    public Config addMultiMapConfig(MultiMapConfig multiMapConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getMultiMapConfigs(), multiMapConfig.getName(), multiMapConfig);
        configurationService.broadcastConfig(multiMapConfig);
        return this;
    }

    @Override
    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        Map<String, MultiMapConfig> staticConfigs = staticConfig.getMultiMapConfigs();
        Map<String, MultiMapConfig> dynamicConfigs = configurationService.getMultiMapConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setMultiMapConfigs(Map<String, MultiMapConfig> multiMapConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        return getReplicatedMapConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return getReplicatedMapConfigInternal(name, name);
    }

    private ReplicatedMapConfig getReplicatedMapConfigInternal(String name, String fallbackName) {
        return (ReplicatedMapConfig) configSearcher.getConfig(name, fallbackName, supplierFor(ReplicatedMapConfig.class));
    }

    @Override
    public Config addReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getReplicatedMapConfigs(), replicatedMapConfig.getName(),
                replicatedMapConfig);
        configurationService.broadcastConfig(replicatedMapConfig);
        return this;
    }

    @Override
    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        Map<String, ReplicatedMapConfig> staticConfigs = staticConfig.getReplicatedMapConfigs();
        Map<String, ReplicatedMapConfig> dynamicConfigs = configurationService.getReplicatedMapConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setReplicatedMapConfigs(Map<String, ReplicatedMapConfig> replicatedMapConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public RingbufferConfig findRingbufferConfig(String name) {
        return getRingbufferConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public RingbufferConfig getRingbufferConfig(String name) {
        return getRingbufferConfigInternal(name, name);
    }

    private RingbufferConfig getRingbufferConfigInternal(String name, String fallbackName) {
        return (RingbufferConfig) configSearcher.getConfig(name, fallbackName, supplierFor(RingbufferConfig.class));
    }

    @Override
    public Config addRingBufferConfig(RingbufferConfig ringbufferConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getRingbufferConfigs(), ringbufferConfig.getName(),
                ringbufferConfig);
        configurationService.broadcastConfig(ringbufferConfig);
        return this;
    }

    @Override
    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        Map<String, RingbufferConfig> staticConfigs = staticConfig.getRingbufferConfigs();
        Map<String, RingbufferConfig> dynamicConfigs = configurationService.getRingbufferConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setRingbufferConfigs(Map<String, RingbufferConfig> ringbufferConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public AtomicLongConfig findAtomicLongConfig(String name) {
        return getAtomicLongConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public AtomicLongConfig getAtomicLongConfig(String name) {
        return getAtomicLongConfigInternal(name, name);
    }

    @Override
    public Config addAtomicLongConfig(AtomicLongConfig atomicLongConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getAtomicLongConfigs(), atomicLongConfig.getName(), atomicLongConfig);
        configurationService.broadcastConfig(atomicLongConfig);
        return this;
    }

    @Override
    public Map<String, AtomicLongConfig> getAtomicLongConfigs() {
        Map<String, AtomicLongConfig> staticConfigs = staticConfig.getAtomicLongConfigs();
        Map<String, AtomicLongConfig> dynamicConfigs = configurationService.getAtomicLongConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setAtomicLongConfigs(Map<String, AtomicLongConfig> atomicLongConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    private AtomicLongConfig getAtomicLongConfigInternal(String name, String fallbackName) {
        return (AtomicLongConfig) configSearcher.getConfig(name, fallbackName, supplierFor(AtomicLongConfig.class));
    }

    @Override
    public AtomicReferenceConfig findAtomicReferenceConfig(String name) {
        return getAtomicReferenceConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public AtomicReferenceConfig getAtomicReferenceConfig(String name) {
        return getAtomicReferenceConfigInternal(name, name);
    }

    @Override
    public Config addAtomicReferenceConfig(AtomicReferenceConfig atomicReferenceConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getAtomicReferenceConfigs(), atomicReferenceConfig.getName(),
                atomicReferenceConfig);
        configurationService.broadcastConfig(atomicReferenceConfig);
        return this;
    }

    @Override
    public Map<String, AtomicReferenceConfig> getAtomicReferenceConfigs() {
        Map<String, AtomicReferenceConfig> staticConfigs = staticConfig.getAtomicReferenceConfigs();
        Map<String, AtomicReferenceConfig> dynamicConfigs = configurationService.getAtomicReferenceConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setAtomicReferenceConfigs(Map<String, AtomicReferenceConfig> atomicReferenceConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    private AtomicReferenceConfig getAtomicReferenceConfigInternal(String name, String fallbackName) {
        return (AtomicReferenceConfig) configSearcher.getConfig(name, fallbackName, supplierFor(AtomicReferenceConfig.class));
    }

    @Override
    public CountDownLatchConfig findCountDownLatchConfig(String name) {
        return getCountDownLatchConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public CountDownLatchConfig getCountDownLatchConfig(String name) {
        return getCountDownLatchConfigInternal(name, name);
    }

    @Override
    public Config addCountDownLatchConfig(CountDownLatchConfig countDownLatchConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getCountDownLatchConfigs(), countDownLatchConfig.getName(),
                countDownLatchConfig);
        configurationService.broadcastConfig(countDownLatchConfig);
        return this;
    }

    @Override
    public Map<String, CountDownLatchConfig> getCountDownLatchConfigs() {
        Map<String, CountDownLatchConfig> staticConfigs = staticConfig.getCountDownLatchConfigs();
        Map<String, CountDownLatchConfig> dynamicConfigs = configurationService.getCountDownLatchConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setCountDownLatchConfigs(Map<String, CountDownLatchConfig> countDownLatchConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    private CountDownLatchConfig getCountDownLatchConfigInternal(String name, String fallbackName) {
        return (CountDownLatchConfig) configSearcher.getConfig(name, fallbackName, supplierFor(CountDownLatchConfig.class));
    }

    @Override
    public TopicConfig findTopicConfig(String name) {
        return getTopicConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public TopicConfig getTopicConfig(String name) {
        return getTopicConfigInternal(name, name);
    }

    private TopicConfig getTopicConfigInternal(String name, String fallbackName) {
        return (TopicConfig) configSearcher.getConfig(name, fallbackName, supplierFor(TopicConfig.class));
    }

    @Override
    public Config addTopicConfig(TopicConfig topicConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getTopicConfigs(), topicConfig.getName(),
                topicConfig);
        configurationService.broadcastConfig(topicConfig);
        return this;
    }

    @Override
    public Map<String, TopicConfig> getTopicConfigs() {
        Map<String, TopicConfig> staticConfigs = staticConfig.getTopicConfigs();
        Map<String, TopicConfig> dynamicConfigs = configurationService.getTopicConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setTopicConfigs(Map<String, TopicConfig> mapTopicConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        return getReliableTopicConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public ReliableTopicConfig getReliableTopicConfig(String name) {
        return getReliableTopicConfigInternal(name, name);
    }

    private ReliableTopicConfig getReliableTopicConfigInternal(String name, String fallbackName) {
        return (ReliableTopicConfig) configSearcher.getConfig(name, fallbackName, supplierFor(ReliableTopicConfig.class));
    }

    @Override
    public Map<String, ReliableTopicConfig> getReliableTopicConfigs() {
        Map<String, ReliableTopicConfig> staticConfigs = staticConfig.getReliableTopicConfigs();
        Map<String, ReliableTopicConfig> dynamicConfigs = configurationService.getReliableTopicConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config addReliableTopicConfig(ReliableTopicConfig reliableTopicConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getReliableTopicConfigs(), reliableTopicConfig.getName(),
                reliableTopicConfig);
        configurationService.broadcastConfig(reliableTopicConfig);
        return this;
    }

    @Override
    public Config setReliableTopicConfigs(Map<String, ReliableTopicConfig> reliableTopicConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        return getExecutorConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public ExecutorConfig getExecutorConfig(String name) {
        return getExecutorConfigInternal(name, name);
    }

    private ExecutorConfig getExecutorConfigInternal(String name, String fallbackName) {
        return (ExecutorConfig) configSearcher.getConfig(name, fallbackName, supplierFor(ExecutorConfig.class));
    }

    @Override
    public Config addExecutorConfig(ExecutorConfig executorConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getExecutorConfigs(), executorConfig.getName(),
                executorConfig);
        configurationService.broadcastConfig(executorConfig);
        return this;
    }

    @Override
    public Map<String, ExecutorConfig> getExecutorConfigs() {
        Map<String, ExecutorConfig> staticConfigs = staticConfig.getExecutorConfigs();
        Map<String, ExecutorConfig> dynamicConfigs = configurationService.getExecutorConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setExecutorConfigs(Map<String, ExecutorConfig> executorConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        return getDurableExecutorConfigInternal(name, "default");
    }

    @Override
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        return getDurableExecutorConfigInternal(name, name);
    }

    private DurableExecutorConfig getDurableExecutorConfigInternal(String name, String fallbackName) {
        return (DurableExecutorConfig) configSearcher.getConfig(name, fallbackName, supplierFor(DurableExecutorConfig.class));
    }

    @Override
    public Config addDurableExecutorConfig(DurableExecutorConfig durableExecutorConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getDurableExecutorConfigs(), durableExecutorConfig.getName(),
                durableExecutorConfig);
        configurationService.broadcastConfig(durableExecutorConfig);
        return this;
    }

    @Override
    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        Map<String, DurableExecutorConfig> staticConfigs = staticConfig.getDurableExecutorConfigs();
        Map<String, DurableExecutorConfig> dynamicConfigs = configurationService.getDurableExecutorConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setDurableExecutorConfigs(Map<String, DurableExecutorConfig> durableExecutorConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }


    @Override
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        return getScheduledExecutorConfigInternal(name, "default");
    }

    @Override
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        return getScheduledExecutorConfigInternal(name, name);
    }

    private ScheduledExecutorConfig getScheduledExecutorConfigInternal(String name, String fallbackName) {
        return (ScheduledExecutorConfig) configSearcher.getConfig(name, fallbackName, supplierFor(ScheduledExecutorConfig.class));
    }

    @Override
    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        Map<String, ScheduledExecutorConfig> staticConfigs = staticConfig.getScheduledExecutorConfigs();
        Map<String, ScheduledExecutorConfig> dynamicConfigs = configurationService.getScheduledExecutorConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config addScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getScheduledExecutorConfigs(), scheduledExecutorConfig.getName(),
                scheduledExecutorConfig);
        configurationService.broadcastConfig(scheduledExecutorConfig);
        return this;
    }

    @Override
    public Config setScheduledExecutorConfigs(Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        return getCardinalityEstimatorConfigInternal(name, "default");
    }

    @Override
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        return getCardinalityEstimatorConfigInternal(name, name);
    }

    private CardinalityEstimatorConfig getCardinalityEstimatorConfigInternal(String name, String fallbackName) {
        return (CardinalityEstimatorConfig) configSearcher.getConfig(name, fallbackName,
                supplierFor(CardinalityEstimatorConfig.class));
    }


    @Override
    public Config addCardinalityEstimatorConfig(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getCardinalityEstimatorConfigs(),
                cardinalityEstimatorConfig.getName(), cardinalityEstimatorConfig);
        configurationService.broadcastConfig(cardinalityEstimatorConfig);
        return this;
    }

    @Override
    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        Map<String, CardinalityEstimatorConfig> staticConfigs = staticConfig.getCardinalityEstimatorConfigs();
        Map<String, CardinalityEstimatorConfig> dynamicConfigs = configurationService.getCardinalityEstimatorConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setCardinalityEstimatorConfigs(Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public PNCounterConfig findPNCounterConfig(String name) {
        return getPNCounterConfigInternal(name, "default");
    }

    @Override
    public PNCounterConfig getPNCounterConfig(String name) {
        return getPNCounterConfigInternal(name, name);
    }

    private PNCounterConfig getPNCounterConfigInternal(String name, String fallbackName) {
        return (PNCounterConfig) configSearcher.getConfig(name, fallbackName, supplierFor(PNCounterConfig.class));
    }

    @Override
    public Config addPNCounterConfig(PNCounterConfig pnCounterConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getPNCounterConfigs(), pnCounterConfig.getName(), pnCounterConfig);
        configurationService.broadcastConfig(pnCounterConfig);
        return this;
    }

    @Override
    public Map<String, PNCounterConfig> getPNCounterConfigs() {
        Map<String, PNCounterConfig> staticConfigs = staticConfig.getPNCounterConfigs();
        Map<String, PNCounterConfig> dynamicConfigs = configurationService.getPNCounterConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setPNCounterConfigs(Map<String, PNCounterConfig> pnCounterConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SemaphoreConfig findSemaphoreConfig(String name) {
        return getSemaphoreConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public SemaphoreConfig getSemaphoreConfig(String name) {
        return getSemaphoreConfigInternal(name, name);
    }

    private SemaphoreConfig getSemaphoreConfigInternal(String name, String fallbackName) {
        return (SemaphoreConfig) configSearcher.getConfig(name, fallbackName, supplierFor(SemaphoreConfig.class));
    }

    @Override
    public Config addSemaphoreConfig(SemaphoreConfig semaphoreConfig) {
        checkStaticConfigurationDoesNotExist(staticConfig.getSemaphoreConfigsAsMap(),
                semaphoreConfig.getName(), semaphoreConfig);
        configurationService.broadcastConfig(semaphoreConfig);
        return this;
    }

    @Override
    public Collection<SemaphoreConfig> getSemaphoreConfigs() {
        Collection<SemaphoreConfig> staticConfigs = staticConfig.getSemaphoreConfigs();
        Map<String, SemaphoreConfig> semaphoreConfigs = configurationService.getSemaphoreConfigs();

        ArrayList<SemaphoreConfig> aggregated = new ArrayList<SemaphoreConfig>(staticConfigs);
        aggregated.addAll(semaphoreConfigs.values());

        return aggregated;
    }

    @Override
    public Map<String, SemaphoreConfig> getSemaphoreConfigsAsMap() {
        Map<String, SemaphoreConfig> staticConfigs = staticConfig.getSemaphoreConfigsAsMap();
        Map<String, SemaphoreConfig> dynamicConfigs = configurationService.getSemaphoreConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setSemaphoreConfigs(Map<String, SemaphoreConfig> semaphoreConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public EventJournalConfig findCacheEventJournalConfig(String name) {
        return getCacheEventJournalConfigInternal(name, "default");
    }

    @Override
    public EventJournalConfig getCacheEventJournalConfig(String name) {
        return getCacheEventJournalConfigInternal(name, name);
    }

    private EventJournalConfig getCacheEventJournalConfigInternal(String name, String fallbackName) {
        return (EventJournalConfig) configSearcher.getConfig(name, fallbackName, cacheEventJournalConfigSupplier);
    }

    @Override
    public Map<String, EventJournalConfig> getCacheEventJournalConfigs() {
        Map<String, EventJournalConfig> staticConfigs = staticConfig.getCacheEventJournalConfigs();
        Map<String, EventJournalConfig> dynamicConfigs = configurationService.getCacheEventJournalConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public EventJournalConfig findMapEventJournalConfig(String name) {
        return getMapEventJournalConfigInternal(name, "default");
    }

    @Override
    public EventJournalConfig getMapEventJournalConfig(String name) {
        return getMapEventJournalConfigInternal(name, name);
    }

    private EventJournalConfig getMapEventJournalConfigInternal(String name, String fallbackName) {
        return (EventJournalConfig) configSearcher.getConfig(name, fallbackName, mapEventJournalConfigSupplier);
    }

    @Override
    public Map<String, EventJournalConfig> getMapEventJournalConfigs() {
        Map<String, EventJournalConfig> staticConfigs = staticConfig.getMapEventJournalConfigs();
        Map<String, EventJournalConfig> dynamicConfigs = configurationService.getMapEventJournalConfigs();
        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config addEventJournalConfig(EventJournalConfig eventJournalConfig) {
        final String mapName = eventJournalConfig.getMapName();
        final String cacheName = eventJournalConfig.getCacheName();
        if (StringUtil.isNullOrEmpty(mapName) && StringUtil.isNullOrEmpty(cacheName)) {
            throw new IllegalArgumentException("Event journal config should have non-empty map name and/or cache name");
        }

        if (!StringUtil.isNullOrEmpty(mapName)) {
            Map<String, EventJournalConfig> staticConfigs = staticConfig.getMapEventJournalConfigs();
            checkStaticConfigurationDoesNotExist(staticConfigs, mapName, eventJournalConfig);
        }
        if (!StringUtil.isNullOrEmpty(cacheName)) {
            Map<String, EventJournalConfig> staticConfigs = staticConfig.getCacheEventJournalConfigs();
            checkStaticConfigurationDoesNotExist(staticConfigs, cacheName, eventJournalConfig);
        }
        configurationService.broadcastConfig(eventJournalConfig);
        return this;
    }

    @Override
    public Config setMapEventJournalConfigs(Map<String, EventJournalConfig> eventJournalConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Config setCacheEventJournalConfigs(Map<String, EventJournalConfig> eventJournalConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Map<String, FlakeIdGeneratorConfig> getFlakeIdGeneratorConfigs() {
        Map<String, FlakeIdGeneratorConfig> staticMapConfigs = staticConfig.getFlakeIdGeneratorConfigs();
        Map<String, FlakeIdGeneratorConfig> dynamicMapConfigs = configurationService.getFlakeIdGeneratorConfigs();
        return aggregate(staticMapConfigs, dynamicMapConfigs);
    }

    @Override
    public FlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String name) {
        return getFlakeIdGeneratorConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public FlakeIdGeneratorConfig getFlakeIdGeneratorConfig(String name) {
        return getFlakeIdGeneratorConfigInternal(name, name);
    }

    private FlakeIdGeneratorConfig getFlakeIdGeneratorConfigInternal(String name, String fallbackName) {
        return (FlakeIdGeneratorConfig) configSearcher.getConfig(name, fallbackName, supplierFor(FlakeIdGeneratorConfig.class));
    }

    @Override
    public Config addFlakeIdGeneratorConfig(FlakeIdGeneratorConfig config) {
        checkStaticConfigurationDoesNotExist(staticConfig.getFlakeIdGeneratorConfigs(), config.getName(), config);
        configurationService.broadcastConfig(config);
        return this;
    }

    @Override
    public Config setFlakeIdGeneratorConfigs(Map<String, FlakeIdGeneratorConfig> map) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public WanReplicationConfig getWanReplicationConfig(String name) {
        return staticConfig.getWanReplicationConfig(name);
    }

    @Override
    public Config addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        return staticConfig.addWanReplicationConfig(wanReplicationConfig);
    }

    @Override
    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        return staticConfig.getWanReplicationConfigs();
    }

    @Override
    public Config setWanReplicationConfigs(Map<String, WanReplicationConfig> wanReplicationConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public JobTrackerConfig findJobTrackerConfig(String name) {
        return staticConfig.findJobTrackerConfig(name);
    }

    @Override
    public JobTrackerConfig getJobTrackerConfig(String name) {
        return staticConfig.getJobTrackerConfig(name);
    }

    @Override
    public Config addJobTrackerConfig(JobTrackerConfig jobTrackerConfig) {
        return staticConfig.addJobTrackerConfig(jobTrackerConfig);
    }

    @Override
    public Map<String, JobTrackerConfig> getJobTrackerConfigs() {
        return staticConfig.getJobTrackerConfigs();
    }

    @Override
    public Config setJobTrackerConfigs(Map<String, JobTrackerConfig> jobTrackerConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Map<String, QuorumConfig> getQuorumConfigs() {
        return staticConfig.getQuorumConfigs();
    }

    @Override
    public QuorumConfig getQuorumConfig(String name) {
        return staticConfig.getQuorumConfig(name);
    }

    @Override
    public QuorumConfig findQuorumConfig(String name) {
        return staticConfig.findQuorumConfig(name);
    }

    @Override
    public Config setQuorumConfigs(Map<String, QuorumConfig> quorumConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Config addQuorumConfig(QuorumConfig quorumConfig) {
        return staticConfig.addQuorumConfig(quorumConfig);
    }

    @Override
    public ManagementCenterConfig getManagementCenterConfig() {
        return staticConfig.getManagementCenterConfig();
    }

    @Override
    public Config setManagementCenterConfig(ManagementCenterConfig managementCenterConfig) {
        return staticConfig.setManagementCenterConfig(managementCenterConfig);
    }

    @Override
    public ServicesConfig getServicesConfig() {
        return staticConfig.getServicesConfig();
    }

    @Override
    public Config setServicesConfig(ServicesConfig servicesConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SecurityConfig getSecurityConfig() {
        return dynamicSecurityConfig;
    }

    @Override
    public Config setSecurityConfig(SecurityConfig securityConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Config addListenerConfig(ListenerConfig listenerConfig) {
        return staticConfig.addListenerConfig(listenerConfig);
    }

    @Override
    public List<ListenerConfig> getListenerConfigs() {
        return staticConfig.getListenerConfigs();
    }

    @Override
    public Config setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SerializationConfig getSerializationConfig() {
        return staticConfig.getSerializationConfig();
    }

    @Override
    public Config setSerializationConfig(SerializationConfig serializationConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public PartitionGroupConfig getPartitionGroupConfig() {
        return staticConfig.getPartitionGroupConfig();
    }

    @Override
    public Config setPartitionGroupConfig(PartitionGroupConfig partitionGroupConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public HotRestartPersistenceConfig getHotRestartPersistenceConfig() {
        return staticConfig.getHotRestartPersistenceConfig();
    }

    @Override
    public Config setHotRestartPersistenceConfig(HotRestartPersistenceConfig hrConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public CRDTReplicationConfig getCRDTReplicationConfig() {
        return staticConfig.getCRDTReplicationConfig();
    }

    @Override
    public Config setCRDTReplicationConfig(CRDTReplicationConfig crdtReplicationConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ManagedContext getManagedContext() {
        return staticConfig.getManagedContext();
    }

    @Override
    public Config setManagedContext(ManagedContext managedContext) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return staticConfig.getUserContext();
    }

    @Override
    public Config setUserContext(ConcurrentMap<String, Object> userContext) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public NativeMemoryConfig getNativeMemoryConfig() {
        return staticConfig.getNativeMemoryConfig();
    }

    @Override
    public Config setNativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public URL getConfigurationUrl() {
        return staticConfig.getConfigurationUrl();
    }

    @Override
    public Config setConfigurationUrl(URL configurationUrl) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public File getConfigurationFile() {
        return staticConfig.getConfigurationFile();
    }

    @Override
    public Config setConfigurationFile(File configurationFile) {
        return staticConfig.setConfigurationFile(configurationFile);
    }

    @Override
    public String getLicenseKey() {
        return staticConfig.getLicenseKey();
    }

    @Override
    public Config setLicenseKey(String licenseKey) {
        return staticConfig.setLicenseKey(licenseKey);
    }

    @Override
    public boolean isLiteMember() {
        return staticConfig.isLiteMember();
    }

    @Override
    public Config setLiteMember(boolean liteMember) {
        return staticConfig.setLiteMember(liteMember);
    }

    @Override
    public UserCodeDeploymentConfig getUserCodeDeploymentConfig() {
        return staticConfig.getUserCodeDeploymentConfig();
    }

    @Override
    public Config setUserCodeDeploymentConfig(UserCodeDeploymentConfig userCodeDeploymentConfig) {
        return staticConfig.setUserCodeDeploymentConfig(userCodeDeploymentConfig);
    }

    @Override
    public String toString() {
        return staticConfig.toString();
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
        this.configSearcher = initConfigSearcher();
    }

    public void onSecurityServiceUpdated(SecurityService securityService) {
        this.dynamicSecurityConfig = new DynamicSecurityConfig(staticConfig.getSecurityConfig(), securityService);
    }

    private Searcher initConfigSearcher() {
        return ConfigSearch.searcherFor(staticConfig, configurationService, configPatternMatcher, isStaticFirst);
    }
}
