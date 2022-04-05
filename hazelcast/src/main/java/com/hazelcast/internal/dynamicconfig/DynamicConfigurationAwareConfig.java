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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.AuditlogConfig;
import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.DeviceConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.DynamicConfigurationConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.InstanceTrackingConfig;
import com.hazelcast.config.IntegrityCheckerConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.MetricsConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.PersistenceConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SqlConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.config.CacheSimpleConfigReadOnly;
import com.hazelcast.internal.config.ExecutorConfigReadOnly;
import com.hazelcast.internal.config.FlakeIdGeneratorConfigReadOnly;
import com.hazelcast.internal.config.ListConfigReadOnly;
import com.hazelcast.internal.config.MapConfigReadOnly;
import com.hazelcast.internal.config.MultiMapConfigReadOnly;
import com.hazelcast.internal.config.QueueConfigReadOnly;
import com.hazelcast.internal.config.ReliableTopicConfigReadOnly;
import com.hazelcast.internal.config.ReplicatedMapConfigReadOnly;
import com.hazelcast.internal.config.RingbufferConfigReadOnly;
import com.hazelcast.internal.config.ServicesConfig;
import com.hazelcast.internal.config.SetConfigReadOnly;
import com.hazelcast.internal.config.TopicConfigReadOnly;
import com.hazelcast.internal.dynamicconfig.search.ConfigSearch;
import com.hazelcast.internal.dynamicconfig.search.ConfigSupplier;
import com.hazelcast.internal.dynamicconfig.search.Searcher;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.config.PersistenceAndHotRestartPersistenceMerger.merge;
import static com.hazelcast.internal.dynamicconfig.AggregatingMap.aggregate;
import static com.hazelcast.internal.dynamicconfig.search.ConfigSearch.supplierFor;
import static com.hazelcast.spi.properties.ClusterProperty.SEARCH_DYNAMIC_CONFIG_FIRST;

@SuppressWarnings({"unchecked",
        "checkstyle:methodcount",
        "checkstyle:classfanoutcomplexity",
        "checkstyle:classdataabstractioncoupling"})
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

    private final Config staticConfig;
    private final ConfigPatternMatcher configPatternMatcher;
    private final boolean isStaticFirst;
    private final DynamicCPSubsystemConfig dynamicCPSubsystemConfig;

    private volatile ConfigurationService configurationService = new EmptyConfigurationService();
    private volatile DynamicSecurityConfig dynamicSecurityConfig;
    private volatile Searcher configSearcher;

    public DynamicConfigurationAwareConfig(Config staticConfig, HazelcastProperties properties) {
        assert !(staticConfig instanceof DynamicConfigurationAwareConfig) : "A static Config object is required";
        merge(staticConfig.getHotRestartPersistenceConfig(), staticConfig.getPersistenceConfig());

        this.staticConfig = staticConfig;
        this.configPatternMatcher = staticConfig.getConfigPatternMatcher();
        this.isStaticFirst = !properties.getBoolean(SEARCH_DYNAMIC_CONFIG_FIRST);
        this.dynamicSecurityConfig = new DynamicSecurityConfig(staticConfig.getSecurityConfig(), null);
        this.dynamicCPSubsystemConfig = new DynamicCPSubsystemConfig(staticConfig.getCPSubsystemConfig());
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
    public Config setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        return staticConfig.setConfigPatternMatcher(configPatternMatcher);
    }

    @Override
    public String getProperty(String name) {
        return staticConfig.getProperty(name);
    }

    @Override
    public Config setProperty(@Nonnull String name, @Nonnull String value) {
        return staticConfig.setProperty(name, value);
    }

    @Override
    public MemberAttributeConfig getMemberAttributeConfig() {
        return staticConfig.getMemberAttributeConfig();
    }

    @Override
    public Config setMemberAttributeConfig(MemberAttributeConfig memberAttributeConfig) {
        return staticConfig.setMemberAttributeConfig(memberAttributeConfig);
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
    public String getClusterName() {
        return staticConfig.getClusterName();
    }

    @Override
    public Config setClusterName(String clusterName) {
        return staticConfig.setClusterName(clusterName);
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
    public AdvancedNetworkConfig getAdvancedNetworkConfig() {
        return staticConfig.getAdvancedNetworkConfig();
    }

    @Override
    public Config setAdvancedNetworkConfig(AdvancedNetworkConfig advancedNetworkConfig) {
        return staticConfig.setAdvancedNetworkConfig(advancedNetworkConfig);
    }

    @Override
    public MapConfig findMapConfig(String name) {
        return new MapConfigReadOnly(getMapConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getMapConfigs(),
                mapConfig.getName(), mapConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(mapConfig);
        }
        return this;
    }

    public static <T> boolean checkStaticConfigDoesNotExist(
            Map<String, T> staticConfigurations,
            String configName,
            T newConfig
    ) {
        T existingConfiguration = staticConfigurations.get(configName);
        return checkStaticConfigDoesNotExist(existingConfiguration, newConfig);
    }

    // be careful to not use this method with get*Config as it creates the config if not found
    public static <T> boolean checkStaticConfigDoesNotExist(T oldConfig, T newConfig) {
        if (oldConfig != null && !oldConfig.equals(newConfig)) {
            throw new InvalidConfigurationException("Cannot add a new dynamic configuration " + newConfig
                    + " as static configuration already contains " + oldConfig);
        }
        return oldConfig == null;
    }

    public Config getStaticConfig() {
        return staticConfig;
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
        return new CacheSimpleConfigReadOnly(getCacheConfigInternal(name, "default"));
    }

    @Override
    public CacheSimpleConfig findCacheConfigOrNull(String name) {
        //intentional: as of Hazelcast 3.x we do not use default for JCache!
        CacheSimpleConfig cacheConfig = getCacheConfigInternal(name, null);
        if (cacheConfig == null) {
            return null;
        }
        return new CacheSimpleConfigReadOnly(cacheConfig);
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getCacheConfigs(),
                cacheConfig.getName(), cacheConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(cacheConfig);
        }
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
        return new QueueConfigReadOnly(getQueueConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getQueueConfigs(),
                queueConfig.getName(), queueConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(queueConfig);
        }
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
    public ListConfig findListConfig(String name) {
        return new ListConfigReadOnly(getListConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getListConfigs(),
                listConfig.getName(), listConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(listConfig);
        }
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
        return new SetConfigReadOnly(getSetConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getSetConfigs(),
                setConfig.getName(), setConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(setConfig);
        }
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
        return new MultiMapConfigReadOnly(getMultiMapConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getMultiMapConfigs(),
                multiMapConfig.getName(), multiMapConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(multiMapConfig);
        }
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
        return new ReplicatedMapConfigReadOnly(getReplicatedMapConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getReplicatedMapConfigs(),
                replicatedMapConfig.getName(), replicatedMapConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(replicatedMapConfig);
        }
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
        return new RingbufferConfigReadOnly(getRingbufferConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getRingbufferConfigs(),
                ringbufferConfig.getName(), ringbufferConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(ringbufferConfig);
        }
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
    public TopicConfig findTopicConfig(String name) {
        return new TopicConfigReadOnly(getTopicConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getTopicConfigs(),
                topicConfig.getName(), topicConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(topicConfig);
        }
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
        return new ReliableTopicConfigReadOnly(getReliableTopicConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getReliableTopicConfigs(),
                reliableTopicConfig.getName(), reliableTopicConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(reliableTopicConfig);
        }
        return this;
    }

    @Override
    public Config setReliableTopicConfigs(Map<String, ReliableTopicConfig> reliableTopicConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        return new ExecutorConfigReadOnly(getExecutorConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getExecutorConfigs(),
                executorConfig.getName(), executorConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(executorConfig);
        }
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getDurableExecutorConfigs(),
                durableExecutorConfig.getName(), durableExecutorConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(durableExecutorConfig);
        }
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getScheduledExecutorConfigs(),
                scheduledExecutorConfig.getName(), scheduledExecutorConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(scheduledExecutorConfig);
        }
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getCardinalityEstimatorConfigs(),
                cardinalityEstimatorConfig.getName(), cardinalityEstimatorConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(cardinalityEstimatorConfig);
        }
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getPNCounterConfigs(),
                pnCounterConfig.getName(), pnCounterConfig);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(pnCounterConfig);
        }
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
    public Map<String, FlakeIdGeneratorConfig> getFlakeIdGeneratorConfigs() {
        Map<String, FlakeIdGeneratorConfig> staticMapConfigs = staticConfig.getFlakeIdGeneratorConfigs();
        Map<String, FlakeIdGeneratorConfig> dynamicMapConfigs = configurationService.getFlakeIdGeneratorConfigs();
        return aggregate(staticMapConfigs, dynamicMapConfigs);
    }

    @Override
    public FlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String name) {
        return new FlakeIdGeneratorConfigReadOnly(getFlakeIdGeneratorConfigInternal(name, "default"));
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
        boolean staticConfigDoesNotExist = checkStaticConfigDoesNotExist(staticConfig.getFlakeIdGeneratorConfigs(),
                config.getName(), config);
        if (staticConfigDoesNotExist) {
            configurationService.broadcastConfig(config);
        }
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
        throw new UnsupportedOperationException("Unsupported operation");
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
    public Map<String, SplitBrainProtectionConfig> getSplitBrainProtectionConfigs() {
        return staticConfig.getSplitBrainProtectionConfigs();
    }

    @Override
    public SplitBrainProtectionConfig getSplitBrainProtectionConfig(String name) {
        return staticConfig.getSplitBrainProtectionConfig(name);
    }

    @Override
    public SplitBrainProtectionConfig findSplitBrainProtectionConfig(String name) {
        return staticConfig.findSplitBrainProtectionConfig(name);
    }

    @Override
    public Config setSplitBrainProtectionConfigs(Map<String, SplitBrainProtectionConfig> splitBrainProtectionConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Config addSplitBrainProtectionConfig(SplitBrainProtectionConfig splitBrainProtectionConfig) {
        return staticConfig.addSplitBrainProtectionConfig(splitBrainProtectionConfig);
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
    protected ServicesConfig getServicesConfig() {
        return ConfigAccessor.getServicesConfig(staticConfig);
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
    public PersistenceConfig getPersistenceConfig() {
        return staticConfig.getPersistenceConfig();
    }

    @Override
    public Config setPersistenceConfig(PersistenceConfig prConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public DynamicConfigurationConfig getDynamicConfigurationConfig() {
        return staticConfig.getDynamicConfigurationConfig();
    }

    @Override
    public Config setDynamicConfigurationConfig(DynamicConfigurationConfig dynamicConfigurationConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public DeviceConfig getDeviceConfig(String name) {
        return staticConfig.getDeviceConfig(name);
    }

    @Override
    public <T extends DeviceConfig> T getDeviceConfig(Class<T> clazz, String name) {
        return staticConfig.getDeviceConfig(clazz, name);
    }

    @Override
    public Map<String, DeviceConfig> getDeviceConfigs() {
        return staticConfig.getDeviceConfigs();
    }

    @Override
    public Config setDeviceConfigs(Map<String, DeviceConfig> deviceConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Config addDeviceConfig(DeviceConfig deviceConfig) {
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

    public void setServices(NodeEngineImpl nodeEngine) {
        this.configurationService = nodeEngine.getConfigurationService();
        this.configSearcher = initConfigSearcher();
    }

    public void onSecurityServiceUpdated(SecurityService securityService) {
        this.dynamicSecurityConfig = new DynamicSecurityConfig(staticConfig.getSecurityConfig(), securityService);
    }

    private Searcher initConfigSearcher() {
        return ConfigSearch.searcherFor(staticConfig, configurationService, configPatternMatcher, isStaticFirst);
    }

    @Override
    public CPSubsystemConfig getCPSubsystemConfig() {
        return dynamicCPSubsystemConfig;
    }

    @Override
    public Config setCPSubsystemConfig(CPSubsystemConfig cpSubsystemConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    @Override
    public MetricsConfig getMetricsConfig() {
        return staticConfig.getMetricsConfig();
    }

    @Nonnull
    @Override
    public Config setMetricsConfig(@Nonnull MetricsConfig metricsConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    @Override
    public AuditlogConfig getAuditlogConfig() {
        return staticConfig.getAuditlogConfig();
    }

    @Nonnull
    @Override
    public Config setAuditlogConfig(@Nonnull AuditlogConfig auditlogConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    @Override
    public InstanceTrackingConfig getInstanceTrackingConfig() {
        return staticConfig.getInstanceTrackingConfig();
    }

    @Nonnull
    @Override
    public Config setInstanceTrackingConfig(@Nonnull InstanceTrackingConfig instanceTrackingConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    @Override
    public SqlConfig getSqlConfig() {
        return staticConfig.getSqlConfig();
    }

    @Nonnull
    @Override
    public Config setSqlConfig(@Nonnull SqlConfig sqlConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    @Override
    public JetConfig getJetConfig() {
        return staticConfig.getJetConfig();
    }

    @Nonnull
    @Override
    public Config setJetConfig(JetConfig jetConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Nonnull
    @Override
    public IntegrityCheckerConfig getIntegrityCheckerConfig() {
        return staticConfig.getIntegrityCheckerConfig();
    }

    @Nonnull
    @Override
    public Config setIntegrityCheckerConfig(final IntegrityCheckerConfig config) {
        throw new UnsupportedOperationException("Unsupported operation");
    }
}
