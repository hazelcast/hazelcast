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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.ClusterVersionListener;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.version.Version;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.cluster.Versions.V3_8;
import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static com.hazelcast.util.InvocationUtil.invokeOnStableClusterSerial;
import static java.lang.Boolean.getBoolean;

@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodcount"})
public class ClusterWideConfigurationService implements MigrationAwareService,
        CoreService, ClusterVersionListener, ManagedService, ConfigurationService, SplitBrainHandlerService {
    public static final String SERVICE_NAME = "configuration-service";
    public static final int CONFIG_PUBLISH_MAX_ATTEMPT_COUNT = 100;

    //this is meant to be used as a workaround for buggy equals/hashcode implementations
    private static final boolean IGNORE_CONFLICTING_CONFIGS_WORKAROUND =
            getBoolean("hazelcast.dynamicconfig.ignore.conflicts");

    private final DynamicConfigListener listener;
    private Object journalMutex = new Object();

    private NodeEngine nodeEngine;
    private final ConcurrentMap<String, MapConfig> mapConfigs = new ConcurrentHashMap<String, MapConfig>();
    private final ConcurrentMap<String, MultiMapConfig> multiMapConfigs = new ConcurrentHashMap<String, MultiMapConfig>();
    private final ConcurrentMap<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs =
            new ConcurrentHashMap<String, CardinalityEstimatorConfig>();
    private final ConcurrentMap<String, RingbufferConfig> ringbufferConfigs = new ConcurrentHashMap<String, RingbufferConfig>();
    private final ConcurrentMap<String, LockConfig> lockConfigs = new ConcurrentHashMap<String, LockConfig>();
    private final ConcurrentMap<String, ListConfig> listConfigs = new ConcurrentHashMap<String, ListConfig>();
    private final ConcurrentMap<String, SetConfig> setConfigs = new ConcurrentHashMap<String, SetConfig>();
    private final ConcurrentMap<String, ReplicatedMapConfig> replicatedMapConfigs =
            new ConcurrentHashMap<String, ReplicatedMapConfig>();
    private final ConcurrentMap<String, TopicConfig> topicConfigs = new ConcurrentHashMap<String, TopicConfig>();
    private final ConcurrentMap<String, ExecutorConfig> executorConfigs = new ConcurrentHashMap<String, ExecutorConfig>();
    private final ConcurrentMap<String, DurableExecutorConfig> durableExecutorConfigs =
            new ConcurrentHashMap<String, DurableExecutorConfig>();
    private final ConcurrentMap<String, ScheduledExecutorConfig> scheduledExecutorConfigs =
            new ConcurrentHashMap<String, ScheduledExecutorConfig>();
    private final ConcurrentMap<String, SemaphoreConfig> semaphoreConfigs = new ConcurrentHashMap<String, SemaphoreConfig>();
    private final ConcurrentMap<String, QueueConfig> queueConfigs = new ConcurrentHashMap<String, QueueConfig>();
    private final ConcurrentMap<String, ReliableTopicConfig> reliableTopicConfigs =
            new ConcurrentHashMap<String, ReliableTopicConfig>();
    private final ConcurrentMap<String, CacheSimpleConfig> cacheSimpleConfigs =
            new ConcurrentHashMap<String, CacheSimpleConfig>();
    private final ConcurrentMap<String, EventJournalConfig> cacheEventJournalConfigs =
            new ConcurrentHashMap<String, EventJournalConfig>();
    private final ConcurrentMap<String, EventJournalConfig> mapEventJournalConfigs =
            new ConcurrentHashMap<String, EventJournalConfig>();

    private final ConfigPatternMatcher configPatternMatcher;

    @SuppressWarnings("unchecked")
    private final Map<?, ? extends IdentifiedDataSerializable>[] allConfigurations = new Map[]{
            mapConfigs,
            multiMapConfigs,
            cardinalityEstimatorConfigs,
            ringbufferConfigs,
            lockConfigs,
            listConfigs,
            setConfigs,
            replicatedMapConfigs,
            topicConfigs,
            executorConfigs,
            durableExecutorConfigs,
            scheduledExecutorConfigs,
            semaphoreConfigs,
            queueConfigs,
            reliableTopicConfigs,
            cacheSimpleConfigs,
            cacheEventJournalConfigs,
            mapEventJournalConfigs,
    };

    private volatile Version version;

    public ClusterWideConfigurationService(NodeEngine nodeEngine, DynamicConfigListener dynamicConfigListener) {
        this.nodeEngine = nodeEngine;
        this.listener = dynamicConfigListener;
        this.configPatternMatcher = nodeEngine.getConfig().getConfigPatternMatcher();
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (version.isLessOrEqual(V3_8)) {
            return null;
        }
        IdentifiedDataSerializable[] allConfigurations = getAllConfigurations();
        if (allConfigurations.length == 0) {
            return null;
        }
        return new DynamicConfigReplicationOperation(allConfigurations, true);
    }

    private IdentifiedDataSerializable[] getAllConfigurations() {
        List<IdentifiedDataSerializable> all = new ArrayList<IdentifiedDataSerializable>();
        for (Map<?, ? extends IdentifiedDataSerializable> entry : allConfigurations) {
            Collection<? extends IdentifiedDataSerializable> values = entry.values();
            all.addAll(values);
        }
        return all.toArray(new IdentifiedDataSerializable[0]);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        version = newVersion;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        listener.onServiceInitialized(this);
    }

    @Override
    public void reset() {
        for (Map<?, ?> entry : allConfigurations) {
            entry.clear();
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        //no-op
    }

    @Override
    public void broadcastConfig(IdentifiedDataSerializable config) {
        // we create an defensive copy as local operation execution might use a fast-path
        // and avoid config serialization altogether.
        // we certainly do not want the dynamic config service to reference object a user can mutate
        IdentifiedDataSerializable clonedConfig = cloneConfig(config);
        invokeOnStableClusterSerial(nodeEngine, new AddDynamicConfigOperationFactory(clonedConfig),
                CONFIG_PUBLISH_MAX_ATTEMPT_COUNT);
    }

    private IdentifiedDataSerializable cloneConfig(IdentifiedDataSerializable config) {
        SerializationService serializationService = nodeEngine.getSerializationService();
        Data data = serializationService.toData(config);
        return serializationService.toObject(data);
    }

    @Override
    @SuppressWarnings("checkstyle:methodlength")
    public void registerConfigLocally(IdentifiedDataSerializable newConfig, boolean failWhenNotEquals) {
        IdentifiedDataSerializable currentConfig = null;
        if (newConfig instanceof MultiMapConfig) {
            MultiMapConfig multiMapConfig = (MultiMapConfig) newConfig;
            currentConfig = multiMapConfigs.putIfAbsent(multiMapConfig.getName(), multiMapConfig);
        } else if (newConfig instanceof MapConfig) {
            MapConfig newMapConfig = (MapConfig) newConfig;
            currentConfig = mapConfigs.putIfAbsent(newMapConfig.getName(), newMapConfig);
            if (currentConfig == null) {
                listener.onConfigRegistered(newMapConfig);
            }
        } else if (newConfig instanceof CardinalityEstimatorConfig) {
            CardinalityEstimatorConfig cardinalityEstimatorConfig = (CardinalityEstimatorConfig) newConfig;
            currentConfig = cardinalityEstimatorConfigs.putIfAbsent(cardinalityEstimatorConfig.getName(),
                    cardinalityEstimatorConfig);
        } else if (newConfig instanceof RingbufferConfig) {
            RingbufferConfig ringbufferConfig = (RingbufferConfig) newConfig;
            currentConfig = ringbufferConfigs.putIfAbsent(ringbufferConfig.getName(), ringbufferConfig);
        } else if (newConfig instanceof LockConfig) {
            LockConfig lockConfig = (LockConfig) newConfig;
            currentConfig = lockConfigs.putIfAbsent(lockConfig.getName(), lockConfig);
        } else if (newConfig instanceof ListConfig) {
            ListConfig listConfig = (ListConfig) newConfig;
            currentConfig = listConfigs.putIfAbsent(listConfig.getName(), listConfig);
        } else if (newConfig instanceof SetConfig) {
            SetConfig setConfig = (SetConfig) newConfig;
            currentConfig = setConfigs.putIfAbsent(setConfig.getName(), setConfig);
        } else if (newConfig instanceof ReplicatedMapConfig) {
            ReplicatedMapConfig replicatedMapConfig = (ReplicatedMapConfig) newConfig;
            currentConfig = replicatedMapConfigs.putIfAbsent(replicatedMapConfig.getName(), replicatedMapConfig);
        } else if (newConfig instanceof TopicConfig) {
            TopicConfig topicConfig = (TopicConfig) newConfig;
            currentConfig = topicConfigs.putIfAbsent(topicConfig.getName(), topicConfig);
        } else if (newConfig instanceof ExecutorConfig) {
            ExecutorConfig executorConfig = (ExecutorConfig) newConfig;
            currentConfig = executorConfigs.putIfAbsent(executorConfig.getName(), executorConfig);
        } else if (newConfig instanceof DurableExecutorConfig) {
            DurableExecutorConfig durableExecutorConfig = (DurableExecutorConfig) newConfig;
            currentConfig = durableExecutorConfigs.putIfAbsent(durableExecutorConfig.getName(), durableExecutorConfig);
        } else if (newConfig instanceof ScheduledExecutorConfig) {
            ScheduledExecutorConfig scheduledExecutorConfig = (ScheduledExecutorConfig) newConfig;
            currentConfig = scheduledExecutorConfigs.putIfAbsent(scheduledExecutorConfig.getName(), scheduledExecutorConfig);
        } else if (newConfig instanceof QueueConfig) {
            QueueConfig queueConfig = (QueueConfig) newConfig;
            currentConfig = queueConfigs.putIfAbsent(queueConfig.getName(), queueConfig);
        } else if (newConfig instanceof ReliableTopicConfig) {
            ReliableTopicConfig reliableTopicConfig = (ReliableTopicConfig) newConfig;
            currentConfig = reliableTopicConfigs.putIfAbsent(reliableTopicConfig.getName(), reliableTopicConfig);
        } else if (newConfig instanceof CacheSimpleConfig) {
            CacheSimpleConfig cacheSimpleConfig = (CacheSimpleConfig) newConfig;
            currentConfig = cacheSimpleConfigs.putIfAbsent(cacheSimpleConfig.getName(), cacheSimpleConfig);
            if (currentConfig == null) {
                listener.onConfigRegistered(cacheSimpleConfig);
            }
        } else if (newConfig instanceof EventJournalConfig) {
            EventJournalConfig eventJournalConfig = (EventJournalConfig) newConfig;
            registerEventJournalConfig(eventJournalConfig, failWhenNotEquals);
        } else {
            throw new UnsupportedOperationException("Unsupported config type: " + newConfig);
        }
        checkCurrentConfigNullOrEqual(failWhenNotEquals, currentConfig, newConfig);
    }

    private void registerEventJournalConfig(EventJournalConfig eventJournalConfig, boolean failWhenNotEquals) {
        String mapName = eventJournalConfig.getMapName();
        String cacheName = eventJournalConfig.getMapName();
        synchronized (journalMutex) {
            EventJournalConfig currentMapJournalConfig = null;
            if (mapName != null) {
                currentMapJournalConfig = mapEventJournalConfigs.putIfAbsent(mapName, eventJournalConfig);
                checkCurrentConfigNullOrEqual(failWhenNotEquals, currentMapJournalConfig, eventJournalConfig);
            }
            if (cacheName != null) {
                EventJournalConfig currentCacheJournalConfig = cacheEventJournalConfigs.putIfAbsent(cacheName,
                        eventJournalConfig);
                try {
                    checkCurrentConfigNullOrEqual(failWhenNotEquals, currentCacheJournalConfig, eventJournalConfig);
                } catch (ConfigurationException e) {
                    // exception when registering cache journal config.
                    // let's remove map journal if we configured any
                    if (mapName != null && currentMapJournalConfig == null) {
                        mapEventJournalConfigs.remove(mapName);
                    }
                    throw e;
                }
            }
        }
    }

    private void checkCurrentConfigNullOrEqual(boolean failWhenNotEquals, Object currentConfig, Object newConfig) {
        if (IGNORE_CONFLICTING_CONFIGS_WORKAROUND || !failWhenNotEquals || currentConfig == null) {
            return;
        }
        if (!currentConfig.equals(newConfig)) {
            throw new ConfigurationException("Cannot add a dynamic configuration '" + newConfig + "' as there"
                     + " is already a conflicting configuration '" + currentConfig + "'");
        }
    }

    @Override
    public MultiMapConfig findMultiMapConfig(String name) {
        return lookupByPattern(configPatternMatcher, multiMapConfigs, name);
    }

    @Override
    public ConcurrentMap<String, MultiMapConfig> getMultiMapConfigs() {
        return multiMapConfigs;
    }

    @Override
    public MapConfig findMapConfig(String name) {
        return lookupByPattern(configPatternMatcher, mapConfigs, name);
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        return mapConfigs;
    }

    @Override
    public TopicConfig findTopicConfig(String name) {
        return lookupByPattern(configPatternMatcher, topicConfigs, name);
    }

    @Override
    public ConcurrentMap<String, TopicConfig> getTopicConfigs() {
        return topicConfigs;
    }

    @Override
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        return lookupByPattern(configPatternMatcher, cardinalityEstimatorConfigs, name);
    }

    @Override
    public ConcurrentMap<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        return cardinalityEstimatorConfigs;
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        return lookupByPattern(configPatternMatcher, executorConfigs, name);
    }

    @Override
    public ConcurrentMap<String, ExecutorConfig> getExecutorConfigs() {
        return executorConfigs;
    }

    @Override
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        return lookupByPattern(configPatternMatcher, scheduledExecutorConfigs, name);
    }

    @Override
    public ConcurrentMap<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        return scheduledExecutorConfigs;
    }

    @Override
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        return lookupByPattern(configPatternMatcher, durableExecutorConfigs, name);
    }

    @Override
    public ConcurrentMap<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        return durableExecutorConfigs;
    }

    @Override
    public SemaphoreConfig findSemaphoreConfig(String name) {
        return lookupByPattern(configPatternMatcher, semaphoreConfigs, name);
    }

    @Override
    public ConcurrentMap<String, SemaphoreConfig> getSemaphoreConfigs() {
        return semaphoreConfigs;
    }

    @Override
    public RingbufferConfig findRingbufferConfig(String name) {
        return lookupByPattern(configPatternMatcher, ringbufferConfigs, name);
    }

    @Override
    public ConcurrentMap<String, RingbufferConfig> getRingbufferConfigs() {
        return ringbufferConfigs;
    }

    @Override
    public LockConfig findLockConfig(String name) {
        return lookupByPattern(configPatternMatcher, lockConfigs, name);
    }

    @Override
    public Map<String, LockConfig> getLockConfigs() {
        return lockConfigs;
    }

    @Override
    public ListConfig findListConfig(String name) {
        return lookupByPattern(configPatternMatcher, listConfigs, name);
    }

    @Override
    public ConcurrentMap<String, ListConfig> getListConfigs() {
        return listConfigs;
    }

    @Override
    public QueueConfig findQueueConfig(String name) {
        return lookupByPattern(configPatternMatcher, queueConfigs, name);
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        return queueConfigs;
    }

    @Override
    public SetConfig findSetConfig(String name) {
        return lookupByPattern(configPatternMatcher, setConfigs, name);
    }

    @Override
    public ConcurrentMap<String, SetConfig> getSetConfigs() {
        return setConfigs;
    }

    @Override
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        return lookupByPattern(configPatternMatcher, replicatedMapConfigs, name);
    }

    @Override
    public ConcurrentMap<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return replicatedMapConfigs;
    }

    @Override
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        return lookupByPattern(configPatternMatcher, reliableTopicConfigs, name);
    }

    @Override
    public ConcurrentMap<String, ReliableTopicConfig> getReliableTopicConfigs() {
        return reliableTopicConfigs;
    }

    @Override
    public CacheSimpleConfig findCacheSimpleConfig(String name) {
        return lookupByPattern(configPatternMatcher, cacheSimpleConfigs, name);
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheSimpleConfigs() {
        return cacheSimpleConfigs;
    }

    @Override
    public EventJournalConfig findCacheEventJournalConfig(String name) {
        return lookupByPattern(configPatternMatcher, cacheEventJournalConfigs, name);
    }

    @Override
    public Map<String, EventJournalConfig> getCacheEventJournalConfigs() {
        return cacheEventJournalConfigs;
    }

    @Override
    public EventJournalConfig findMapEventJournalConfig(String name) {
        return lookupByPattern(configPatternMatcher, mapEventJournalConfigs, name);
    }

    @Override
    public Map<String, EventJournalConfig> getMapEventJournalConfigs() {
        return mapEventJournalConfigs;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        if (version.isLessOrEqual(V3_8)) {
            return null;
        }
        IdentifiedDataSerializable[] allConfigurations = getAllConfigurations();
        if (allConfigurations.length == 0) {
            return null;
        }
        return new Merger(nodeEngine, new DynamicConfigReplicationOperation(allConfigurations, false));
    }

    public static class Merger implements Runnable {
        private final NodeEngine nodeEngine;
        private final Operation replicationOperation;

        public Merger(NodeEngine nodeEngine, Operation replicationOperation) {
            this.nodeEngine = nodeEngine;
            this.replicationOperation = replicationOperation;
        }

        @Override
        public void run() {
            OperationService operationService = nodeEngine.getOperationService();
            BinaryOperationFactory operationFactory = new BinaryOperationFactory(replicationOperation, nodeEngine);
            try {
                // intentionally targeting partitions and not members to avoid races
                // it has generates extra load, but it's worth it.
                operationService.invokeOnAllPartitions(SERVICE_NAME, operationFactory);
            } catch (Exception e) {
                throw new HazelcastException("Error while merging configurations", e);
            }
        }
    }
}
