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

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.version.Version;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static com.hazelcast.internal.cluster.Versions.V4_0;
import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static com.hazelcast.internal.util.FutureUtil.waitForever;
import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;
import static java.lang.Boolean.getBoolean;
import static java.lang.String.format;
import static java.util.Collections.singleton;

@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class ClusterWideConfigurationService implements
        PreJoinAwareService,
        CoreService,
        ClusterVersionListener,
        ManagedService,
        ConfigurationService,
        SplitBrainHandlerService {

    public static final int CONFIG_PUBLISH_MAX_ATTEMPT_COUNT = 100;

    // RU_COMPAT
    // maps config class to cluster version in which it was introduced
    static final Map<Class<? extends IdentifiedDataSerializable>, Version> CONFIG_TO_VERSION;

    //this is meant to be used as a workaround for buggy equals/hashcode implementations
    private static final boolean IGNORE_CONFLICTING_CONFIGS_WORKAROUND = getBoolean("hazelcast.dynamicconfig.ignore.conflicts");

    protected final NodeEngine nodeEngine;
    protected final ILogger logger;

    private final DynamicConfigListener listener;

    private final ConcurrentMap<String, MapConfig> mapConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MultiMapConfig> multiMapConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PNCounterConfig> pnCounterConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, RingbufferConfig> ringbufferConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ListConfig> listConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SetConfig> setConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ReplicatedMapConfig> replicatedMapConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TopicConfig> topicConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ExecutorConfig> executorConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DurableExecutorConfig> durableExecutorConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ScheduledExecutorConfig> scheduledExecutorConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, QueueConfig> queueConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ReliableTopicConfig> reliableTopicConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CacheSimpleConfig> cacheSimpleConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, FlakeIdGeneratorConfig> flakeIdGeneratorConfigs = new ConcurrentHashMap<>();

    private final ConfigPatternMatcher configPatternMatcher;

    @SuppressWarnings("unchecked")
    private final Map<?, ? extends IdentifiedDataSerializable>[] allConfigurations = new Map[]{
            mapConfigs,
            multiMapConfigs,
            cardinalityEstimatorConfigs,
            ringbufferConfigs,
            listConfigs,
            setConfigs,
            replicatedMapConfigs,
            topicConfigs,
            executorConfigs,
            durableExecutorConfigs,
            scheduledExecutorConfigs,
            queueConfigs,
            reliableTopicConfigs,
            cacheSimpleConfigs,
            flakeIdGeneratorConfigs,
            pnCounterConfigs,
    };

    private volatile Version version;

    static {
        CONFIG_TO_VERSION = initializeConfigToVersionMap();
    }

    public ClusterWideConfigurationService(
            NodeEngine nodeEngine,
            DynamicConfigListener dynamicConfigListener
    ) {
        this.nodeEngine = nodeEngine;
        this.listener = dynamicConfigListener;
        this.configPatternMatcher = nodeEngine.getConfig().getConfigPatternMatcher();
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public Operation getPreJoinOperation() {
        IdentifiedDataSerializable[] allConfigurations = collectAllDynamicConfigs();
        if (noConfigurationExist(allConfigurations)) {
            // there is no dynamic configuration -> no need to send an empty operation
            return null;
        }
        return new DynamicConfigPreJoinOperation(allConfigurations, ConfigCheckMode.WARNING);
    }

    private boolean noConfigurationExist(IdentifiedDataSerializable[] configurations) {
        return configurations.length == 0;
    }

    private IdentifiedDataSerializable[] collectAllDynamicConfigs() {
        List<IdentifiedDataSerializable> all = new ArrayList<>();
        for (Map<?, ? extends IdentifiedDataSerializable> entry : allConfigurations) {
            Collection<? extends IdentifiedDataSerializable> values = entry.values();
            all.addAll(values);
        }
        return all.toArray(new IdentifiedDataSerializable[0]);
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
        InternalCompletableFuture<Object> future = broadcastConfigAsync(config);
        future.joinInternal();
    }

    @Override
    public void updateLicense(String licenseKey) {
        throw new UnsupportedOperationException("Updating the license requires Hazelcast Enterprise");
    }

    @Override
    public ConfigUpdateResult update(@Nullable Config newConfig) {
        throw new UnsupportedOperationException("Configuration Reload requires Hazelcast Enterprise Edition");
    }

    @Override
    public UUID updateAsync(String configPatch) {
        throw new UnsupportedOperationException("Configuration Reload requires Hazelcast Enterprise Edition");
    }

    public InternalCompletableFuture<Object> broadcastConfigAsync(IdentifiedDataSerializable config) {
        checkConfigVersion(config);
        // we create a defensive copy as local operation execution might use a fast-path
        // and avoid config serialization altogether.
        // we certainly do not want the dynamic config service to reference object a user can mutate
        IdentifiedDataSerializable clonedConfig = cloneConfig(config);
        ClusterService clusterService = nodeEngine.getClusterService();
        return invokeOnStableClusterSerial(
                nodeEngine,
                new AddDynamicConfigOperationSupplier(clusterService, clonedConfig),
                CONFIG_PUBLISH_MAX_ATTEMPT_COUNT
        );
    }

    private void checkConfigVersion(IdentifiedDataSerializable config) {
        Class<? extends IdentifiedDataSerializable> configClass = config.getClass();
        Version currentClusterVersion = version;
        Version introducedIn = CONFIG_TO_VERSION.get(configClass);
        if (currentClusterVersion.isLessThan(introducedIn)) {
            throw new UnsupportedOperationException(format("Config '%s' is available since version '%s'. "
                            + "Current cluster version '%s' does not allow dynamically adding '%1$s'.",
                    configClass.getSimpleName(),
                    introducedIn.toString(),
                    currentClusterVersion
            ));
        }
    }

    private IdentifiedDataSerializable cloneConfig(IdentifiedDataSerializable config) {
        SerializationService serializationService = nodeEngine.getSerializationService();
        Data data = serializationService.toData(config);
        return serializationService.toObject(data);
    }


    /**
     * Register a dynamic configuration in a local member. When a dynamic configuration with the same name already
     * exists then this call has no effect.
     *
     * @param newConfig       Configuration to register.
     * @param configCheckMode behaviour when a config is detected
     * @throws UnsupportedOperationException when given configuration type is not supported
     * @throws InvalidConfigurationException when conflict is detected and configCheckMode is on THROW_EXCEPTION
     */
    @SuppressWarnings("checkstyle:methodlength")
    public void registerConfigLocally(IdentifiedDataSerializable newConfig, ConfigCheckMode configCheckMode) {
        IdentifiedDataSerializable currentConfig;
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
            currentConfig = cardinalityEstimatorConfigs.putIfAbsent(
                    cardinalityEstimatorConfig.getName(),
                    cardinalityEstimatorConfig
            );
        } else if (newConfig instanceof RingbufferConfig) {
            RingbufferConfig ringbufferConfig = (RingbufferConfig) newConfig;
            currentConfig = ringbufferConfigs.putIfAbsent(ringbufferConfig.getName(), ringbufferConfig);
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
        } else if (newConfig instanceof FlakeIdGeneratorConfig) {
            FlakeIdGeneratorConfig config = (FlakeIdGeneratorConfig) newConfig;
            currentConfig = flakeIdGeneratorConfigs.putIfAbsent(config.getName(), config);
        } else if (newConfig instanceof PNCounterConfig) {
            PNCounterConfig config = (PNCounterConfig) newConfig;
            currentConfig = pnCounterConfigs.putIfAbsent(config.getName(), config);
        } else {
            throw new UnsupportedOperationException("Unsupported config type: " + newConfig);
        }
        checkCurrentConfigNullOrEqual(configCheckMode, currentConfig, newConfig);
        persist(newConfig);
    }

    protected void checkCurrentConfigNullOrEqual(ConfigCheckMode checkMode, Object currentConfig, Object newConfig) {
        if (IGNORE_CONFLICTING_CONFIGS_WORKAROUND) {
            return;
        }
        if (currentConfig == null) {
            return;
        }
        if (!currentConfig.equals(newConfig)) {
            String message = "Cannot add a dynamic configuration '" + newConfig + "' as there"
                    + " is already a conflicting configuration '" + currentConfig + "'";
            switch (checkMode) {
                case THROW_EXCEPTION:
                    throw new InvalidConfigurationException(message);
                case WARNING:
                    logger.warning(message);
                    break;
                case SILENT:
                    logger.finest(message);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown consistency check mode: " + checkMode);
            }
        }
    }

    @Override
    public void persist(Object subConfig) {
        if (nodeEngine.getConfig().getDynamicConfigurationConfig().isPersistenceEnabled()) {
            // Code should never come here. We should fast fail in
            // DefaultNodeExtension#checkDynamicConfigurationPersistenceAllowed()
            throw new UnsupportedOperationException("Dynamic Configuration Persistence requires Hazelcast Enterprise Edition");
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
    public PNCounterConfig findPNCounterConfig(String name) {
        return lookupByPattern(configPatternMatcher, pnCounterConfigs, name);
    }

    @Override
    public ConcurrentMap<String, PNCounterConfig> getPNCounterConfigs() {
        return pnCounterConfigs;
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
    public RingbufferConfig findRingbufferConfig(String name) {
        return lookupByPattern(configPatternMatcher, ringbufferConfigs, name);
    }

    @Override
    public ConcurrentMap<String, RingbufferConfig> getRingbufferConfigs() {
        return ringbufferConfigs;
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
    public FlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String baseName) {
        return lookupByPattern(configPatternMatcher, flakeIdGeneratorConfigs, baseName);
    }

    @Override
    public Map<String, FlakeIdGeneratorConfig> getFlakeIdGeneratorConfigs() {
        return flakeIdGeneratorConfigs;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        IdentifiedDataSerializable[] allConfigurations = collectAllDynamicConfigs();
        if (noConfigurationExist(allConfigurations)) {
            return null;
        }
        return new Merger(nodeEngine, allConfigurations);
    }

    public static class Merger implements Runnable {
        private final NodeEngine nodeEngine;
        private final IdentifiedDataSerializable[] allConfigurations;

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
        public Merger(NodeEngine nodeEngine, IdentifiedDataSerializable[] allConfigurations) {
            this.nodeEngine = nodeEngine;
            this.allConfigurations = allConfigurations;
        }

        @Override
        public void run() {
            try {
                Future<Object> future = invokeOnStableClusterSerial(
                        nodeEngine,
                        () -> new DynamicConfigPreJoinOperation(allConfigurations, ConfigCheckMode.SILENT),
                        CONFIG_PUBLISH_MAX_ATTEMPT_COUNT
                );
                waitForever(singleton(future), FutureUtil.RETHROW_EVERYTHING);
            } catch (Exception e) {
                throw new HazelcastException("Error while merging configurations", e);
            }
        }
    }

    private static Map<Class<? extends IdentifiedDataSerializable>, Version> initializeConfigToVersionMap() {
        Map<Class<? extends IdentifiedDataSerializable>, Version> configToVersion =
                new HashMap<>();

        configToVersion.put(MapConfig.class, V4_0);
        configToVersion.put(MultiMapConfig.class, V4_0);
        configToVersion.put(CardinalityEstimatorConfig.class, V4_0);
        configToVersion.put(RingbufferConfig.class, V4_0);
        configToVersion.put(ListConfig.class, V4_0);
        configToVersion.put(SetConfig.class, V4_0);
        configToVersion.put(ReplicatedMapConfig.class, V4_0);
        configToVersion.put(TopicConfig.class, V4_0);
        configToVersion.put(ExecutorConfig.class, V4_0);
        configToVersion.put(DurableExecutorConfig.class, V4_0);
        configToVersion.put(ScheduledExecutorConfig.class, V4_0);
        configToVersion.put(QueueConfig.class, V4_0);
        configToVersion.put(ReliableTopicConfig.class, V4_0);
        configToVersion.put(CacheSimpleConfig.class, V4_0);
        configToVersion.put(EventJournalConfig.class, V4_0);
        configToVersion.put(FlakeIdGeneratorConfig.class, V4_0);
        configToVersion.put(PNCounterConfig.class, V4_0);
        configToVersion.put(MerkleTreeConfig.class, V4_0);

        return Collections.unmodifiableMap(configToVersion);
    }
}
