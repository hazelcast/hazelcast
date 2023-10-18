/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NamedConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.internal.management.operation.UpdateTcpIpMemberListOperation;
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
import com.hazelcast.version.Version;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;
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
import static com.hazelcast.internal.cluster.Versions.V5_2;
import static com.hazelcast.internal.cluster.Versions.V5_4;
import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static com.hazelcast.internal.util.FutureUtil.waitForever;
import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;
import static java.lang.Boolean.getBoolean;
import static java.lang.String.format;
import static java.util.Collections.singleton;

@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodcount", "checkstyle:classfanoutcomplexity",
        "checkstyle:ExecutableStatementCount"})
public class ClusterWideConfigurationService implements
        PreJoinAwareService<DynamicConfigPreJoinOperation>,
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

    /** Config class -> {@link NamedConfig#getName()} -> Config instances */
    private final ConcurrentMap<Class<? extends IdentifiedDataSerializable>, ConcurrentMap<String, IdentifiedDataSerializable>>
            allConfigurations = new ConcurrentHashMap<>();

    private final ConfigPatternMatcher configPatternMatcher;

    private volatile Version version;

    static {
        Map<Class<? extends IdentifiedDataSerializable>, Version> configToVersion = new HashMap<>();

        configToVersion.put(MerkleTreeConfig.class, V4_0);
        configToVersion.put(EventJournalConfig.class, V4_0);
        configToVersion.put(MapConfig.class, V4_0);
        configToVersion.put(MultiMapConfig.class, V4_0);
        configToVersion.put(CardinalityEstimatorConfig.class, V4_0);
        configToVersion.put(PNCounterConfig.class, V4_0);
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
        configToVersion.put(FlakeIdGeneratorConfig.class, V4_0);
        configToVersion.put(DataConnectionConfig.class, V5_2);
        configToVersion.put(WanReplicationConfig.class, V5_4);

        CONFIG_TO_VERSION = Collections.unmodifiableMap(configToVersion);
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
    public DynamicConfigPreJoinOperation getPreJoinOperation() {
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
        return allConfigurations.values().stream().map(Map::values).flatMap(Collection::stream)
                .toArray(IdentifiedDataSerializable[]::new);
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
        allConfigurations.values().forEach(Map::clear);
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
        throw new UnsupportedOperationException("Configuration Update requires Hazelcast Enterprise Edition");
    }

    @Override
    public UUID updateAsync(String configPatch) {
        throw new UnsupportedOperationException("Configuration Update requires Hazelcast Enterprise Edition");
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
    @SuppressWarnings({"checkstyle:methodlength", "checkstyle:BooleanExpressionComplexity"})
    public void registerConfigLocally(IdentifiedDataSerializable newConfig, ConfigCheckMode configCheckMode) {
        IdentifiedDataSerializable currentConfig;
        if ((newConfig instanceof MultiMapConfig) || (newConfig instanceof MapConfig)
                || (newConfig instanceof CardinalityEstimatorConfig) || (newConfig instanceof RingbufferConfig)
                || (newConfig instanceof ListConfig) || (newConfig instanceof SetConfig)
                || (newConfig instanceof ReplicatedMapConfig) || (newConfig instanceof TopicConfig)
                || (newConfig instanceof ExecutorConfig) || (newConfig instanceof DurableExecutorConfig)
                || (newConfig instanceof ScheduledExecutorConfig) || (newConfig instanceof QueueConfig)
                || (newConfig instanceof ReliableTopicConfig) || (newConfig instanceof CacheSimpleConfig)
                || (newConfig instanceof FlakeIdGeneratorConfig) || (newConfig instanceof PNCounterConfig)
                || (newConfig instanceof DataConnectionConfig) || (newConfig instanceof WanReplicationConfig)) {
            if (newConfig instanceof NamedConfig) {
                // Cast to allow adding to an upper-bounded wildcard
                Map<String, IdentifiedDataSerializable> configs = (Map<String, IdentifiedDataSerializable>) getConfigs(
                        newConfig.getClass());
                currentConfig = configs.putIfAbsent(((NamedConfig) newConfig).getName(), newConfig);
            } else {
                throw new IllegalArgumentException(
                        "Expected " + newConfig.getClass().getName() + " to be a " + NamedConfig.class.getName());
            }
        } else {
            throw new UnsupportedOperationException("Unsupported config type: " + newConfig);
        }

        if (currentConfig == null) {
            if (newConfig instanceof MapConfig) {
                listener.onConfigRegistered((MapConfig) newConfig);
            } else if (newConfig instanceof CacheSimpleConfig) {
                listener.onConfigRegistered((CacheSimpleConfig) newConfig);
            } else if (newConfig instanceof DataConnectionConfig) {
                nodeEngine.getDataConnectionService().createConfigDataConnection((DataConnectionConfig) newConfig);
            } else if (newConfig instanceof WanReplicationConfig) {
                nodeEngine.getWanReplicationService().addWanReplicationConfig((WanReplicationConfig) newConfig);
            }
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
        return lookupByPattern(configPatternMatcher, getMultiMapConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, MultiMapConfig> getMultiMapConfigs() {
        return getConfigs(MultiMapConfig.class);
    }

    @Override
    public MapConfig findMapConfig(String name) {
        return lookupByPattern(configPatternMatcher, getMapConfigs(), name);
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        return getConfigs(MapConfig.class);
    }

    @Override
    public TopicConfig findTopicConfig(String name) {
        return lookupByPattern(configPatternMatcher, getTopicConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, TopicConfig> getTopicConfigs() {
        return getConfigs(TopicConfig.class);
    }

    @Override
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        return lookupByPattern(configPatternMatcher, getCardinalityEstimatorConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        return getConfigs(CardinalityEstimatorConfig.class);
    }

    @Override
    public PNCounterConfig findPNCounterConfig(String name) {
        return lookupByPattern(configPatternMatcher, getPNCounterConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, PNCounterConfig> getPNCounterConfigs() {
        return getConfigs(PNCounterConfig.class);
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        return lookupByPattern(configPatternMatcher, getExecutorConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, ExecutorConfig> getExecutorConfigs() {
        return getConfigs(ExecutorConfig.class);
    }

    @Override
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        return lookupByPattern(configPatternMatcher, getScheduledExecutorConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        return getConfigs(ScheduledExecutorConfig.class);
    }

    @Override
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        return lookupByPattern(configPatternMatcher, getDurableExecutorConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        return getConfigs(DurableExecutorConfig.class);
    }

    @Override
    public RingbufferConfig findRingbufferConfig(String name) {
        return lookupByPattern(configPatternMatcher, getRingbufferConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, RingbufferConfig> getRingbufferConfigs() {
        return getConfigs(RingbufferConfig.class);
    }

    @Override
    public ListConfig findListConfig(String name) {
        return lookupByPattern(configPatternMatcher, getListConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, ListConfig> getListConfigs() {
        return getConfigs(ListConfig.class);
    }

    @Override
    public QueueConfig findQueueConfig(String name) {
        return lookupByPattern(configPatternMatcher, getQueueConfigs(), name);
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        return getConfigs(QueueConfig.class);
    }

    @Override
    public SetConfig findSetConfig(String name) {
        return lookupByPattern(configPatternMatcher, getSetConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, SetConfig> getSetConfigs() {
        return getConfigs(SetConfig.class);
    }

    @Override
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        return lookupByPattern(configPatternMatcher, getReplicatedMapConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return getConfigs(ReplicatedMapConfig.class);
    }

    @Override
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        return lookupByPattern(configPatternMatcher, getReliableTopicConfigs(), name);
    }

    @Override
    public ConcurrentMap<String, ReliableTopicConfig> getReliableTopicConfigs() {
        return getConfigs(ReliableTopicConfig.class);
    }

    @Override
    public CacheSimpleConfig findCacheSimpleConfig(String name) {
        return lookupByPattern(configPatternMatcher, getCacheSimpleConfigs(), name);
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheSimpleConfigs() {
        return getConfigs(CacheSimpleConfig.class);
    }

    @Override
    public FlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String name) {
        return lookupByPattern(configPatternMatcher, getFlakeIdGeneratorConfigs(), name);
    }

    @Override
    public Map<String, FlakeIdGeneratorConfig> getFlakeIdGeneratorConfigs() {
        return getConfigs(FlakeIdGeneratorConfig.class);
    }

    @Override
    public DataConnectionConfig findDataConnectionConfig(String name) {
        return lookupByPattern(configPatternMatcher, getDataConnectionConfigs(), name);
    }

    @Override
    public Map<String, DataConnectionConfig> getDataConnectionConfigs() {
        return getConfigs(DataConnectionConfig.class);
    }

    @Override
    public WanReplicationConfig findWanReplicationConfig(String name) {
        return lookupByPattern(configPatternMatcher, getWanReplicationConfigs(), name);
    }

    @Override
    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        return getConfigs(WanReplicationConfig.class);
    }

    private <T extends IdentifiedDataSerializable> ConcurrentMap<String, T> getConfigs(Class<T> clazz) {
        return (ConcurrentMap<String, T>) allConfigurations.computeIfAbsent(clazz, x -> new ConcurrentHashMap<>());
    }

    @Override
    public Runnable prepareMergeRunnable() {
        IdentifiedDataSerializable[] allConfigurations = collectAllDynamicConfigs();
        if (noConfigurationExist(allConfigurations)) {
            return null;
        }
        return new Merger(nodeEngine, allConfigurations);
    }

    @Override
    public void updateTcpIpConfigMemberList(List<String> memberList) {
        invokeOnStableClusterSerial(
                nodeEngine,
                () -> new UpdateTcpIpMemberListOperation(memberList), CONFIG_PUBLISH_MAX_ATTEMPT_COUNT
        ).join();
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
}
