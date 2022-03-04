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

package com.hazelcast.internal.dynamicconfig.search;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.internal.dynamicconfig.ConfigurationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Config search factory, which allows to build searchers for the given config types.
 *
 * @since 3.11
 * @see Searcher
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:executablestatementcount"})
public final class ConfigSearch {
    private static final Map<Class, ConfigSupplier> CONFIG_SUPPLIERS = new ConcurrentHashMap<Class, ConfigSupplier>();

    private ConfigSearch() {
    }

    static {
        CONFIG_SUPPLIERS.put(MapConfig.class, new ConfigSupplier<MapConfig>() {
            @Override
            public MapConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
                return configurationService.findMapConfig(name);
            }

            @Override
            public MapConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getMapConfig(name);
            }

            @Override
            public Map<String, MapConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getMapConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(CacheSimpleConfig.class, new ConfigSupplier<CacheSimpleConfig>() {
            @Override
            public CacheSimpleConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
                return configurationService.findCacheSimpleConfig(name);
            }

            @Override
            public CacheSimpleConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getCacheConfig(name);
            }

            @Override
            public Map<String, CacheSimpleConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getCacheConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(QueueConfig.class, new ConfigSupplier<QueueConfig>() {
            @Override
            public QueueConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
                return configurationService.findQueueConfig(name);
            }

            @Override
            public QueueConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getQueueConfig(name);
            }

            @Override
            public Map<String, QueueConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getQueueConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(ListConfig.class, new ConfigSupplier<ListConfig>() {
            @Override
            public ListConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
                return configurationService.findListConfig(name);
            }

            @Override
            public ListConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getListConfig(name);
            }

            @Override
            public Map<String, ListConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getListConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(SetConfig.class, new ConfigSupplier<SetConfig>() {
            @Override
            public SetConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
                return configurationService.findSetConfig(name);
            }

            @Override
            public SetConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getSetConfig(name);
            }

            @Override
            public Map<String, SetConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getSetConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(MultiMapConfig.class, new ConfigSupplier<MultiMapConfig>() {
            @Override
            public MultiMapConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
                return configurationService.findMultiMapConfig(name);
            }

            @Override
            public MultiMapConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getMultiMapConfig(name);
            }

            @Override
            public Map<String, MultiMapConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getMultiMapConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(ReplicatedMapConfig.class, new ConfigSupplier<ReplicatedMapConfig>() {
            @Override
            public ReplicatedMapConfig getDynamicConfig(@Nonnull ConfigurationService configurationService,
                                                        @Nonnull String name) {
                return configurationService.findReplicatedMapConfig(name);
            }

            @Override
            public ReplicatedMapConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getReplicatedMapConfig(name);
            }

            @Override
            public Map<String, ReplicatedMapConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getReplicatedMapConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(RingbufferConfig.class, new ConfigSupplier<RingbufferConfig>() {
            @Override
            public RingbufferConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
                return configurationService.findRingbufferConfig(name);
            }

            @Override
            public RingbufferConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getRingbufferConfig(name);
            }

            @Override
            public Map<String, RingbufferConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getRingbufferConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(TopicConfig.class, new ConfigSupplier<TopicConfig>() {
            @Override
            public TopicConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
                return configurationService.findTopicConfig(name);
            }

            @Override
            public TopicConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getTopicConfig(name);
            }

            @Override
            public Map<String, TopicConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getTopicConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(ReliableTopicConfig.class, new ConfigSupplier<ReliableTopicConfig>() {
            @Override
            public ReliableTopicConfig getDynamicConfig(@Nonnull ConfigurationService configurationService,
                                                        @Nonnull String name) {
                return configurationService.findReliableTopicConfig(name);
            }

            @Override
            public ReliableTopicConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getReliableTopicConfig(name);
            }

            @Override
            public Map<String, ReliableTopicConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getReliableTopicConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(ExecutorConfig.class, new ConfigSupplier<ExecutorConfig>() {
            @Override
            public ExecutorConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
                return configurationService.findExecutorConfig(name);
            }

            @Override
            public ExecutorConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getExecutorConfig(name);
            }

            @Override
            public Map<String, ExecutorConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getExecutorConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(DurableExecutorConfig.class, new ConfigSupplier<DurableExecutorConfig>() {
            @Override
            public DurableExecutorConfig getDynamicConfig(@Nonnull ConfigurationService configurationService,
                                                          @Nonnull String name) {
                return configurationService.findDurableExecutorConfig(name);
            }

            @Override
            public DurableExecutorConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getDurableExecutorConfig(name);
            }

            @Override
            public Map<String, DurableExecutorConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getDurableExecutorConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(ScheduledExecutorConfig.class, new ConfigSupplier<ScheduledExecutorConfig>() {
            @Override
            public ScheduledExecutorConfig getDynamicConfig(@Nonnull ConfigurationService configurationService,
                                                            @Nonnull String name) {
                return configurationService.findScheduledExecutorConfig(name);
            }

            @Override
            public ScheduledExecutorConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getScheduledExecutorConfig(name);
            }

            @Override
            public Map<String, ScheduledExecutorConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getScheduledExecutorConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(CardinalityEstimatorConfig.class, new ConfigSupplier<CardinalityEstimatorConfig>() {
            @Override
            public CardinalityEstimatorConfig getDynamicConfig(@Nonnull ConfigurationService configurationService,
                                                               @Nonnull String name) {
                return configurationService.findCardinalityEstimatorConfig(name);
            }

            @Override
            public CardinalityEstimatorConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getCardinalityEstimatorConfig(name);
            }

            @Override
            public Map<String, CardinalityEstimatorConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getCardinalityEstimatorConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(FlakeIdGeneratorConfig.class, new ConfigSupplier<FlakeIdGeneratorConfig>() {
            @Override
            public FlakeIdGeneratorConfig getDynamicConfig(@Nonnull ConfigurationService configurationService,
                                                           @Nonnull String name) {
                return configurationService.findFlakeIdGeneratorConfig(name);
            }

            @Override
            public FlakeIdGeneratorConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getFlakeIdGeneratorConfig(name);
            }

            @Override
            public Map<String, FlakeIdGeneratorConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getFlakeIdGeneratorConfigs();
            }
        });
        CONFIG_SUPPLIERS.put(PNCounterConfig.class, new ConfigSupplier<PNCounterConfig>() {
            @Override
            public PNCounterConfig getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name) {
                return configurationService.findPNCounterConfig(name);
            }

            @Override
            public PNCounterConfig getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name) {
                return staticConfig.getPNCounterConfig(name);
            }

            @Override
            public Map<String, PNCounterConfig> getStaticConfigs(@Nonnull Config staticConfig) {
                return staticConfig.getPNCounterConfigs();
            }
        });
    }

    /**
     * Factory method for providing {@link ConfigSupplier} instances for the given config class
     *
     * @param cls class of the config we're looking for
     * @param <T> type of config class
     * @return {@link ConfigSupplier} config supplier for the given config type
     * @see ClusterProperty#SEARCH_DYNAMIC_CONFIG_FIRST
     */
    @Nullable
    public static <T extends IdentifiedDataSerializable> ConfigSupplier<T> supplierFor(@Nonnull Class<T> cls) {
        return CONFIG_SUPPLIERS.get(cls);
    }

    /**
     * Factory method for creating {@link Searcher} instances
     *
     * @param staticConfig hazelcast static config
     * @param configurationService configuration service
     * @param isStaticFirst <code>true</code> if {@link Searcher} should look into static config first, and
     *                      <code>false</code> otherwise.
     * @param <T> type of config class
     * @return {@link Searcher} for the given config type and conditions
     * @see ClusterProperty#SEARCH_DYNAMIC_CONFIG_FIRST
     * @see ConfigSupplier
     */
    @Nonnull
    public static <T extends IdentifiedDataSerializable>
    Searcher<T> searcherFor(@Nonnull final Config staticConfig,
                            @Nonnull final ConfigurationService configurationService,
                            @Nonnull final ConfigPatternMatcher configPatternMatcher, boolean isStaticFirst) {
        return isStaticFirst
                ? new StaticFirstSearcher<T>(configurationService, staticConfig, configPatternMatcher)
                : new DynamicFirstSearcher<T>(configurationService, staticConfig, configPatternMatcher);
    }
}
