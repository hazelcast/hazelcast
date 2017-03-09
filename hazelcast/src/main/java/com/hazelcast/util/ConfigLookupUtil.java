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

package com.hazelcast.util;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Map;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;

/**
 * Utility methods that will be used by Config
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public final class ConfigLookupUtil {

    /**
     * EXECUTOR_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<ExecutorConfig, ExecutorConfig> EXECUTOR_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<ExecutorConfig, ExecutorConfig>() {
                @Override
                public ExecutorConfig createNew(ExecutorConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new ExecutorConfig(defaultConfig);
                    } else {
                        return new ExecutorConfig();
                    }
                }
            };

    /**
     * SCHEDULED_EXECUTOR_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<ScheduledExecutorConfig, ScheduledExecutorConfig>
            SCHEDULED_EXECUTOR_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<ScheduledExecutorConfig, ScheduledExecutorConfig>() {
                @Override
                public ScheduledExecutorConfig createNew(ScheduledExecutorConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new ScheduledExecutorConfig(defaultConfig);
                    } else {
                        return new ScheduledExecutorConfig();
                    }
                }
            };

    /**
     * DURABLE_EXECUTOR_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<DurableExecutorConfig, DurableExecutorConfig> DURABLE_EXECUTOR_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<DurableExecutorConfig, DurableExecutorConfig>() {
                @Override
                public DurableExecutorConfig createNew(DurableExecutorConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new DurableExecutorConfig(defaultConfig);
                    } else {
                        return new DurableExecutorConfig();
                    }
                }
            };

    /**
     * MAP_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<MapConfig, MapConfig> MAP_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<MapConfig, MapConfig>() {
                @Override
                public MapConfig createNew(MapConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new MapConfig(defaultConfig);
                    } else {
                        return new MapConfig();
                    }
                }
            };

    /**
     * CACHE_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<CacheSimpleConfig, CacheSimpleConfig> CACHE_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<CacheSimpleConfig, CacheSimpleConfig>() {
                @Override
                public CacheSimpleConfig createNew(CacheSimpleConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new CacheSimpleConfig(defaultConfig);
                    } else {
                        return new CacheSimpleConfig();
                    }
                }
            };

    /**
     * QUEUE_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<QueueConfig, QueueConfig> QUEUE_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<QueueConfig, QueueConfig>() {
                @Override
                public QueueConfig createNew(QueueConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new QueueConfig(defaultConfig);
                    } else {
                        return new QueueConfig();
                    }
                }
            };

    /**
     * LOCK_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<LockConfig, LockConfig> LOCK_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<LockConfig, LockConfig>() {
                @Override
                public LockConfig createNew(LockConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new LockConfig(defaultConfig);
                    } else {
                        return new LockConfig();
                    }
                }
            };

    /**
     * LIST_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<ListConfig, ListConfig> LIST_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<ListConfig, ListConfig>() {
                @Override
                public ListConfig createNew(ListConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new ListConfig(defaultConfig);
                    } else {
                        return new ListConfig();
                    }
                }
            };

    /**
     * SET_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<SetConfig, SetConfig> SET_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<SetConfig, SetConfig>() {
                @Override
                public SetConfig createNew(SetConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new SetConfig(defaultConfig);
                    } else {
                        return new SetConfig();
                    }
                }
            };

    /**
     * MULTI_MAP_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<MultiMapConfig, MultiMapConfig> MULTI_MAP_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<MultiMapConfig, MultiMapConfig>() {
                @Override
                public MultiMapConfig createNew(MultiMapConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new MultiMapConfig(defaultConfig);
                    } else {
                        return new MultiMapConfig();
                    }
                }
            };

    /**
     * REP_MAP_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<ReplicatedMapConfig, ReplicatedMapConfig> REP_MAP_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<ReplicatedMapConfig, ReplicatedMapConfig>() {
                @Override
                public ReplicatedMapConfig createNew(ReplicatedMapConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new ReplicatedMapConfig(defaultConfig);
                    } else {
                        return new ReplicatedMapConfig();
                    }
                }
            };

    /**
     * RING_BUFFER_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<RingbufferConfig, RingbufferConfig> RING_BUFFER_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<RingbufferConfig, RingbufferConfig>() {
                @Override
                public RingbufferConfig createNew(RingbufferConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new RingbufferConfig(defaultConfig);
                    } else {
                        return new RingbufferConfig();
                    }
                }
            };

    /**
     * TOPIC_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<TopicConfig, TopicConfig> TOPIC_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<TopicConfig, TopicConfig>() {
                @Override
                public TopicConfig createNew(TopicConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new TopicConfig(defaultConfig);
                    } else {
                        return new TopicConfig();
                    }
                }
            };

    /**
     * RELIABLE_TOPIC_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<ReliableTopicConfig, ReliableTopicConfig> RELIABLE_TOPIC_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<ReliableTopicConfig, ReliableTopicConfig>() {
                @Override
                public ReliableTopicConfig createNew(ReliableTopicConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new ReliableTopicConfig(defaultConfig);
                    } else {
                        return new ReliableTopicConfig();
                    }
                }
            };

    /**
     * CARD_EST_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<CardinalityEstimatorConfig, CardinalityEstimatorConfig> CARD_EST_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<CardinalityEstimatorConfig, CardinalityEstimatorConfig>() {
                @Override
                public CardinalityEstimatorConfig createNew(CardinalityEstimatorConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new CardinalityEstimatorConfig(defaultConfig);
                    } else {
                        return new CardinalityEstimatorConfig();
                    }
                }
            };

    /**
     * SEMAPHORE_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<SemaphoreConfig, SemaphoreConfig> SEMAPHORE_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<SemaphoreConfig, SemaphoreConfig>() {
                @Override
                public SemaphoreConfig createNew(SemaphoreConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new SemaphoreConfig(defaultConfig);
                    } else {
                        return new SemaphoreConfig();
                    }
                }
            };

    /**
     * JOB_TRACKER_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<JobTrackerConfig, JobTrackerConfig> JOB_TRACKER_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<JobTrackerConfig, JobTrackerConfig>() {
                @Override
                public JobTrackerConfig createNew(JobTrackerConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new JobTrackerConfig(defaultConfig);
                    } else {
                        return new JobTrackerConfig();
                    }
                }
            };

    /**
     * QUORUM_CONFIG_CONSTRUCTOR
     */
    public static final ConstructorFunction<QuorumConfig, QuorumConfig> QUORUM_CONFIG_CONSTRUCTOR =
            new ConstructorFunction<QuorumConfig, QuorumConfig>() {
                @Override
                public QuorumConfig createNew(QuorumConfig defaultConfig) {
                    if (defaultConfig != null) {
                        return new QuorumConfig(defaultConfig);
                    } else {
                        return new QuorumConfig();
                    }
                }
            };

    private static final ILogger LOGGER = Logger.getLogger(Config.class);

    private ConfigLookupUtil() {

    }

    public static <C extends NamedConfig> C findConfig(Map<String, C> configMap, String name,
                                                       ConstructorFunction<C, C> constructorFunction,
                                                       ConfigPatternMatcher configPatternMatcher) {
        String baseName = getBaseName(name);
        C config = lookupByPattern(configMap, baseName, configPatternMatcher);
        if (config != null) {
            return (C) config.getAsReadOnly();
        }
        return (C) getConfig(configMap, "default", constructorFunction, configPatternMatcher).getAsReadOnly();
    }

    public static <C extends NamedConfig> C getConfig(Map<String, C> configMap, String name,
                                                      ConstructorFunction<C, C> constructorFunction,
                                                      ConfigPatternMatcher configPatternMatcher) {
        String baseName = getBaseName(name);
        C config = lookupByPattern(configMap, baseName, configPatternMatcher);
        if (config != null) {
            return config;
        }
        C defConfig = configMap.get("default");
        if (defConfig == null) {
            defConfig = constructorFunction.createNew(null);
            defConfig.setName("default");
            addConfig("default", configMap, defConfig);
        }
        config = constructorFunction.createNew(defConfig);
        config.setName(name);
        addConfig(name, configMap, config);
        return config;
    }

    private static <C extends NamedConfig> void addConfig(String name, Map<String, C> configMap, C executorConfig) {
        configMap.put(name, executorConfig);
    }

    public static <C> C lookupByPattern(Map<String, C> configPatterns, String itemName,
                                        ConfigPatternMatcher configPatternMatcher) {
        C candidate = configPatterns.get(itemName);
        if (candidate != null) {
            return candidate;
        }
        String configPatternKey = configPatternMatcher.matches(configPatterns.keySet(), itemName);
        if (configPatternKey != null) {
            return configPatterns.get(configPatternKey);
        }
        if (!"default".equals(itemName) && !itemName.startsWith("hz:")) {
            LOGGER.finest("No configuration found for " + itemName + ", using default config!");
        }
        return null;
    }

    public static <C extends NamedConfig> void setConfig(Map<String, C> currentConfigMap, Map<String, C> newConfigMap) {
        currentConfigMap.clear();
        currentConfigMap.putAll(newConfigMap);
        for (Map.Entry<String, C> entry : newConfigMap.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
    }

}
