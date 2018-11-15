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

import com.hazelcast.internal.dynamicconfig.AddDynamicConfigOperation;
import com.hazelcast.internal.dynamicconfig.DynamicConfigPreJoinOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.eviction.LFUEvictionPolicy;
import com.hazelcast.map.eviction.LRUEvictionPolicy;
import com.hazelcast.map.eviction.RandomEvictionPolicy;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CONFIG_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CONFIG_DS_FACTORY_ID;

/**
 * DataSerializerHook for com.hazelcast.config classes
 */
@SuppressWarnings("checkstyle:javadocvariable")
public final class ConfigDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(CONFIG_DS_FACTORY, CONFIG_DS_FACTORY_ID);

    public static final int WAN_REPLICATION_CONFIG = 0;
    public static final int WAN_CONSUMER_CONFIG = 1;
    public static final int WAN_PUBLISHER_CONFIG = 2;
    public static final int NEAR_CACHE_CONFIG = 3;
    public static final int NEAR_CACHE_PRELOADER_CONFIG = 4;
    public static final int ADD_DYNAMIC_CONFIG_OP = 5;
    public static final int DYNAMIC_CONFIG_PRE_JOIN_OP = 6;
    public static final int MULTIMAP_CONFIG = 7;
    public static final int LISTENER_CONFIG = 8;
    public static final int ENTRY_LISTENER_CONFIG = 9;
    public static final int MAP_CONFIG = 10;
    public static final int RANDOM_EVICTION_POLICY = 11;
    public static final int LFU_EVICTION_POLICY = 12;
    public static final int LRU_EVICTION_POLICY = 13;
    public static final int MAP_STORE_CONFIG = 14;
    public static final int MAP_PARTITION_LOST_LISTENER_CONFIG = 15;
    public static final int MAP_INDEX_CONFIG = 16;
    public static final int MAP_ATTRIBUTE_CONFIG = 17;
    public static final int QUERY_CACHE_CONFIG = 18;
    public static final int PREDICATE_CONFIG = 19;
    public static final int PARTITION_STRATEGY_CONFIG = 20;
    public static final int HOT_RESTART_CONFIG = 21;
    public static final int TOPIC_CONFIG = 22;
    public static final int RELIABLE_TOPIC_CONFIG = 23;
    public static final int ITEM_LISTENER_CONFIG = 24;
    public static final int QUEUE_STORE_CONFIG = 25;
    public static final int QUEUE_CONFIG = 26;
    public static final int LOCK_CONFIG = 27;
    public static final int LIST_CONFIG = 28;
    public static final int SET_CONFIG = 29;
    public static final int EXECUTOR_CONFIG = 30;
    public static final int DURABLE_EXECUTOR_CONFIG = 31;
    public static final int SCHEDULED_EXECUTOR_CONFIG = 32;
    public static final int SEMAPHORE_CONFIG = 33;
    public static final int REPLICATED_MAP_CONFIG = 34;
    public static final int RINGBUFFER_CONFIG = 35;
    public static final int RINGBUFFER_STORE_CONFIG = 36;
    public static final int CARDINALITY_ESTIMATOR_CONFIG = 37;
    public static final int SIMPLE_CACHE_CONFIG = 38;
    public static final int SIMPLE_CACHE_CONFIG_EXPIRY_POLICY_FACTORY_CONFIG = 39;
    public static final int SIMPLE_CACHE_CONFIG_TIMED_EXPIRY_POLICY_FACTORY_CONFIG = 40;
    public static final int SIMPLE_CACHE_CONFIG_DURATION_CONFIG = 41;
    public static final int QUORUM_CONFIG = 42;
    public static final int MAP_LISTENER_TO_ENTRY_LISTENER_ADAPTER = 43;
    public static final int EVENT_JOURNAL_CONFIG = 44;
    public static final int QUORUM_LISTENER_CONFIG = 45;
    public static final int CACHE_PARTITION_LOST_LISTENER_CONFIG = 46;
    public static final int SIMPLE_CACHE_ENTRY_LISTENER_CONFIG = 47;
    public static final int FLAKE_ID_GENERATOR_CONFIG = 48;
    public static final int ATOMIC_LONG_CONFIG = 49;
    public static final int ATOMIC_REFERENCE_CONFIG = 50;
    public static final int MERGE_POLICY_CONFIG = 51;
    public static final int COUNT_DOWN_LATCH_CONFIG = 52;
    public static final int PN_COUNTER_CONFIG = 53;
    public static final int MERKLE_TREE_CONFIG = 54;
    public static final int WAN_SYNC_CONFIG = 55;
    public static final int KUBERNETES_CONFIG = 56;
    public static final int EUREKA_CONFIG = 57;
    public static final int GCP_CONFIG = 58;
    public static final int AZURE_CONFIG = 59;
    public static final int AWS_CONFIG = 60;
    public static final int DISCOVERY_CONFIG = 61;
    public static final int DISCOVERY_STRATEGY_CONFIG = 62;

    private static final int LEN = DISCOVERY_STRATEGY_CONFIG + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[WAN_REPLICATION_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new WanReplicationConfig();
            }
        };
        constructors[WAN_CONSUMER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new WanConsumerConfig();
            }
        };
        constructors[WAN_PUBLISHER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new WanPublisherConfig();
            }
        };
        constructors[NEAR_CACHE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new NearCacheConfig();
            }
        };
        constructors[NEAR_CACHE_PRELOADER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new NearCachePreloaderConfig();
            }
        };
        constructors[ADD_DYNAMIC_CONFIG_OP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AddDynamicConfigOperation();
            }
        };
        constructors[DYNAMIC_CONFIG_PRE_JOIN_OP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DynamicConfigPreJoinOperation();
            }
        };
        constructors[MULTIMAP_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultiMapConfig();
            }
        };
        constructors[LISTENER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListenerConfig();
            }
        };
        constructors[ENTRY_LISTENER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EntryListenerConfig();
            }
        };
        constructors[MAP_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapConfig();
            }
        };
        constructors[RANDOM_EVICTION_POLICY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RandomEvictionPolicy();
            }
        };
        constructors[LFU_EVICTION_POLICY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LFUEvictionPolicy();
            }
        };
        constructors[LRU_EVICTION_POLICY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LRUEvictionPolicy();
            }
        };
        constructors[MAP_STORE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapStoreConfig();
            }
        };
        constructors[MAP_PARTITION_LOST_LISTENER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapPartitionLostListenerConfig();
            }
        };
        constructors[MAP_INDEX_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapIndexConfig();
            }
        };
        constructors[MAP_ATTRIBUTE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapAttributeConfig();
            }
        };
        constructors[QUERY_CACHE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueryCacheConfig();
            }
        };
        constructors[PREDICATE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PredicateConfig();
            }
        };
        constructors[PARTITION_STRATEGY_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitioningStrategyConfig();
            }
        };
        constructors[HOT_RESTART_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HotRestartConfig();
            }
        };
        constructors[TOPIC_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TopicConfig();
            }
        };
        constructors[RELIABLE_TOPIC_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReliableTopicConfig();
            }
        };
        constructors[ITEM_LISTENER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ItemListenerConfig();
            }
        };
        constructors[QUEUE_STORE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueStoreConfig();
            }
        };
        constructors[QUEUE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueConfig();
            }
        };
        constructors[LOCK_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LockConfig();
            }
        };
        constructors[LIST_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListConfig();
            }
        };
        constructors[SET_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SetConfig();
            }
        };
        constructors[EXECUTOR_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ExecutorConfig();
            }
        };
        constructors[DURABLE_EXECUTOR_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DurableExecutorConfig();
            }
        };
        constructors[SCHEDULED_EXECUTOR_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ScheduledExecutorConfig();
            }
        };
        constructors[SEMAPHORE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SemaphoreConfig();
            }
        };
        constructors[REPLICATED_MAP_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedMapConfig();
            }
        };
        constructors[RINGBUFFER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RingbufferConfig();
            }
        };
        constructors[RINGBUFFER_STORE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RingbufferStoreConfig();
            }
        };
        constructors[CARDINALITY_ESTIMATOR_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CardinalityEstimatorConfig();
            }
        };
        constructors[SIMPLE_CACHE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheSimpleConfig();
            }
        };
        constructors[SIMPLE_CACHE_CONFIG_EXPIRY_POLICY_FACTORY_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new CacheSimpleConfig.ExpiryPolicyFactoryConfig();
                    }
                };
        constructors[SIMPLE_CACHE_CONFIG_TIMED_EXPIRY_POLICY_FACTORY_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig();
                    }
                };
        constructors[SIMPLE_CACHE_CONFIG_DURATION_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig();
                    }
                };
        constructors[QUORUM_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new QuorumConfig();
                    }
                };
        constructors[MAP_LISTENER_TO_ENTRY_LISTENER_ADAPTER] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new EntryListenerConfig.MapListenerToEntryListenerAdapter();
                    }
                };
        constructors[EVENT_JOURNAL_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new EventJournalConfig();
                    }
                };
        constructors[QUORUM_LISTENER_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new QuorumListenerConfig();
                    }
                };
        constructors[CACHE_PARTITION_LOST_LISTENER_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new CachePartitionLostListenerConfig();
                    }
                };
        constructors[SIMPLE_CACHE_ENTRY_LISTENER_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new CacheSimpleEntryListenerConfig();
                    }
                };
        constructors[FLAKE_ID_GENERATOR_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new FlakeIdGeneratorConfig();
                    }
                };
        constructors[ATOMIC_LONG_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new AtomicLongConfig();
                    }
                };
        constructors[ATOMIC_REFERENCE_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new AtomicReferenceConfig();
                    }
                };
        constructors[MERGE_POLICY_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new MergePolicyConfig();
                    }
                };
        constructors[COUNT_DOWN_LATCH_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new CountDownLatchConfig();
                    }
                };
        constructors[PN_COUNTER_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new PNCounterConfig();
                    }
                };
        constructors[MERKLE_TREE_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new MerkleTreeConfig();
                    }
                };
        constructors[WAN_SYNC_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new WanSyncConfig();
                    }
                };
        constructors[KUBERNETES_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new KubernetesConfig();
                    }
                };
        constructors[EUREKA_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new EurekaConfig();
                    }
                };
        constructors[GCP_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new GcpConfig();
                    }
                };
        constructors[AZURE_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new AzureConfig();
                    }
                };
        constructors[AWS_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new AwsConfig();
                    }
                };
        constructors[DISCOVERY_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new DiscoveryConfig();
                    }
                };
        constructors[DISCOVERY_STRATEGY_CONFIG] =
                new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new DiscoveryStrategyConfig();
                    }
                };
        return new ArrayDataSerializableFactory(constructors);
    }
}
