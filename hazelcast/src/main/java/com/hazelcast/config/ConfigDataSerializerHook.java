/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
    public static final int WAN_CUSTOM_PUBLISHER_CONFIG = 1;
    public static final int WAN_BATCH_PUBLISHER_CONFIG = 2;
    public static final int WAN_CONSUMER_CONFIG = 3;
    public static final int NEAR_CACHE_CONFIG = 4;
    public static final int NEAR_CACHE_PRELOADER_CONFIG = 5;
    public static final int ADD_DYNAMIC_CONFIG_OP = 6;
    public static final int DYNAMIC_CONFIG_PRE_JOIN_OP = 7;
    public static final int MULTIMAP_CONFIG = 8;
    public static final int LISTENER_CONFIG = 9;
    public static final int ENTRY_LISTENER_CONFIG = 10;
    public static final int MAP_CONFIG = 11;
    public static final int RANDOM_EVICTION_POLICY = 12;
    public static final int LFU_EVICTION_POLICY = 13;
    public static final int LRU_EVICTION_POLICY = 14;
    public static final int MAP_STORE_CONFIG = 15;
    public static final int MAP_PARTITION_LOST_LISTENER_CONFIG = 16;
    public static final int MAP_INDEX_CONFIG = 17;
    public static final int MAP_ATTRIBUTE_CONFIG = 18;
    public static final int QUERY_CACHE_CONFIG = 19;
    public static final int PREDICATE_CONFIG = 20;
    public static final int PARTITION_STRATEGY_CONFIG = 21;
    public static final int HOT_RESTART_CONFIG = 22;
    public static final int TOPIC_CONFIG = 23;
    public static final int RELIABLE_TOPIC_CONFIG = 24;
    public static final int ITEM_LISTENER_CONFIG = 25;
    public static final int QUEUE_STORE_CONFIG = 26;
    public static final int QUEUE_CONFIG = 27;
    public static final int LOCK_CONFIG = 28;
    public static final int LIST_CONFIG = 29;
    public static final int SET_CONFIG = 30;
    public static final int EXECUTOR_CONFIG = 31;
    public static final int DURABLE_EXECUTOR_CONFIG = 32;
    public static final int SCHEDULED_EXECUTOR_CONFIG = 33;
    public static final int SEMAPHORE_CONFIG = 34;
    public static final int REPLICATED_MAP_CONFIG = 35;
    public static final int RINGBUFFER_CONFIG = 36;
    public static final int RINGBUFFER_STORE_CONFIG = 37;
    public static final int CARDINALITY_ESTIMATOR_CONFIG = 38;
    public static final int SIMPLE_CACHE_CONFIG = 39;
    public static final int SIMPLE_CACHE_CONFIG_EXPIRY_POLICY_FACTORY_CONFIG = 40;
    public static final int SIMPLE_CACHE_CONFIG_TIMED_EXPIRY_POLICY_FACTORY_CONFIG = 41;
    public static final int SIMPLE_CACHE_CONFIG_DURATION_CONFIG = 42;
    public static final int QUORUM_CONFIG = 43;
    public static final int MAP_LISTENER_TO_ENTRY_LISTENER_ADAPTER = 44;
    public static final int EVENT_JOURNAL_CONFIG = 45;
    public static final int QUORUM_LISTENER_CONFIG = 46;
    public static final int CACHE_PARTITION_LOST_LISTENER_CONFIG = 47;
    public static final int SIMPLE_CACHE_ENTRY_LISTENER_CONFIG = 48;
    public static final int FLAKE_ID_GENERATOR_CONFIG = 49;
    public static final int ATOMIC_LONG_CONFIG = 50;
    public static final int ATOMIC_REFERENCE_CONFIG = 51;
    public static final int MERGE_POLICY_CONFIG = 52;
    public static final int COUNT_DOWN_LATCH_CONFIG = 53;
    public static final int PN_COUNTER_CONFIG = 54;
    public static final int MERKLE_TREE_CONFIG = 55;
    public static final int WAN_SYNC_CONFIG = 56;
    public static final int KUBERNETES_CONFIG = 57;
    public static final int EUREKA_CONFIG = 58;
    public static final int GCP_CONFIG = 59;
    public static final int AZURE_CONFIG = 60;
    public static final int AWS_CONFIG = 61;
    public static final int DISCOVERY_CONFIG = 62;
    public static final int DISCOVERY_STRATEGY_CONFIG = 63;

    private static final int LEN = DISCOVERY_STRATEGY_CONFIG + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[WAN_REPLICATION_CONFIG] = arg -> new WanReplicationConfig();
        constructors[WAN_CONSUMER_CONFIG] = arg -> new WanConsumerConfig();
        constructors[WAN_CUSTOM_PUBLISHER_CONFIG] = arg -> new CustomWanPublisherConfig();
        constructors[WAN_BATCH_PUBLISHER_CONFIG] = arg -> new WanBatchReplicationPublisherConfig();
        constructors[NEAR_CACHE_CONFIG] = arg -> new NearCacheConfig();
        constructors[NEAR_CACHE_PRELOADER_CONFIG] = arg -> new NearCachePreloaderConfig();
        constructors[ADD_DYNAMIC_CONFIG_OP] = arg -> new AddDynamicConfigOperation();
        constructors[DYNAMIC_CONFIG_PRE_JOIN_OP] = arg -> new DynamicConfigPreJoinOperation();
        constructors[MULTIMAP_CONFIG] = arg -> new MultiMapConfig();
        constructors[LISTENER_CONFIG] = arg -> new ListenerConfig();
        constructors[ENTRY_LISTENER_CONFIG] = arg -> new EntryListenerConfig();
        constructors[MAP_CONFIG] = arg -> new MapConfig();
        constructors[RANDOM_EVICTION_POLICY] = arg -> new RandomEvictionPolicy();
        constructors[LFU_EVICTION_POLICY] = arg -> new LFUEvictionPolicy();
        constructors[LRU_EVICTION_POLICY] = arg -> new LRUEvictionPolicy();
        constructors[MAP_STORE_CONFIG] = arg -> new MapStoreConfig();
        constructors[MAP_PARTITION_LOST_LISTENER_CONFIG] = arg -> new MapPartitionLostListenerConfig();
        constructors[MAP_INDEX_CONFIG] = arg -> new MapIndexConfig();
        constructors[MAP_ATTRIBUTE_CONFIG] = arg -> new MapAttributeConfig();
        constructors[QUERY_CACHE_CONFIG] = arg -> new QueryCacheConfig();
        constructors[PREDICATE_CONFIG] = arg -> new PredicateConfig();
        constructors[PARTITION_STRATEGY_CONFIG] = arg -> new PartitioningStrategyConfig();
        constructors[HOT_RESTART_CONFIG] = arg -> new HotRestartConfig();
        constructors[TOPIC_CONFIG] = arg -> new TopicConfig();
        constructors[RELIABLE_TOPIC_CONFIG] = arg -> new ReliableTopicConfig();
        constructors[ITEM_LISTENER_CONFIG] = arg -> new ItemListenerConfig();
        constructors[QUEUE_STORE_CONFIG] = arg -> new QueueStoreConfig();
        constructors[QUEUE_CONFIG] = arg -> new QueueConfig();
        constructors[LOCK_CONFIG] = arg -> new LockConfig();
        constructors[LIST_CONFIG] = arg -> new ListConfig();
        constructors[SET_CONFIG] = arg -> new SetConfig();
        constructors[EXECUTOR_CONFIG] = arg -> new ExecutorConfig();
        constructors[DURABLE_EXECUTOR_CONFIG] = arg -> new DurableExecutorConfig();
        constructors[SCHEDULED_EXECUTOR_CONFIG] = arg -> new ScheduledExecutorConfig();
        constructors[SEMAPHORE_CONFIG] = arg -> new SemaphoreConfig();
        constructors[REPLICATED_MAP_CONFIG] = arg -> new ReplicatedMapConfig();
        constructors[RINGBUFFER_CONFIG] = arg -> new RingbufferConfig();
        constructors[RINGBUFFER_STORE_CONFIG] = arg -> new RingbufferStoreConfig();
        constructors[CARDINALITY_ESTIMATOR_CONFIG] = arg -> new CardinalityEstimatorConfig();
        constructors[SIMPLE_CACHE_CONFIG] = arg -> new CacheSimpleConfig();
        constructors[SIMPLE_CACHE_CONFIG_EXPIRY_POLICY_FACTORY_CONFIG] =
                arg -> new CacheSimpleConfig.ExpiryPolicyFactoryConfig();
        constructors[SIMPLE_CACHE_CONFIG_TIMED_EXPIRY_POLICY_FACTORY_CONFIG] =
                arg -> new CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig();
        constructors[SIMPLE_CACHE_CONFIG_DURATION_CONFIG] =
                arg -> new CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig();
        constructors[QUORUM_CONFIG] = arg -> new QuorumConfig();
        constructors[MAP_LISTENER_TO_ENTRY_LISTENER_ADAPTER] =
                arg -> new EntryListenerConfig.MapListenerToEntryListenerAdapter();
        constructors[EVENT_JOURNAL_CONFIG] = arg -> new EventJournalConfig();
        constructors[QUORUM_LISTENER_CONFIG] = arg -> new QuorumListenerConfig();
        constructors[CACHE_PARTITION_LOST_LISTENER_CONFIG] = arg -> new CachePartitionLostListenerConfig();
        constructors[SIMPLE_CACHE_ENTRY_LISTENER_CONFIG] = arg -> new CacheSimpleEntryListenerConfig();
        constructors[FLAKE_ID_GENERATOR_CONFIG] = arg -> new FlakeIdGeneratorConfig();
        constructors[ATOMIC_LONG_CONFIG] = arg -> new AtomicLongConfig();
        constructors[ATOMIC_REFERENCE_CONFIG] = arg -> new AtomicReferenceConfig();
        constructors[MERGE_POLICY_CONFIG] = arg -> new MergePolicyConfig();
        constructors[COUNT_DOWN_LATCH_CONFIG] = arg -> new CountDownLatchConfig();
        constructors[PN_COUNTER_CONFIG] = arg -> new PNCounterConfig();
        constructors[MERKLE_TREE_CONFIG] = arg -> new MerkleTreeConfig();
        constructors[WAN_SYNC_CONFIG] = arg -> new WanSyncConfig();
        constructors[KUBERNETES_CONFIG] = arg -> new KubernetesConfig();
        constructors[EUREKA_CONFIG] = arg -> new EurekaConfig();
        constructors[GCP_CONFIG] = arg -> new GcpConfig();
        constructors[AZURE_CONFIG] = arg -> new AzureConfig();
        constructors[AWS_CONFIG] = arg -> new AwsConfig();
        constructors[DISCOVERY_CONFIG] = arg -> new DiscoveryConfig();
        constructors[DISCOVERY_STRATEGY_CONFIG] = arg -> new DiscoveryStrategyConfig();
        return new ArrayDataSerializableFactory(constructors);
    }
}
