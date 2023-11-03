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

package com.hazelcast.internal.config;

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.BTreeIndexConfig;
import com.hazelcast.config.BitmapIndexOptions;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.DiskTierConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MemoryTierConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.config.TieredStoreConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.internal.dynamicconfig.AddDynamicConfigOperation;
import com.hazelcast.internal.dynamicconfig.DynamicConfigPreJoinOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CONFIG_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CONFIG_DS_FACTORY_ID;

/**
 * DataSerializerHook for com.hazelcast.config classes
 */
@SuppressWarnings({"checkstyle:javadocvariable", "deprecation"})
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
    public static final int INDEX_CONFIG = 17;
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
    public static final int LIST_CONFIG = 28;
    public static final int SET_CONFIG = 29;
    public static final int EXECUTOR_CONFIG = 30;
    public static final int DURABLE_EXECUTOR_CONFIG = 31;
    public static final int SCHEDULED_EXECUTOR_CONFIG = 32;
    public static final int REPLICATED_MAP_CONFIG = 33;
    public static final int RINGBUFFER_CONFIG = 34;
    public static final int RINGBUFFER_STORE_CONFIG = 35;
    public static final int CARDINALITY_ESTIMATOR_CONFIG = 36;
    public static final int SIMPLE_CACHE_CONFIG = 37;
    public static final int SIMPLE_CACHE_CONFIG_EXPIRY_POLICY_FACTORY_CONFIG = 38;
    public static final int SIMPLE_CACHE_CONFIG_TIMED_EXPIRY_POLICY_FACTORY_CONFIG = 39;
    public static final int SIMPLE_CACHE_CONFIG_DURATION_CONFIG = 40;
    public static final int SPLIT_BRAIN_PROTECTION_CONFIG = 41;
    public static final int MAP_LISTENER_TO_ENTRY_LISTENER_ADAPTER = 42;
    public static final int EVENT_JOURNAL_CONFIG = 43;
    public static final int SPLIT_BRAIN_PROTECTION_LISTENER_CONFIG = 44;
    public static final int CACHE_PARTITION_LOST_LISTENER_CONFIG = 45;
    public static final int SIMPLE_CACHE_ENTRY_LISTENER_CONFIG = 46;
    public static final int FLAKE_ID_GENERATOR_CONFIG = 47;
    public static final int MERGE_POLICY_CONFIG = 48;
    public static final int PN_COUNTER_CONFIG = 49;
    public static final int MERKLE_TREE_CONFIG = 50;
    public static final int WAN_SYNC_CONFIG = 51;
    public static final int KUBERNETES_CONFIG = 52;
    public static final int EUREKA_CONFIG = 53;
    public static final int GCP_CONFIG = 54;
    public static final int AZURE_CONFIG = 55;
    public static final int AWS_CONFIG = 56;
    public static final int DISCOVERY_CONFIG = 57;
    public static final int DISCOVERY_STRATEGY_CONFIG = 58;
    public static final int WAN_REPLICATION_REF = 59;
    public static final int EVICTION_CONFIG = 60;
    public static final int PERMISSION_CONFIG = 61;
    public static final int BITMAP_INDEX_OPTIONS = 62;
    public static final int DATA_PERSISTENCE_CONFIG = 63;
    public static final int TIERED_STORE_CONFIG = 64;
    public static final int MEMORY_TIER_CONFIG = 65;
    public static final int DISK_TIER_CONFIG = 66;
    public static final int BTREE_INDEX_CONFIG = 67;
    public static final int DATA_CONNECTION_CONFIG = 68;
    public static final int PARTITION_ATTRIBUTE_CONFIG = 69;

    private static final int LEN = PARTITION_ATTRIBUTE_CONFIG + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[WAN_REPLICATION_CONFIG] = WanReplicationConfig::new;
        constructors[WAN_CONSUMER_CONFIG] = WanConsumerConfig::new;
        constructors[WAN_CUSTOM_PUBLISHER_CONFIG] = WanCustomPublisherConfig::new;
        constructors[WAN_BATCH_PUBLISHER_CONFIG] = WanBatchPublisherConfig::new;
        constructors[NEAR_CACHE_CONFIG] = NearCacheConfig::new;
        constructors[NEAR_CACHE_PRELOADER_CONFIG] = NearCachePreloaderConfig::new;
        constructors[ADD_DYNAMIC_CONFIG_OP] = AddDynamicConfigOperation::new;
        constructors[DYNAMIC_CONFIG_PRE_JOIN_OP] = DynamicConfigPreJoinOperation::new;
        constructors[MULTIMAP_CONFIG] = MultiMapConfig::new;
        constructors[LISTENER_CONFIG] = ListenerConfig::new;
        constructors[ENTRY_LISTENER_CONFIG] = EntryListenerConfig::new;
        constructors[MAP_CONFIG] = MapConfig::new;
        constructors[MAP_STORE_CONFIG] = MapStoreConfig::new;
        constructors[MAP_PARTITION_LOST_LISTENER_CONFIG] = MapPartitionLostListenerConfig::new;
        constructors[INDEX_CONFIG] = IndexConfig::new;
        constructors[MAP_ATTRIBUTE_CONFIG] = AttributeConfig::new;
        constructors[QUERY_CACHE_CONFIG] = QueryCacheConfig::new;
        constructors[PREDICATE_CONFIG] = PredicateConfig::new;
        constructors[PARTITION_STRATEGY_CONFIG] = PartitioningStrategyConfig::new;
        constructors[HOT_RESTART_CONFIG] = HotRestartConfig::new;
        constructors[TOPIC_CONFIG] = TopicConfig::new;
        constructors[RELIABLE_TOPIC_CONFIG] = ReliableTopicConfig::new;
        constructors[ITEM_LISTENER_CONFIG] = ItemListenerConfig::new;
        constructors[QUEUE_STORE_CONFIG] = QueueStoreConfig::new;
        constructors[QUEUE_CONFIG] = QueueConfig::new;
        constructors[LIST_CONFIG] = ListConfig::new;
        constructors[SET_CONFIG] = SetConfig::new;
        constructors[EXECUTOR_CONFIG] = ExecutorConfig::new;
        constructors[DURABLE_EXECUTOR_CONFIG] = DurableExecutorConfig::new;
        constructors[SCHEDULED_EXECUTOR_CONFIG] = ScheduledExecutorConfig::new;
        constructors[REPLICATED_MAP_CONFIG] = ReplicatedMapConfig::new;
        constructors[RINGBUFFER_CONFIG] = RingbufferConfig::new;
        constructors[RINGBUFFER_STORE_CONFIG] = RingbufferStoreConfig::new;
        constructors[CARDINALITY_ESTIMATOR_CONFIG] = CardinalityEstimatorConfig::new;
        constructors[SIMPLE_CACHE_CONFIG] = CacheSimpleConfig::new;
        constructors[SIMPLE_CACHE_CONFIG_EXPIRY_POLICY_FACTORY_CONFIG] =
                ExpiryPolicyFactoryConfig::new;
        constructors[SIMPLE_CACHE_CONFIG_TIMED_EXPIRY_POLICY_FACTORY_CONFIG] =
                TimedExpiryPolicyFactoryConfig::new;
        constructors[SIMPLE_CACHE_CONFIG_DURATION_CONFIG] =
                DurationConfig::new;
        constructors[SPLIT_BRAIN_PROTECTION_CONFIG] = SplitBrainProtectionConfig::new;
        constructors[EVENT_JOURNAL_CONFIG] = EventJournalConfig::new;
        constructors[SPLIT_BRAIN_PROTECTION_LISTENER_CONFIG] = SplitBrainProtectionListenerConfig::new;
        constructors[CACHE_PARTITION_LOST_LISTENER_CONFIG] = CachePartitionLostListenerConfig::new;
        constructors[SIMPLE_CACHE_ENTRY_LISTENER_CONFIG] = CacheSimpleEntryListenerConfig::new;
        constructors[FLAKE_ID_GENERATOR_CONFIG] = FlakeIdGeneratorConfig::new;
        constructors[MERGE_POLICY_CONFIG] = MergePolicyConfig::new;
        constructors[PN_COUNTER_CONFIG] = PNCounterConfig::new;
        constructors[MERKLE_TREE_CONFIG] = MerkleTreeConfig::new;
        constructors[WAN_SYNC_CONFIG] = WanSyncConfig::new;
        constructors[KUBERNETES_CONFIG] = KubernetesConfig::new;
        constructors[EUREKA_CONFIG] = EurekaConfig::new;
        constructors[GCP_CONFIG] = GcpConfig::new;
        constructors[AZURE_CONFIG] = AzureConfig::new;
        constructors[AWS_CONFIG] = AwsConfig::new;
        constructors[DISCOVERY_CONFIG] = DiscoveryConfig::new;
        constructors[DISCOVERY_STRATEGY_CONFIG] = DiscoveryStrategyConfig::new;
        constructors[WAN_REPLICATION_REF] = WanReplicationRef::new;
        constructors[EVICTION_CONFIG] = EvictionConfig::new;
        constructors[PERMISSION_CONFIG] = PermissionConfig::new;
        constructors[BITMAP_INDEX_OPTIONS] = BitmapIndexOptions::new;
        constructors[DATA_PERSISTENCE_CONFIG] = DataPersistenceConfig::new;
        constructors[TIERED_STORE_CONFIG] = TieredStoreConfig::new;
        constructors[MEMORY_TIER_CONFIG] = MemoryTierConfig::new;
        constructors[DISK_TIER_CONFIG] = DiskTierConfig::new;
        constructors[BTREE_INDEX_CONFIG] = BTreeIndexConfig::new;
        constructors[DATA_CONNECTION_CONFIG] = DataConnectionConfig::new;
        constructors[PARTITION_ATTRIBUTE_CONFIG] = PartitioningAttributeConfig::new;

        return new ArrayDataSerializableFactory(constructors);
    }
}
