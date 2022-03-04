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

import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.DiskTierConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MemoryTierConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
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
import com.hazelcast.config.TieredStoreConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.memory.Capacity;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.internal.config.AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom;
import static com.hazelcast.internal.config.ConfigSections.LICENSE_KEY;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.classNameOrImplClass;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static java.lang.Boolean.TRUE;

/**
 * YAML counterpart of {@link ConfigXmlGenerator}. Note that this class isn't
 * complete like its counterpart. However, since dynamic configuration
 * persistence depends on this class, dynamically configurable configurations
 * must be added to this class.
 */
@SuppressWarnings({"checkstyle:MethodCount", "checkstyle:ClassFanOutComplexity"})
public class DynamicConfigYamlGenerator {

    private static final int INDENT = 2;

    String generate(Config config) {
        Map<String, Object> document = new LinkedHashMap<>();
        Map<String, Object> root = new LinkedHashMap<>();
        document.put("hazelcast", root);

        root.put("cluster-name", config.getClusterName());

        licenseKeyYamlGenerator(root, config);
        mapYamlGenerator(root, config);
        cacheYamlGenerator(root, config);
        queueYamlGenerator(root, config);
        listYamlGenerator(root, config);
        setYamlGenerator(root, config);
        multiMapYamlGenerator(root, config);
        replicatedMapYamlGenerator(root, config);
        ringbufferYamlGenerator(root, config);
        topicYamlGenerator(root, config);
        reliableTopicYamlGenerator(root, config);
        executorYamlGenerator(root, config);
        durableExecutorYamlGenerator(root, config);
        scheduledExecutorYamlGenerator(root, config);
        cardinalityEstimatorYamlGenerator(root, config);
        flakeIdGeneratorYamlGenerator(root, config);
        pnCounterYamlGenerator(root, config);
        wanReplicationYamlGenerator(root, config);

        DumpSettings dumpSettings = DumpSettings.builder()
                .setDefaultFlowStyle(FlowStyle.BLOCK)
                .setIndicatorIndent(INDENT - 2)
                .setIndent(INDENT)
                .build();
        Dump dump = new Dump(dumpSettings);
        return dump.dumpToString(document);
    }

    public static void licenseKeyYamlGenerator(Map<String, Object> parent, Config config) {
        addNonNullToMap(parent, LICENSE_KEY.getName(), config.getLicenseKey());
    }

    @SuppressWarnings("checkstyle:MethodLength")
    public static void mapYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getMapConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (MapConfig subConfigAsObject : config.getMapConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            String cacheDeserializedValues = subConfigAsObject.getCacheDeserializedValues() != null
                    ? subConfigAsObject.getCacheDeserializedValues().name().replaceAll("_", "-")
                    : null;

            addNonNullToMap(subConfigAsMap, "in-memory-format",
                    subConfigAsObject.getInMemoryFormat().name());
            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "per-entry-stats-enabled",
                    subConfigAsObject.isPerEntryStatsEnabled());
            addNonNullToMap(subConfigAsMap, "cache-deserialized-values",
                    cacheDeserializedValues);
            addNonNullToMap(subConfigAsMap, "backup-count",
                    subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count",
                    subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "time-to-live-seconds",
                    subConfigAsObject.getTimeToLiveSeconds());
            addNonNullToMap(subConfigAsMap, "max-idle-seconds",
                    subConfigAsObject.getMaxIdleSeconds());
            addNonNullToMap(subConfigAsMap, "merge-policy",
                    getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "read-backup-data",
                    subConfigAsObject.isReadBackupData());
            addNonNullToMap(subConfigAsMap, "metadata-policy",
                    subConfigAsObject.getMetadataPolicy().name());
            addNonNullToMap(subConfigAsMap, "eviction",
                    getEvictionConfigAsMap(subConfigAsObject.getEvictionConfig()));
            addNonNullToMap(subConfigAsMap, "merkle-tree",
                    getMerkleTreeConfigAsMap(subConfigAsObject.getMerkleTreeConfig()));
            addNonNullToMap(subConfigAsMap, "event-journal",
                    getEventJournalConfigAsMap(subConfigAsObject.getEventJournalConfig()));
            addNonNullToMap(subConfigAsMap, "data-persistence",
                    getDataPersistenceConfigAsMap(subConfigAsObject.getDataPersistenceConfig()));
            addNonNullToMap(subConfigAsMap, "map-store",
                    getMapStoreConfigAsMap(subConfigAsObject.getMapStoreConfig()));
            addNonNullToMap(subConfigAsMap, "near-cache",
                    getNearCacheConfigAsMap(subConfigAsObject.getNearCacheConfig()));
            addNonNullToMap(subConfigAsMap, "wan-replication-ref",
                    getWanReplicationRefAsMap(subConfigAsObject.getWanReplicationRef(), true));
            addNonNullToMap(subConfigAsMap, "near-cache",
                    getNearCacheConfigAsMap(subConfigAsObject.getNearCacheConfig()));
            addNonNullToMap(subConfigAsMap, "indexes",
                    getIndexConfigsAsList(subConfigAsObject.getIndexConfigs()));
            addNonNullToMap(subConfigAsMap, "attributes",
                    getAttributeConfigsAsMap(subConfigAsObject.getAttributeConfigs()));
            addNonNullToMap(subConfigAsMap, "entry-listeners",
                    getEntryListenerConfigsAsList(subConfigAsObject.getEntryListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "partition-lost-listeners",
                    getListenerConfigsAsList(subConfigAsObject.getPartitionLostListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "partition-strategy",
                    getPartitioningStrategyAsString(subConfigAsObject.getPartitioningStrategyConfig()));
            addNonNullToMap(subConfigAsMap, "query-caches",
                    getQueryCacheConfigsAsMap(subConfigAsObject.getQueryCacheConfigs()));
            addNonNullToMap(subConfigAsMap, "tiered-store",
                    getTieredStoreConfigAsMap(subConfigAsObject.getTieredStoreConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("map", child);
    }

    public static void cacheYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getCacheConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (CacheSimpleConfig subConfigAsObject : config.getCacheConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "key-type",
                    wrapObjectWithMap("class-name", subConfigAsObject.getKeyType()));
            addNonNullToMap(subConfigAsMap, "value-type"
                    , wrapObjectWithMap("class-name", subConfigAsObject.getValueType()));
            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "management-enabled",
                    subConfigAsObject.isManagementEnabled());
            addNonNullToMap(subConfigAsMap, "read-through",
                    subConfigAsObject.isReadThrough());
            addNonNullToMap(subConfigAsMap, "write-through",
                    subConfigAsObject.isWriteThrough());
            addNonNullToMap(subConfigAsMap, "cache-loader-factory",
                    wrapObjectWithMap("class-name", subConfigAsObject.getCacheLoaderFactory()));
            addNonNullToMap(subConfigAsMap, "cache-writer-factory",
                    wrapObjectWithMap("class-name", subConfigAsObject.getCacheWriterFactory()));
            addNonNullToMap(subConfigAsMap, "cache-loader",
                    wrapObjectWithMap("class-name", subConfigAsObject.getCacheLoader()));
            addNonNullToMap(subConfigAsMap, "cache-writer",
                    wrapObjectWithMap("class-name", subConfigAsObject.getCacheWriter()));
            addNonNullToMap(subConfigAsMap, "expiry-policy-factory",
                    getExpiryPolicyFactoryConfigAsMap(subConfigAsObject.getExpiryPolicyFactoryConfig()));
            addNonNullToMap(subConfigAsMap, "cache-entry-listeners",
                    getCacheSimpleEntryListenerConfigsAsList(subConfigAsObject.getCacheEntryListeners()));
            addNonNullToMap(subConfigAsMap, "in-memory-format",
                    subConfigAsObject.getInMemoryFormat().name());
            addNonNullToMap(subConfigAsMap, "backup-count",
                    subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count",
                    subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "eviction",
                    getEvictionConfigAsMap(subConfigAsObject.getEvictionConfig()));
            addNonNullToMap(subConfigAsMap, "wan-replication-ref",
                    getWanReplicationRefAsMap(subConfigAsObject.getWanReplicationRef(), false));
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "partition-lost-listeners",
                    getListenerConfigsAsList(subConfigAsObject.getPartitionLostListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "merge-policy",
                    getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));
            addNonNullToMap(subConfigAsMap, "event-journal",
                    getEventJournalConfigAsMap(subConfigAsObject.getEventJournalConfig()));
            addNonNullToMap(subConfigAsMap, "data-persistence",
                    getDataPersistenceConfigAsMap(subConfigAsObject.getDataPersistenceConfig()));
            addNonNullToMap(subConfigAsMap, "merkle-tree",
                    getMerkleTreeConfigAsMap(subConfigAsObject.getMerkleTreeConfig()));
            addNonNullToMap(subConfigAsMap, "disable-per-entry-invalidation-events",
                    subConfigAsObject.isDisablePerEntryInvalidationEvents());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("cache", child);
    }

    public static void queueYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getQueueConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (QueueConfig subConfigAsObject : config.getQueueConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "priority-comparator-class-name",
                    subConfigAsObject.getPriorityComparatorClassName());
            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "max-size",
                    subConfigAsObject.getMaxSize());
            addNonNullToMap(subConfigAsMap, "backup-count",
                    subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count",
                    subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "empty-queue-ttl",
                    subConfigAsObject.getEmptyQueueTtl());
            addNonNullToMap(subConfigAsMap, "item-listeners",
                    getItemListenerConfigsAsList(subConfigAsObject.getItemListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "queue-store",
                    getQueueStoreConfigAsMap(subConfigAsObject.getQueueStoreConfig()));
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy",
                    getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("queue", child);
    }

    public static void listYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getListConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ListConfig subConfigAsObject : config.getListConfigs().values()) {
            child.put(subConfigAsObject.getName(), getCollectionConfigAsMap(subConfigAsObject));
        }

        parent.put("list", child);
    }

    public static void setYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getSetConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (SetConfig subConfigAsObject : config.getSetConfigs().values()) {
            child.put(subConfigAsObject.getName(), getCollectionConfigAsMap(subConfigAsObject));
        }

        parent.put("set", child);
    }

    public static void multiMapYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getMultiMapConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (MultiMapConfig subConfigAsObject : config.getMultiMapConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "backup-count",
                    subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count",
                    subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "binary",
                    subConfigAsObject.isBinary());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "value-collection-type",
                    subConfigAsObject.getValueCollectionType().name());
            addNonNullToMap(subConfigAsMap, "entry-listeners",
                    getEntryListenerConfigsAsList(subConfigAsObject.getEntryListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "merge-policy",
                    getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("multimap", child);
    }

    public static void replicatedMapYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getReplicatedMapConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ReplicatedMapConfig subConfigAsObject : config.getReplicatedMapConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "in-memory-format",
                    subConfigAsObject.getInMemoryFormat().name());
            addNonNullToMap(subConfigAsMap, "async-fillup",
                    subConfigAsObject.isAsyncFillup());
            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy",
                    getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));
            addNonNullToMap(subConfigAsMap, "entry-listeners",
                    getEntryListenerConfigsAsList(subConfigAsObject.getListenerConfigs()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("replicatedmap", child);
    }

    public static void ringbufferYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getRingbufferConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (RingbufferConfig subConfigAsObject : config.getRingbufferConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "capacity",
                    subConfigAsObject.getCapacity());
            addNonNullToMap(subConfigAsMap, "time-to-live-seconds",
                    subConfigAsObject.getTimeToLiveSeconds());
            addNonNullToMap(subConfigAsMap, "backup-count",
                    subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count",
                    subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "in-memory-format",
                    subConfigAsObject.getInMemoryFormat().name());
            addNonNullToMap(subConfigAsMap, "ringbuffer-store",
                    getRingbufferStoreConfigAsMap(subConfigAsObject.getRingbufferStoreConfig()));
            addNonNullToMap(subConfigAsMap, "merge-policy",
                    getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("ringbuffer", child);
    }

    public static void topicYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getTopicConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (TopicConfig subConfigAsObject : config.getTopicConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "global-ordering-enabled",
                    subConfigAsObject.isGlobalOrderingEnabled());
            addNonNullToMap(subConfigAsMap, "message-listeners",
                    getListenerConfigsAsList(subConfigAsObject.getMessageListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "multi-threading-enabled",
                    subConfigAsObject.isMultiThreadingEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("topic", child);
    }

    public static void reliableTopicYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getReliableTopicConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ReliableTopicConfig subConfigAsObject : config.getReliableTopicConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "read-batch-size",
                    subConfigAsObject.getReadBatchSize());
            addNonNullToMap(subConfigAsMap, "topic-overload-policy",
                    subConfigAsObject.getTopicOverloadPolicy().name());
            addNonNullToMap(subConfigAsMap, "message-listeners",
                    getListenerConfigsAsList(subConfigAsObject.getMessageListenerConfigs()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("reliable-topic", child);
    }

    public static void executorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getExecutorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ExecutorConfig subConfigAsObject : config.getExecutorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "pool-size",
                    subConfigAsObject.getPoolSize());
            addNonNullToMap(subConfigAsMap, "queue-capacity",
                    subConfigAsObject.getQueueCapacity());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("executor-service", child);
    }

    public static void durableExecutorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getDurableExecutorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (DurableExecutorConfig subConfigAsObject : config.getDurableExecutorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "pool-size",
                    subConfigAsObject.getPoolSize());
            addNonNullToMap(subConfigAsMap, "durability",
                    subConfigAsObject.getDurability());
            addNonNullToMap(subConfigAsMap, "capacity",
                    subConfigAsObject.getCapacity());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("durable-executor-service", child);
    }

    public static void scheduledExecutorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getScheduledExecutorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ScheduledExecutorConfig subConfigAsObject : config.getScheduledExecutorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "pool-size",
                    subConfigAsObject.getPoolSize());
            addNonNullToMap(subConfigAsMap, "durability",
                    subConfigAsObject.getDurability());
            addNonNullToMap(subConfigAsMap, "capacity",
                    subConfigAsObject.getCapacity());
            addNonNullToMap(subConfigAsMap, "capacity-policy",
                    subConfigAsObject.getCapacityPolicy().name());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy",
                    getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));
            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("scheduled-executor-service", child);
    }

    public static void cardinalityEstimatorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getCardinalityEstimatorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (CardinalityEstimatorConfig subConfigAsObject : config.getCardinalityEstimatorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "backup-count",
                    subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count",
                    subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy",
                    getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("cardinality-estimator", child);
    }

    public static void flakeIdGeneratorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getFlakeIdGeneratorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (FlakeIdGeneratorConfig subConfigAsObject : config.getFlakeIdGeneratorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "prefetch-count",
                    subConfigAsObject.getPrefetchCount());
            addNonNullToMap(subConfigAsMap, "prefetch-validity-millis",
                    subConfigAsObject.getPrefetchValidityMillis());
            addNonNullToMap(subConfigAsMap, "epoch-start",
                    subConfigAsObject.getEpochStart());
            addNonNullToMap(subConfigAsMap, "node-id-offset",
                    subConfigAsObject.getNodeIdOffset());
            addNonNullToMap(subConfigAsMap, "bits-sequence",
                    subConfigAsObject.getBitsSequence());
            addNonNullToMap(subConfigAsMap, "bits-node-id",
                    subConfigAsObject.getBitsNodeId());
            addNonNullToMap(subConfigAsMap, "allowed-future-millis",
                    subConfigAsObject.getAllowedFutureMillis());
            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("flake-id-generator", child);
    }

    public static void pnCounterYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getPNCounterConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (PNCounterConfig subConfigAsObject : config.getPNCounterConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "replica-count",
                    subConfigAsObject.getReplicaCount());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                    subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "statistics-enabled",
                    subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("pn-counter", child);
    }

    public static void wanReplicationYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getWanReplicationConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (WanReplicationConfig subConfigAsObject : config.getWanReplicationConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "batch-publisher",
                    getWanBatchPublisherConfigsAsMap(subConfigAsObject.getBatchPublisherConfigs()));
            addNonNullToMap(subConfigAsMap, "custom-publisher",
                    getWanCustomPublisherConfigsAsMap(subConfigAsObject.getCustomPublisherConfigs()));
            addNonNullToMap(subConfigAsMap, "consumer",
                    getWanConsumerConfigsAsMap(subConfigAsObject.getConsumerConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("wan-replication", child);
    }

    private static Map<String, Object> getWanConsumerConfigsAsMap(WanConsumerConfig wanConsumerConfig) {
        if (wanConsumerConfig == null) {
            return null;
        }

        Map<String, Object> wanConsumerConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(wanConsumerConfigAsMap, "class-name",
                classNameOrImplClass(wanConsumerConfig.getClassName(), wanConsumerConfig.getImplementation()));
        addNonNullToMap(wanConsumerConfigAsMap, "persist-wan-replicated-data",
                wanConsumerConfig.isPersistWanReplicatedData());
        addNonNullToMap(wanConsumerConfigAsMap, "properties",
                getPropertiesAsMap(wanConsumerConfig.getProperties()));

        return wanConsumerConfigAsMap;
    }

    private static Map<String, Object> getWanCustomPublisherConfigsAsMap(
            List<WanCustomPublisherConfig> wanCustomPublisherConfigs
    ) {
        if (wanCustomPublisherConfigs == null || wanCustomPublisherConfigs.isEmpty()) {
            return null;
        }

        Map<String, Object> wanBatchPublisherConfigsAsMap = new LinkedHashMap<>();
        for (WanCustomPublisherConfig wanCustomPublisherConfig : wanCustomPublisherConfigs) {
            Map<String, Object> wanCustomPublisherConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(wanCustomPublisherConfigAsMap, "class-name",
                    wanCustomPublisherConfig.getClassName());
            addNonNullToMap(wanCustomPublisherConfigAsMap, "properties",
                    getPropertiesAsMap(wanCustomPublisherConfig.getProperties()));

            wanBatchPublisherConfigsAsMap.put(wanCustomPublisherConfig.getPublisherId(), wanCustomPublisherConfigAsMap);
        }

        return wanBatchPublisherConfigsAsMap;
    }

    @SuppressWarnings("checkstyle:MethodLength")
    private static Map<String, Object> getWanBatchPublisherConfigsAsMap(
            List<WanBatchPublisherConfig> wanBatchPublisherConfigs
    ) {
        if (wanBatchPublisherConfigs == null || wanBatchPublisherConfigs.isEmpty()) {
            return null;
        }

        Map<String, Object> wanBatchPublisherConfigsAsMap = new LinkedHashMap<>();

        for (WanBatchPublisherConfig wanBatchPublisherConfig : wanBatchPublisherConfigs) {
            Map<String, Object> wanBatchPublisherConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(wanBatchPublisherConfigAsMap, "cluster-name",
                    wanBatchPublisherConfig.getClusterName());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "batch-size",
                    wanBatchPublisherConfig.getBatchSize());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "batch-max-delay-millis",
                    wanBatchPublisherConfig.getBatchMaxDelayMillis());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "response-timeout-millis",
                    wanBatchPublisherConfig.getResponseTimeoutMillis());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "acknowledge-type",
                    wanBatchPublisherConfig.getAcknowledgeType().name());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "initial-publisher-state",
                    wanBatchPublisherConfig.getInitialPublisherState().name());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "snapshot-enabled",
                    wanBatchPublisherConfig.isSnapshotEnabled());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "idle-max-park-ns",
                    wanBatchPublisherConfig.getIdleMaxParkNs());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "idle-min-park-ns",
                    wanBatchPublisherConfig.getIdleMinParkNs());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "max-concurrent-invocations",
                    wanBatchPublisherConfig.getMaxConcurrentInvocations());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "discovery-period-seconds",
                    wanBatchPublisherConfig.getDiscoveryPeriodSeconds());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "use-endpoint-private-address",
                    wanBatchPublisherConfig.isUseEndpointPrivateAddress());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "queue-full-behavior",
                    wanBatchPublisherConfig.getQueueFullBehavior().name());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "max-target-endpoints",
                    wanBatchPublisherConfig.getMaxTargetEndpoints());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "queue-capacity",
                    wanBatchPublisherConfig.getQueueCapacity());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "properties",
                    getPropertiesAsMap(wanBatchPublisherConfig.getProperties()));
            addNonNullToMap(wanBatchPublisherConfigAsMap, "target-endpoints",
                    wanBatchPublisherConfig.getTargetEndpoints());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "endpoint",
                    wanBatchPublisherConfig.getEndpoint());
            addNonNullToMap(wanBatchPublisherConfigAsMap, "sync",
                    wrapObjectWithMap("consistency-check-strategy",
                            wanBatchPublisherConfig.getSyncConfig().getConsistencyCheckStrategy().name()));
            addNonNullToMap(wanBatchPublisherConfigAsMap, "discovery-strategies",
                    getDiscoveryConfigAsMap(wanBatchPublisherConfig.getDiscoveryConfig()));
            addNonNullToMap(wanBatchPublisherConfigAsMap, "discovery-strategies",
                    getDiscoveryConfigAsMap(wanBatchPublisherConfig.getDiscoveryConfig()));

            for (
                    AliasedDiscoveryConfig<?> aliasedDiscoveryConfig
                    :
                    aliasedDiscoveryConfigsFrom(wanBatchPublisherConfig)
            ) {
                addNonNullToMap(wanBatchPublisherConfigAsMap, aliasedDiscoveryConfig.getTag(),
                        getAliasedDiscoveryConfigAsMap(aliasedDiscoveryConfig));
            }

            wanBatchPublisherConfigsAsMap.put(wanBatchPublisherConfig.getPublisherId(), wanBatchPublisherConfigAsMap);
        }

        return wanBatchPublisherConfigsAsMap;
    }

    private static Map<String, Object> getAliasedDiscoveryConfigAsMap(
            AliasedDiscoveryConfig<?> aliasedDiscoveryConfig
    ) {
        if (aliasedDiscoveryConfig == null) {
            return null;
        }

        Map<String, Object> aliasedDiscoveryConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(aliasedDiscoveryConfigAsMap, "enabled", aliasedDiscoveryConfig.isEnabled());
        addNonNullToMap(aliasedDiscoveryConfigAsMap, "use-public-ip", aliasedDiscoveryConfig.isUsePublicIp());
        for (String key : aliasedDiscoveryConfig.getProperties().keySet()) {
            addNonNullToMap(aliasedDiscoveryConfigAsMap, key, aliasedDiscoveryConfig.getProperties().get(key));
        }

        return aliasedDiscoveryConfigAsMap;
    }

    private static Map<String, Object> getDiscoveryConfigAsMap(DiscoveryConfig discoveryConfig) {
        if (discoveryConfig == null) {
            return null;
        }

        Map<String, Object> discoveryConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(discoveryConfigAsMap, "node-filter",
                wrapObjectWithMap("class",
                        classNameOrImplClass(discoveryConfig.getNodeFilterClass(), discoveryConfig.getNodeFilter())));

        List<Map<String, Object>> discoveryStrategyConfigsAsList = new LinkedList<>();
        for (DiscoveryStrategyConfig discoveryStrategyConfig : discoveryConfig.getDiscoveryStrategyConfigs()) {
            Map<String, Object> discoveryStrategyConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(discoveryStrategyConfigAsMap, "enabled", "true");
            addNonNullToMap(discoveryStrategyConfigAsMap, "class",
                    classNameOrImplClass(discoveryStrategyConfig.getClassName(),
                            discoveryStrategyConfig.getDiscoveryStrategyFactory()));
            addNonNullToMap(discoveryStrategyConfigAsMap, "properties",
                    getPropertiesAsMap(discoveryStrategyConfig.getProperties()));

            discoveryStrategyConfigsAsList.add(discoveryStrategyConfigAsMap);
        }

        addNonNullToMap(discoveryConfigAsMap, "discovery-strategies", discoveryStrategyConfigsAsList);

        return discoveryConfigAsMap;
    }

    private static Map<String, Object> getTieredStoreConfigAsMap(TieredStoreConfig tieredStoreConfig) {
        if (tieredStoreConfig == null) {
            return null;
        }
        Map<String, Object> tieredStoreConfigAsMap = new LinkedHashMap<>();
        addNonNullToMap(tieredStoreConfigAsMap, "enabled",
                tieredStoreConfig.isEnabled());
        addNonNullToMap(tieredStoreConfigAsMap, "memory-tier",
                getMemoryTierConfigAsMap(tieredStoreConfig.getMemoryTierConfig()));
        addNonNullToMap(tieredStoreConfigAsMap, "disk-tier",
                getDiskTierConfigAsMap(tieredStoreConfig.getDiskTierConfig()));

        return tieredStoreConfigAsMap;
    }

    private static Map<String, Object> getMemoryTierConfigAsMap(MemoryTierConfig memoryTierConfig) {
        if (memoryTierConfig == null) {
            return null;
        }
        Map<String, Object> memoryTierConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(memoryTierConfigAsMap, "capacity", getCapacityAsMap(memoryTierConfig.getCapacity()));

        return memoryTierConfigAsMap;
    }

    private static Map<String, Object> getCapacityAsMap(Capacity capacity) {
        if (capacity == null) {
            return null;
        }
        Map<String, Object> capacityAsMap = new LinkedHashMap<>();
        addNonNullToMap(capacityAsMap, "unit", capacity.getUnit().toString());
        addNonNullToMap(capacityAsMap, "value", "" + capacity.getValue());

        return capacityAsMap;
    }

    private static Map<String, Object> getDiskTierConfigAsMap(DiskTierConfig diskTierConfig) {
        if (diskTierConfig == null) {
            return null;
        }
        Map<String, Object> diskTierConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(diskTierConfigAsMap, "enabled", diskTierConfig.isEnabled());
        addNonNullToMap(diskTierConfigAsMap, "device-name", diskTierConfig.getDeviceName());

        return diskTierConfigAsMap;
    }

    private static String getPartitioningStrategyAsString(PartitioningStrategyConfig partitioningStrategyConfig) {
        if (partitioningStrategyConfig == null) {
            return null;
        }

        return classNameOrImplClass(partitioningStrategyConfig.getPartitioningStrategyClass(),
                partitioningStrategyConfig.getPartitioningStrategy());
    }

    private static Map<String, Object> getPredicateConfigAsMap(PredicateConfig predicateConfig) {
        if (predicateConfig == null) {
            return null;
        }

        Map<String, Object> predicateConfigAsMap = new LinkedHashMap<>();

        String type = predicateConfig.getClassName() != null
                ? "class-name"
                : "sql";

        String content = predicateConfig.getClassName() != null
                ? predicateConfig.getClassName()
                : predicateConfig.getSql();

        addNonNullToMap(predicateConfigAsMap, type, content);

        return predicateConfigAsMap;
    }

    private static Map<String, Object> getQueryCacheConfigsAsMap(List<QueryCacheConfig> queryCacheConfigs) {
        if (queryCacheConfigs == null || queryCacheConfigs.isEmpty()) {
            return null;
        }

        Map<String, Object> queryCacheConfigsAsMap = new LinkedHashMap<>();

        for (QueryCacheConfig queryCacheConfig : queryCacheConfigs) {
            Map<String, Object> queryCacheConfigAsMap = new LinkedHashMap<>();
            addNonNullToMap(queryCacheConfigAsMap, "include-value",
                    queryCacheConfig.isIncludeValue());
            addNonNullToMap(queryCacheConfigAsMap, "in-memory-format",
                    queryCacheConfig.getInMemoryFormat().name());
            addNonNullToMap(queryCacheConfigAsMap, "populate",
                    queryCacheConfig.isPopulate());
            addNonNullToMap(queryCacheConfigAsMap, "coalesce",
                    queryCacheConfig.isCoalesce());
            addNonNullToMap(queryCacheConfigAsMap, "delay-seconds",
                    queryCacheConfig.getDelaySeconds());
            addNonNullToMap(queryCacheConfigAsMap, "batch-size",
                    queryCacheConfig.getBatchSize());
            addNonNullToMap(queryCacheConfigAsMap, "buffer-size",
                    queryCacheConfig.getBufferSize());
            addNonNullToMap(queryCacheConfigAsMap, "eviction",
                    getEvictionConfigAsMap(queryCacheConfig.getEvictionConfig()));
            addNonNullToMap(queryCacheConfigAsMap, "indexes",
                    getIndexConfigsAsList(queryCacheConfig.getIndexConfigs()));
            addNonNullToMap(queryCacheConfigAsMap, "predicate",
                    getPredicateConfigAsMap(queryCacheConfig.getPredicateConfig()));
            addNonNullToMap(queryCacheConfigAsMap, "entry-listeners",
                    getEntryListenerConfigsAsList(queryCacheConfig.getEntryListenerConfigs()));
            addNonNullToMap(queryCacheConfigAsMap, "serialize-keys",
                    queryCacheConfig.isSerializeKeys());

            queryCacheConfigsAsMap.put(queryCacheConfig.getName(), queryCacheConfigAsMap);
        }

        return queryCacheConfigsAsMap;
    }

    private static Map<String, Object> getAttributeConfigsAsMap(List<AttributeConfig> attributeConfigs) {
        if (attributeConfigs == null || attributeConfigs.isEmpty()) {
            return null;
        }

        Map<String, Object> attributeConfigsAsMap = new LinkedHashMap<>();

        for (AttributeConfig attributeConfig : attributeConfigs) {
            addNonNullToMap(attributeConfigsAsMap, attributeConfig.getName(),
                    wrapObjectWithMap("extractor-class-name", attributeConfig.getExtractorClassName()));
        }

        return attributeConfigsAsMap;
    }

    private static List<Map<String, Object>> getIndexConfigsAsList(List<IndexConfig> indexConfigs) {
        if (indexConfigs == null || indexConfigs.isEmpty()) {
            return null;
        }

        List<Map<String, Object>> indexConfigsAsList = new LinkedList<>();

        for (IndexConfig indexConfig : indexConfigs) {
            Map<String, Object> indexConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(indexConfigAsMap, "name", indexConfig.getName());
            addNonNullToMap(indexConfigAsMap, "type", indexConfig.getType().name());
            addNonNullToMap(indexConfigAsMap, "attributes", indexConfig.getAttributes());

            if (indexConfig.getType() == IndexType.BITMAP) {
                Map<String, Object> bitmapIndexOptionsAsMap = new LinkedHashMap<>();
                addNonNullToMap(bitmapIndexOptionsAsMap, "unique-key",
                        indexConfig.getBitmapIndexOptions().getUniqueKey());
                addNonNullToMap(bitmapIndexOptionsAsMap, "unique-key-transformation",
                        indexConfig.getBitmapIndexOptions().getUniqueKeyTransformation().name());

                indexConfigAsMap.put("bitmap-index-options", bitmapIndexOptionsAsMap);
            }

            addNonNullToList(indexConfigsAsList, indexConfigAsMap);
        }

        return indexConfigsAsList;
    }

    private static Map<String, Object> getNearCacheConfigAsMap(NearCacheConfig nearCacheConfig) {
        if (nearCacheConfig == null) {
            return null;
        }

        Map<String, Object> nearCacheConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(nearCacheConfigAsMap, "name",
                nearCacheConfig.getName());
        addNonNullToMap(nearCacheConfigAsMap, "in-memory-format",
                nearCacheConfig.getInMemoryFormat().name());
        addNonNullToMap(nearCacheConfigAsMap, "invalidate-on-change",
                nearCacheConfig.isInvalidateOnChange());
        addNonNullToMap(nearCacheConfigAsMap, "time-to-live-seconds",
                nearCacheConfig.getTimeToLiveSeconds());
        addNonNullToMap(nearCacheConfigAsMap, "max-idle-seconds",
                nearCacheConfig.getMaxIdleSeconds());
        addNonNullToMap(nearCacheConfigAsMap, "serialize-keys",
                nearCacheConfig.isSerializeKeys());
        addNonNullToMap(nearCacheConfigAsMap, "cache-local-entries",
                nearCacheConfig.isCacheLocalEntries());
        addNonNullToMap(nearCacheConfigAsMap, "eviction",
                getEvictionConfigAsMap(nearCacheConfig.getEvictionConfig()));

        return nearCacheConfigAsMap;
    }

    private static Map<String, Object> getMerkleTreeConfigAsMap(MerkleTreeConfig merkleTreeConfig) {
        if (merkleTreeConfig == null || merkleTreeConfig.getEnabled() == null) {
            return null;
        }

        Map<String, Object> merkleTreeConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(merkleTreeConfigAsMap, "enabled", TRUE.equals(merkleTreeConfig.getEnabled()));
        addNonNullToMap(merkleTreeConfigAsMap, "depth", merkleTreeConfig.getDepth());

        return merkleTreeConfigAsMap;
    }

    private static Map<String, Object> getDataPersistenceConfigAsMap(DataPersistenceConfig dataPersistenceConfig) {
        if (dataPersistenceConfig == null) {
            return null;
        }

        Map<String, Object> dataPersistenceConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(dataPersistenceConfigAsMap, "enabled", dataPersistenceConfig.isEnabled());
        addNonNullToMap(dataPersistenceConfigAsMap, "fsync", dataPersistenceConfig.isFsync());

        return dataPersistenceConfigAsMap;
    }

    private static Map<String, Object> getEventJournalConfigAsMap(EventJournalConfig eventJournalConfig) {
        if (eventJournalConfig == null) {
            return null;
        }

        Map<String, Object> eventJournalConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(eventJournalConfigAsMap, "enabled",
                eventJournalConfig.isEnabled());
        addNonNullToMap(eventJournalConfigAsMap, "capacity",
                eventJournalConfig.getCapacity());
        addNonNullToMap(eventJournalConfigAsMap, "time-to-live-seconds",
                eventJournalConfig.getTimeToLiveSeconds());

        return eventJournalConfigAsMap;
    }

    private static Map<String, Object> getWanReplicationRefAsMap(WanReplicationRef wanReplicationRef, boolean isMap) {
        if (wanReplicationRef == null) {
            return null;
        }

        Map<String, Object> wanReplicationRefAsMap = new LinkedHashMap<>();

        addNonNullToMap(wanReplicationRefAsMap, "merge-policy-class-name",
                wanReplicationRef.getMergePolicyClassName());
        addNonNullToMap(wanReplicationRefAsMap, "republishing-enabled",
                wanReplicationRef.isRepublishingEnabled());
        addNonNullToMap(wanReplicationRefAsMap, "filters",
                wanReplicationRef.getFilters());

        if (isMap) {
            return wrapObjectWithMap(wanReplicationRef.getName(), wanReplicationRefAsMap);
        } else {
            addNonNullToMap(wanReplicationRefAsMap, "name", wanReplicationRef.getName());
            return wanReplicationRefAsMap;
        }
    }

    private static Map<String, Object> getEvictionConfigAsMap(EvictionConfig evictionConfig) {
        if (evictionConfig == null) {
            return null;
        }

        Map<String, Object> evictionConfigAsMap = new LinkedHashMap<>();

        String comparatorClassName = !isNullOrEmpty(evictionConfig.getComparatorClassName())
                ? evictionConfig.getComparatorClassName()
                : null;

        addNonNullToMap(evictionConfigAsMap, "size", evictionConfig.getSize());
        addNonNullToMap(evictionConfigAsMap, "max-size-policy", evictionConfig.getMaxSizePolicy().name());
        addNonNullToMap(evictionConfigAsMap, "eviction-policy", evictionConfig.getEvictionPolicy().name());
        addNonNullToMap(evictionConfigAsMap, "comparator-class-name", comparatorClassName);

        return evictionConfigAsMap;

    }

    private static List<Map<String, Object>> getCacheSimpleEntryListenerConfigsAsList(
            List<CacheSimpleEntryListenerConfig> cacheSimpleEntryListenerConfigs
    ) {
        if (cacheSimpleEntryListenerConfigs == null || cacheSimpleEntryListenerConfigs.isEmpty()) {
            return null;
        }

        List<Map<String, Object>> cacheSimpleEntryListenerConfigsAsList = new LinkedList<>();

        for (CacheSimpleEntryListenerConfig cacheSimpleEntryListenerConfig : cacheSimpleEntryListenerConfigs) {
            Map<String, Object> cacheSimpleEntryListenerConfigsAsMap = new LinkedHashMap<>();

            addNonNullToMap(cacheSimpleEntryListenerConfigsAsMap, "old-value-required",
                    cacheSimpleEntryListenerConfig.isOldValueRequired());
            addNonNullToMap(cacheSimpleEntryListenerConfigsAsMap, "synchronous",
                    cacheSimpleEntryListenerConfig.isSynchronous());
            addNonNullToMap(cacheSimpleEntryListenerConfigsAsMap, "cache-entry-listener-factory",
                    wrapObjectWithMap("class-name", cacheSimpleEntryListenerConfig.getCacheEntryListenerFactory()));
            addNonNullToMap(cacheSimpleEntryListenerConfigsAsMap, "cache-entry-event-filter-factory",
                    wrapObjectWithMap("class-name", cacheSimpleEntryListenerConfig.getCacheEntryEventFilterFactory()));

            addNonNullToList(cacheSimpleEntryListenerConfigsAsList, cacheSimpleEntryListenerConfigsAsMap);
        }

        return cacheSimpleEntryListenerConfigsAsList;
    }

    private static Map<String, Object> getExpiryPolicyFactoryConfigAsMap(
            CacheSimpleConfig.ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig
    ) {
        if (expiryPolicyFactoryConfig == null) {
            return null;
        }

        if (!isNullOrEmpty(expiryPolicyFactoryConfig.getClassName())) {
            return wrapObjectWithMap("class-name", expiryPolicyFactoryConfig.getClassName());
        } else {
            CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig timedConfig
                    = expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();

            if (
                    timedConfig != null
                            && timedConfig.getExpiryPolicyType() != null
                            && timedConfig.getDurationConfig() != null
            ) {
                Map<String, Object> timedConfigAsMap = new LinkedHashMap<>();
                addNonNullToMap(timedConfigAsMap, "expiry-policy-type",
                        timedConfig.getExpiryPolicyType().name());
                addNonNullToMap(timedConfigAsMap, "duration-amount",
                        timedConfig.getDurationConfig().getDurationAmount());
                addNonNullToMap(timedConfigAsMap, "time-unit",
                        timedConfig.getDurationConfig().getTimeUnit().name());

                return wrapObjectWithMap("timed-expiry-policy-factory", timedConfigAsMap);
            }
            return null;
        }
    }

    private static Map<String, Object> getCollectionConfigAsMap(CollectionConfig<?> collectionConfig) {
        if (collectionConfig == null) {
            return null;
        }

        Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(subConfigAsMap, "statistics-enabled",
                collectionConfig.isStatisticsEnabled());
        addNonNullToMap(subConfigAsMap, "max-size",
                collectionConfig.getMaxSize());
        addNonNullToMap(subConfigAsMap, "backup-count",
                collectionConfig.getBackupCount());
        addNonNullToMap(subConfigAsMap, "async-backup-count",
                collectionConfig.getAsyncBackupCount());
        addNonNullToMap(subConfigAsMap, "split-brain-protection-ref",
                collectionConfig.getSplitBrainProtectionName());
        addNonNullToMap(subConfigAsMap, "item-listeners",
                getItemListenerConfigsAsList(collectionConfig.getItemListenerConfigs()));
        addNonNullToMap(subConfigAsMap, "merge-policy",
                getMergePolicyConfigAsMap(collectionConfig.getMergePolicyConfig()));

        return subConfigAsMap;
    }

    private static List<Map<String, Object>> getItemListenerConfigsAsList(
            List<? extends ListenerConfig> listenerConfigs
    ) {
        if (listenerConfigs == null || listenerConfigs.isEmpty()) {
            return null;
        }

        List<Map<String, Object>> listenerConfigsAsList = new LinkedList<>();

        for (ListenerConfig listenerConfig : listenerConfigs) {
            Map<String, Object> listenerConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(listenerConfigAsMap, "class-name",
                    classNameOrImplClass(listenerConfig.getClassName(), listenerConfig.getImplementation()));
            addNonNullToMap(listenerConfigAsMap, "include-value",
                    listenerConfig.isIncludeValue());

            addNonNullToList(listenerConfigsAsList, listenerConfigAsMap);
        }

        return listenerConfigsAsList;
    }

    private static List<Map<String, Object>> getEntryListenerConfigsAsList(
            List<? extends ListenerConfig> listenerConfigs
    ) {
        if (listenerConfigs == null || listenerConfigs.isEmpty()) {
            return null;
        }

        List<Map<String, Object>> listenerConfigsAsList = new LinkedList<>();

        for (ListenerConfig listenerConfig : listenerConfigs) {
            Map<String, Object> listenerConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(listenerConfigAsMap, "class-name",
                    classNameOrImplClass(listenerConfig.getClassName(), listenerConfig.getImplementation()));
            addNonNullToMap(listenerConfigAsMap, "include-value",
                    listenerConfig.isIncludeValue());
            addNonNullToMap(listenerConfigAsMap, "local",
                    listenerConfig.isLocal());

            addNonNullToList(listenerConfigsAsList, listenerConfigAsMap);
        }

        return listenerConfigsAsList;
    }

    private static Map<String, Object> getPropertiesAsMap(@SuppressWarnings("rawtypes") Map<String, Comparable> properties) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }

        Map<String, Object> propertiesAsMap = new LinkedHashMap<>();

        for (String key : properties.keySet()) {
            addNonNullToMap(propertiesAsMap, key, properties.get(key));
        }

        return propertiesAsMap;
    }

    private static Map<String, Object> getPropertiesAsMap(Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }

        Map<String, Object> propertiesAsMap = new LinkedHashMap<>();

        for (Object key : properties.keySet()) {
            addNonNullToMap(propertiesAsMap, key.toString(), properties.getProperty(key.toString()));
        }

        return propertiesAsMap;
    }

    private static Map<String, Object> getStoreConfigAsMap(
            boolean enabled,
            String className,
            String factoryClassName,
            Properties properties
    ) {
        Map<String, Object> storeConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(storeConfigAsMap, "enabled", enabled);
        addNonNullToMap(storeConfigAsMap, "class-name", className);
        addNonNullToMap(storeConfigAsMap, "factory-class-name", factoryClassName);
        addNonNullToMap(storeConfigAsMap, "properties", getPropertiesAsMap(properties));

        return storeConfigAsMap;
    }

    private static Map<String, Object> getMapStoreConfigAsMap(MapStoreConfig mapStoreConfig) {
        if (mapStoreConfig == null) {
            return null;
        }

        // Note that order is different from the ConfigXmlGenerator#classNameOrImplClass().
        // Here implementation takes priority rather than classname.
        String className = mapStoreConfig.getImplementation() != null
                ? mapStoreConfig.getImplementation().getClass().getName()
                : mapStoreConfig.getClassName();
        String factoryClassName = mapStoreConfig.getFactoryImplementation() != null
                ? mapStoreConfig.getFactoryImplementation().getClass().getName()
                : mapStoreConfig.getFactoryClassName();

        Map<String, Object> mapStoreConfigAsMap = getStoreConfigAsMap(
                mapStoreConfig.isEnabled(),
                className,
                factoryClassName,
                mapStoreConfig.getProperties()
        );

        addNonNullToMap(mapStoreConfigAsMap, "initial-mode", mapStoreConfig.getInitialLoadMode().name());
        addNonNullToMap(mapStoreConfigAsMap, "write-coalescing", mapStoreConfig.isWriteCoalescing());
        addNonNullToMap(mapStoreConfigAsMap, "write-delay-seconds", mapStoreConfig.getWriteDelaySeconds());
        addNonNullToMap(mapStoreConfigAsMap, "write-batch-size", mapStoreConfig.getWriteBatchSize());

        return mapStoreConfigAsMap;
    }

    private static Map<String, Object> getQueueStoreConfigAsMap(QueueStoreConfig queueStoreConfig) {
        if (queueStoreConfig == null) {
            return null;
        }

        return getStoreConfigAsMap(
                queueStoreConfig.isEnabled(),
                classNameOrImplClass(
                        queueStoreConfig.getClassName(),
                        queueStoreConfig.getStoreImplementation()
                ),
                classNameOrImplClass(
                        queueStoreConfig.getFactoryClassName(),
                        queueStoreConfig.getFactoryImplementation()
                ),
                queueStoreConfig.getProperties()
        );
    }

    private static Map<String, Object> getRingbufferStoreConfigAsMap(RingbufferStoreConfig ringbufferStoreConfig) {
        if (ringbufferStoreConfig == null) {
            return null;
        }

        return getStoreConfigAsMap(
                ringbufferStoreConfig.isEnabled(),
                classNameOrImplClass(
                        ringbufferStoreConfig.getClassName(),
                        ringbufferStoreConfig.getStoreImplementation()
                ),
                classNameOrImplClass(
                        ringbufferStoreConfig.getFactoryClassName(),
                        ringbufferStoreConfig.getFactoryImplementation()
                ),
                ringbufferStoreConfig.getProperties()
        );
    }

    private static List<String> getListenerConfigsAsList(List<? extends ListenerConfig> listenerConfigs) {
        if (listenerConfigs == null || listenerConfigs.isEmpty()) {
            return null;
        }

        List<String> listenerConfigsAsList = new LinkedList<>();

        for (ListenerConfig listenerConfig : listenerConfigs) {
            addNonNullToList(listenerConfigsAsList,
                    classNameOrImplClass(listenerConfig.getClassName(), listenerConfig.getImplementation()));
        }

        return listenerConfigsAsList;
    }

    private static Map<String, Object> getMergePolicyConfigAsMap(MergePolicyConfig mergePolicyConfig) {
        if (mergePolicyConfig == null) {
            return null;
        }

        Map<String, Object> mergePolicyConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(mergePolicyConfigAsMap, "class-name", mergePolicyConfig.getPolicy());
        addNonNullToMap(mergePolicyConfigAsMap, "batch-size", mergePolicyConfig.getBatchSize());

        return mergePolicyConfigAsMap;
    }

    private static Map<String, Object> wrapObjectWithMap(String key, Object value) {
        if (value == null) {
            return null;
        }
        Map<String, Object> wrappedObject = new LinkedHashMap<>();
        wrappedObject.put(key, value);
        return wrappedObject;
    }

    private static <E> void addNonNullToList(List<E> list, E element) {
        if (element != null) {
            list.add(element);
        }
    }

    private static <K, V> void addNonNullToMap(Map<K, V> map, K key, V value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}
