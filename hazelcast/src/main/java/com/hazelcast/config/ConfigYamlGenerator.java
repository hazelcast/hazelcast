/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.config.ConfigXmlGenerator.classNameOrImplClass;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static java.lang.Boolean.TRUE;

public class ConfigYamlGenerator {

    private static final int INDENT = 2;

    String generate(Config config) {
        Map<String, Object> document = new LinkedHashMap<>();
        Map<String, Object> root = new LinkedHashMap<>();
        document.put("hazelcast", root);

        root.put("cluster-name", config.getClusterName());

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


        DumpSettings dumpSettings = DumpSettings.builder()
                .setDefaultFlowStyle(FlowStyle.BLOCK)
                .setIndicatorIndent(INDENT - 2)
                .setIndent(INDENT)
                .build();
        Dump dump = new Dump(dumpSettings);
        return dump.dumpToString(document);
    }

//    static void xxxGenerator(Map<String, Object> parent, Config config) {
//        if (config.xxx.isEmpty()) {
//            return;
//        }
//
//        Map<String, Object> child = new LinkedHashMap<>();
//        for (xxx subConfigAsObject : config.xxx.values()) {
//            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();
//
//
//            child.put(subConfigAsObject.getName(), subConfigAsMap);
//        }
//
//        parent.put(xxx, child);
//    }

    static void cacheYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getCacheConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (CacheSimpleConfig subConfigAsObject : config.getCacheConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "key-type", wrapObjectWithMap("class-name", subConfigAsObject.getKeyType()));
            addNonNullToMap(subConfigAsMap, "value-type", wrapObjectWithMap("class-name", subConfigAsObject.getValueType()));
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "management-enabled", subConfigAsObject.isManagementEnabled());
            addNonNullToMap(subConfigAsMap, "read-through", subConfigAsObject.isReadThrough());
            addNonNullToMap(subConfigAsMap, "write-through", subConfigAsObject.isWriteThrough());
            addNonNullToMap(subConfigAsMap, "cache-loader-factory", wrapObjectWithMap("class-name", subConfigAsObject.getCacheLoaderFactory()));
            addNonNullToMap(subConfigAsMap, "cache-writer-factory", wrapObjectWithMap("class-name", subConfigAsObject.getCacheWriterFactory()));
            addNonNullToMap(subConfigAsMap, "cache-loader", wrapObjectWithMap("class-name", subConfigAsObject.getCacheLoader()));
            addNonNullToMap(subConfigAsMap, "cache-writer", wrapObjectWithMap("class-name", subConfigAsObject.getCacheWriter()));
            addNonNullToMap(subConfigAsMap, "expiry-policy-factory", getExpiryPolicyFactoryConfigAsMap(subConfigAsObject.getExpiryPolicyFactoryConfig()));
            addNonNullToMap(subConfigAsMap, "cache-entry-listeners", getCacheSimpleEntryListenerConfigsAsList(subConfigAsObject.getCacheEntryListeners()));
            addNonNullToMap(subConfigAsMap, "in-memory-format", subConfigAsObject.getInMemoryFormat().name());
            addNonNullToMap(subConfigAsMap, "backup-count", subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count", subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "eviction", getEvictionConfigAsMap(subConfigAsObject.getEvictionConfig()));
            addNonNullToMap(subConfigAsMap, "wan-replication-ref", getWanReplicationRefAsMap(subConfigAsObject.getWanReplicationRef(), false));
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "partition-lost-listeners", getMessageListenerConfigsAsList(subConfigAsObject.getPartitionLostListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));
            addNonNullToMap(subConfigAsMap, "event-journal", getEventJournalConfigAsMap(subConfigAsObject.getEventJournalConfig()));
            addNonNullToMap(subConfigAsMap, "data-persistence", getDataPersistenceConfigAsMap(subConfigAsObject.getDataPersistenceConfig()));
            addNonNullToMap(subConfigAsMap, "merkle-tree", getMerkleTreeConfigAsMap(subConfigAsObject.getMerkleTreeConfig()));
            addNonNullToMap(subConfigAsMap, "disable-per-entry-invalidation-events", subConfigAsObject.isDisablePerEntryInvalidationEvents());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("cache", child);
    }

    static void queueYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getQueueConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (QueueConfig subConfigAsObject : config.getQueueConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "priority-comparator-class-name", subConfigAsObject.getPriorityComparatorClassName());
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "max-size", subConfigAsObject.getMaxSize());
            addNonNullToMap(subConfigAsMap, "backup-count", subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count", subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "empty-queue-ttl", subConfigAsObject.getEmptyQueueTtl());
            addNonNullToMap(subConfigAsMap, "item-listeners", getItemListenerConfigsAsList(subConfigAsObject.getItemListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "queue-store", getQueueStoreConfigAsMap(subConfigAsObject.getQueueStoreConfig()));
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("queue", child);
    }

    static void listYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getListConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ListConfig subConfigAsObject : config.getListConfigs().values()) {
            child.put(subConfigAsObject.getName(), getCollectionConfigAsMap(subConfigAsObject));
        }

        parent.put("list", child);
    }

    static void setYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getSetConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (SetConfig subConfigAsObject : config.getSetConfigs().values()) {
            child.put(subConfigAsObject.getName(), getCollectionConfigAsMap(subConfigAsObject));
        }

        parent.put("set", child);
    }

    static void multiMapYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getMultiMapConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (MultiMapConfig subConfigAsObject : config.getMultiMapConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "backup-count", subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count", subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "binary", subConfigAsObject.isBinary());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "value-collection-type", subConfigAsObject.getValueCollectionType().name());
            addNonNullToMap(subConfigAsMap, "entry-listeners", getEntryListenerConfigsAsList(subConfigAsObject.getEntryListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("multimap", child);
    }

    static void replicatedMapYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getReplicatedMapConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ReplicatedMapConfig subConfigAsObject : config.getReplicatedMapConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "in-memory-format", subConfigAsObject.getInMemoryFormat().name());
            addNonNullToMap(subConfigAsMap, "async-fillup", subConfigAsObject.isAsyncFillup());
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));
            addNonNullToMap(subConfigAsMap, "entry-listeners", getEntryListenerConfigsAsList(subConfigAsObject.getListenerConfigs()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("replicatedmap", child);
    }

    static void ringbufferYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getRingbufferConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (RingbufferConfig subConfigAsObject : config.getRingbufferConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "capacity", subConfigAsObject.getCapacity());
            addNonNullToMap(subConfigAsMap, "time-to-live-seconds", subConfigAsObject.getTimeToLiveSeconds());
            addNonNullToMap(subConfigAsMap, "backup-count", subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count", subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "in-memory-format", subConfigAsObject.getInMemoryFormat().name());
            addNonNullToMap(subConfigAsMap, "ringbuffer-store", getRingbufferStoreConfigAsMap(subConfigAsObject.getRingbufferStoreConfig()));
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("ringbuffer", child);
    }

    static void topicYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getTopicConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (TopicConfig subConfigAsObject : config.getTopicConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "global-ordering-enabled", subConfigAsObject.isGlobalOrderingEnabled());
            addNonNullToMap(subConfigAsMap, "message-listeners", getMessageListenerConfigsAsList(subConfigAsObject.getMessageListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "multi-threading-enabled", subConfigAsObject.isMultiThreadingEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("topic", child);
    }

    static void reliableTopicYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getReliableTopicConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ReliableTopicConfig subConfigAsObject : config.getReliableTopicConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "read-batch-size", subConfigAsObject.getReadBatchSize());
            addNonNullToMap(subConfigAsMap, "topic-overload-policy", subConfigAsObject.getTopicOverloadPolicy().name());
            addNonNullToMap(subConfigAsMap, "message-listeners", getMessageListenerConfigsAsList(subConfigAsObject.getMessageListenerConfigs()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("reliable-topic", child);
    }

    static void executorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getExecutorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ExecutorConfig subConfigAsObject : config.getExecutorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "pool-size", subConfigAsObject.getPoolSize());
            addNonNullToMap(subConfigAsMap, "queue-capacity", subConfigAsObject.getQueueCapacity());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("executor-service", child);
    }

    static void durableExecutorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getDurableExecutorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (DurableExecutorConfig subConfigAsObject : config.getDurableExecutorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "pool-size", subConfigAsObject.getPoolSize());
            addNonNullToMap(subConfigAsMap, "durability", subConfigAsObject.getDurability());
            addNonNullToMap(subConfigAsMap, "capacity", subConfigAsObject.getCapacity());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("durable-executor-service", child);
    }

    static void scheduledExecutorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getScheduledExecutorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ScheduledExecutorConfig subConfigAsObject : config.getScheduledExecutorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "pool-size", subConfigAsObject.getPoolSize());
            addNonNullToMap(subConfigAsMap, "durability", subConfigAsObject.getDurability());
            addNonNullToMap(subConfigAsMap, "capacity", subConfigAsObject.getCapacity());
            addNonNullToMap(subConfigAsMap, "capacity-policy", subConfigAsObject.getCapacityPolicy().name());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("scheduled-executor-service", child);
    }

    static void cardinalityEstimatorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getCardinalityEstimatorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (CardinalityEstimatorConfig subConfigAsObject : config.getCardinalityEstimatorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "backup-count", subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count", subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("cardinality-estimator", child);
    }

    static void flakeIdGeneratorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getFlakeIdGeneratorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (FlakeIdGeneratorConfig subConfigAsObject : config.getFlakeIdGeneratorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "prefetch-count", subConfigAsObject.getPrefetchCount());
            addNonNullToMap(subConfigAsMap, "prefetch-validity-millis", subConfigAsObject.getPrefetchValidityMillis());
            addNonNullToMap(subConfigAsMap, "epoch-start", subConfigAsObject.getEpochStart());
            addNonNullToMap(subConfigAsMap, "node-id-offset", subConfigAsObject.getNodeIdOffset());
            addNonNullToMap(subConfigAsMap, "bits-sequence", subConfigAsObject.getBitsSequence());
            addNonNullToMap(subConfigAsMap, "bits-node-id", subConfigAsObject.getBitsNodeId());
            addNonNullToMap(subConfigAsMap, "allowed-future-millis", subConfigAsObject.getAllowedFutureMillis());
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("flake-id-generator", child);
    }

    static void pnCounterYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getPNCounterConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (PNCounterConfig subConfigAsObject : config.getPNCounterConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "replica-count", subConfigAsObject.getReplicaCount());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("pn-counter", child);
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

        addNonNullToMap(eventJournalConfigAsMap, "enabled", eventJournalConfig.isEnabled());
        addNonNullToMap(eventJournalConfigAsMap, "capacity", eventJournalConfig.getCapacity());
        addNonNullToMap(eventJournalConfigAsMap, "time-to-live-seconds", eventJournalConfig.getTimeToLiveSeconds());

        return eventJournalConfigAsMap;
    }

    private static Map<String, Object> getWanReplicationRefAsMap(WanReplicationRef wanReplicationRef, boolean isMap) {
        if (wanReplicationRef == null) {
            return null;
        }

        Map<String, Object> wanReplicationRefAsMap = new LinkedHashMap<>();

        addNonNullToMap(wanReplicationRefAsMap, "merge-policy-class-name", wanReplicationRef.getMergePolicyClassName());
        addNonNullToMap(wanReplicationRefAsMap, "republishing-enabled", wanReplicationRef.isRepublishingEnabled());
        addNonNullToMap(wanReplicationRefAsMap, "filters", wanReplicationRef.getFilters());

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

    private static List<Map<String, Object>> getCacheSimpleEntryListenerConfigsAsList(List<CacheSimpleEntryListenerConfig> cacheSimpleEntryListenerConfigs) {
        if (cacheSimpleEntryListenerConfigs == null || cacheSimpleEntryListenerConfigs.isEmpty()) {
            return null;
        }

        List<Map<String, Object>> cacheSimpleEntryListenerConfigsAsList = new LinkedList<>();

        for (CacheSimpleEntryListenerConfig cacheSimpleEntryListenerConfig : cacheSimpleEntryListenerConfigs) {
            Map<String, Object> cacheSimpleEntryListenerConfigsAsMap = new LinkedHashMap<>();

            addNonNullToMap(cacheSimpleEntryListenerConfigsAsMap, "old-value-required", cacheSimpleEntryListenerConfig.isOldValueRequired());
            addNonNullToMap(cacheSimpleEntryListenerConfigsAsMap, "synchronous", cacheSimpleEntryListenerConfig.isSynchronous());
            addNonNullToMap(cacheSimpleEntryListenerConfigsAsMap, "cache-entry-listener-factory", wrapObjectWithMap("class-name", cacheSimpleEntryListenerConfig.getCacheEntryListenerFactory()));
            addNonNullToMap(cacheSimpleEntryListenerConfigsAsMap, "cache-entry-event-filter-factory", wrapObjectWithMap("class-name", cacheSimpleEntryListenerConfig.getCacheEntryEventFilterFactory()));

            addNonNullToList(cacheSimpleEntryListenerConfigsAsList, cacheSimpleEntryListenerConfigsAsMap);
        }

        return cacheSimpleEntryListenerConfigsAsList;
    }

    private static Map<String, Object> getExpiryPolicyFactoryConfigAsMap(CacheSimpleConfig.ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig) {
        if (expiryPolicyFactoryConfig == null) {
            return null;
        }

        if (!isNullOrEmpty(expiryPolicyFactoryConfig.getClassName())) {
            return wrapObjectWithMap("class-name", expiryPolicyFactoryConfig.getClassName());
        } else {
            CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig timedConfig = expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
            if (timedConfig != null && timedConfig.getExpiryPolicyType() != null && timedConfig.getDurationConfig() != null) {
                Map<String, Object> timedConfigAsMap = new LinkedHashMap<>();
                addNonNullToMap(timedConfigAsMap, "expiry-policy-type", timedConfig.getExpiryPolicyType().name());
                addNonNullToMap(timedConfigAsMap, "duration-amount", timedConfig.getDurationConfig().getDurationAmount());
                addNonNullToMap(timedConfigAsMap, "time-unit", timedConfig.getDurationConfig().getTimeUnit().name());
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

        addNonNullToMap(subConfigAsMap, "statistics-enabled", collectionConfig.isStatisticsEnabled());
        addNonNullToMap(subConfigAsMap, "max-size", collectionConfig.getMaxSize());
        addNonNullToMap(subConfigAsMap, "backup-count", collectionConfig.getBackupCount());
        addNonNullToMap(subConfigAsMap, "async-backup-count", collectionConfig.getAsyncBackupCount());
        addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", collectionConfig.getSplitBrainProtectionName());
        addNonNullToMap(subConfigAsMap, "item-listeners", getItemListenerConfigsAsList(collectionConfig.getItemListenerConfigs()));
        addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(collectionConfig.getMergePolicyConfig()));

        return subConfigAsMap;
    }

    private static List<Map<String, Object>> getItemListenerConfigsAsList(List<? extends ListenerConfig> listenerConfigs) {
        if (listenerConfigs == null || listenerConfigs.isEmpty()) {
            return null;
        }

        List<Map<String, Object>> listenerConfigsAsList = new LinkedList<>();

        for (ListenerConfig listenerConfig : listenerConfigs) {
            Map<String, Object> listenerConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(listenerConfigAsMap, "class-name", classNameOrImplClass(listenerConfig.getClassName(), listenerConfig.getImplementation()));
            addNonNullToMap(listenerConfigAsMap, "include-value", listenerConfig.isIncludeValue());

            addNonNullToList(listenerConfigsAsList, listenerConfigAsMap);
        }

        return listenerConfigsAsList;
    }

    private static List<Map<String, Object>> getEntryListenerConfigsAsList(List<? extends ListenerConfig> listenerConfigs) {
        if (listenerConfigs == null || listenerConfigs.isEmpty()) {
            return null;
        }

        List<Map<String, Object>> listenerConfigsAsList = new LinkedList<>();

        for (ListenerConfig listenerConfig : listenerConfigs) {
            Map<String, Object> listenerConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(listenerConfigAsMap, "class-name", classNameOrImplClass(listenerConfig.getClassName(), listenerConfig.getImplementation()));
            addNonNullToMap(listenerConfigAsMap, "include-value", listenerConfig.isIncludeValue());
            addNonNullToMap(listenerConfigAsMap, "local", listenerConfig.isLocal());

            addNonNullToList(listenerConfigsAsList, listenerConfigAsMap);
        }

        return listenerConfigsAsList;
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

    private static Map<String, Object> getQueueStoreConfigAsMap(QueueStoreConfig queueStoreConfig) {
        if (queueStoreConfig == null) {
            return null;
        }

        return getStoreConfigAsMap(
                queueStoreConfig.isEnabled(),
                classNameOrImplClass(queueStoreConfig.getClassName(), queueStoreConfig.getStoreImplementation()),
                classNameOrImplClass(queueStoreConfig.getFactoryClassName(), queueStoreConfig.getFactoryImplementation()),
                queueStoreConfig.getProperties()
        );
    }

    private static Map<String, Object> getRingbufferStoreConfigAsMap(RingbufferStoreConfig ringbufferStoreConfig) {
        if (ringbufferStoreConfig == null) {
            return null;
        }

        return getStoreConfigAsMap(
                ringbufferStoreConfig.isEnabled(),
                classNameOrImplClass(ringbufferStoreConfig.getClassName(), ringbufferStoreConfig.getStoreImplementation()),
                classNameOrImplClass(ringbufferStoreConfig.getFactoryClassName(), ringbufferStoreConfig.getFactoryImplementation()),
                ringbufferStoreConfig.getProperties()
        );
    }

    private static List<String> getMessageListenerConfigsAsList(List<? extends ListenerConfig> listenerConfigs) {
        if (listenerConfigs == null || listenerConfigs.isEmpty()) {
            return null;
        }

        List<String> listenerConfigsAsList = new LinkedList<>();

        for (ListenerConfig listenerConfig : listenerConfigs) {
            addNonNullToList(listenerConfigsAsList, classNameOrImplClass(listenerConfig.getClassName(), listenerConfig.getImplementation()));
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
