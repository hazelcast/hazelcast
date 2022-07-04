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
import com.hazelcast.config.CachePartitionLostListenerConfig;
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
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
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
import com.hazelcast.config.TieredStoreConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.memory.Capacity;
import com.hazelcast.query.impl.IndexUtils;

import java.util.Collection;
import java.util.List;

import static com.hazelcast.internal.config.AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom;
import static com.hazelcast.internal.config.ConfigSections.LICENSE_KEY;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;
import static java.lang.Boolean.TRUE;

@SuppressWarnings({"checkstyle:MethodCount", "checkstyle:ClassFanOutComplexity"})
public final class DynamicConfigXmlGenerator {
    private DynamicConfigXmlGenerator() {
        // not called
    }

    public static void licenseKeyXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        gen.node(LICENSE_KEY.getName(), config.getLicenseKey());
    }

    public static void mapXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        Collection<MapConfig> mapConfigs = config.getMapConfigs().values();
        for (MapConfig m : mapConfigs) {
            String cacheDeserializedVal = m.getCacheDeserializedValues() != null
                    ? m.getCacheDeserializedValues().name().replaceAll("_", "-") : null;
            MergePolicyConfig mergePolicyConfig = m.getMergePolicyConfig();
            gen.open("map", "name", m.getName())
                    .node("in-memory-format", m.getInMemoryFormat())
                    .node("statistics-enabled", m.isStatisticsEnabled())
                    .node("per-entry-stats-enabled", m.isPerEntryStatsEnabled())
                    .node("cache-deserialized-values", cacheDeserializedVal)
                    .node("backup-count", m.getBackupCount())
                    .node("async-backup-count", m.getAsyncBackupCount())
                    .node("time-to-live-seconds", m.getTimeToLiveSeconds())
                    .node("max-idle-seconds", m.getMaxIdleSeconds())
                    .node("merge-policy", mergePolicyConfig.getPolicy(),
                            "batch-size", mergePolicyConfig.getBatchSize())
                    .node("split-brain-protection-ref", m.getSplitBrainProtectionName())
                    .node("read-backup-data", m.isReadBackupData())
                    .node("metadata-policy", m.getMetadataPolicy());

            evictionConfigXmlGenerator(gen, m.getEvictionConfig());
            if (m.getMerkleTreeConfig().getEnabled() != null) {
                appendMerkleTreeConfig(gen, m.getMerkleTreeConfig());
            }
            appendEventJournalConfig(gen, m.getEventJournalConfig());
            appendDataPersistenceConfig(gen, m.getDataPersistenceConfig());
            mapStoreConfigXmlGenerator(gen, m);
            mapNearCacheConfigXmlGenerator(gen, m.getNearCacheConfig());
            wanReplicationConfigXmlGenerator(gen, m.getWanReplicationRef());
            indexConfigXmlGenerator(gen, m);
            attributeConfigXmlGenerator(gen, m);
            entryListenerConfigXmlGenerator(gen, m);
            mapPartitionLostListenerConfigXmlGenerator(gen, m);
            mapPartitionStrategyConfigXmlGenerator(gen, m);
            mapQueryCachesConfigXmlGenerator(gen, m);
            tieredStoreConfigXmlGenerator(gen, m.getTieredStoreConfig());
            gen.close();
        }
    }

    public static void cacheXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (CacheSimpleConfig c : config.getCacheConfigs().values()) {
            gen.open("cache", "name", c.getName());
            if (c.getKeyType() != null) {
                gen.node("key-type", null, "class-name", c.getKeyType());
            }
            if (c.getValueType() != null) {
                gen.node("value-type", null, "class-name", c.getValueType());
            }

            gen.node("statistics-enabled", c.isStatisticsEnabled())
                    .node("management-enabled", c.isManagementEnabled())
                    .node("read-through", c.isReadThrough())
                    .node("write-through", c.isWriteThrough());

            checkAndFillCacheLoaderFactoryConfigXml(gen, c.getCacheLoaderFactory());
            checkAndFillCacheLoaderConfigXml(gen, c.getCacheLoader());
            checkAndFillCacheWriterFactoryConfigXml(gen, c.getCacheWriterFactory());
            checkAndFillCacheWriterConfigXml(gen, c.getCacheWriter());
            cacheExpiryPolicyFactoryConfigXmlGenerator(gen, c.getExpiryPolicyFactoryConfig());

            gen.open("cache-entry-listeners");
            for (CacheSimpleEntryListenerConfig el : c.getCacheEntryListeners()) {
                gen.open("cache-entry-listener",
                                "old-value-required", el.isOldValueRequired(),
                                "synchronous", el.isSynchronous())
                        .node("cache-entry-listener-factory", null, "class-name", el.getCacheEntryListenerFactory())
                        .node("cache-entry-event-filter-factory", null, "class-name", el.getCacheEntryEventFilterFactory())
                        .close();
            }
            gen.close()
                    .node("in-memory-format", c.getInMemoryFormat())
                    .node("backup-count", c.getBackupCount())
                    .node("async-backup-count", c.getAsyncBackupCount());

            evictionConfigXmlGenerator(gen, c.getEvictionConfig());
            wanReplicationConfigXmlGenerator(gen, c.getWanReplicationRef());

            gen.node("split-brain-protection-ref", c.getSplitBrainProtectionName());
            cachePartitionLostListenerConfigXmlGenerator(gen, c.getPartitionLostListenerConfigs());

            gen.node("merge-policy", c.getMergePolicyConfig().getPolicy(),
                    "batch-size", c.getMergePolicyConfig().getBatchSize());

            appendEventJournalConfig(gen, c.getEventJournalConfig());
            appendDataPersistenceConfig(gen, c.getDataPersistenceConfig());
            if (c.getMerkleTreeConfig().getEnabled() != null) {
                appendMerkleTreeConfig(gen, c.getMerkleTreeConfig());
            }

            gen.node("disable-per-entry-invalidation-events", c.isDisablePerEntryInvalidationEvents())
                    .close();
        }
    }

    public static void queueXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        Collection<QueueConfig> qCfgs = config.getQueueConfigs().values();
        for (QueueConfig q : qCfgs) {
            gen.open("queue", "name", q.getName())
                    .node("priority-comparator-class-name", q.getPriorityComparatorClassName())
                    .node("statistics-enabled", q.isStatisticsEnabled())
                    .node("max-size", q.getMaxSize())
                    .node("backup-count", q.getBackupCount())
                    .node("async-backup-count", q.getAsyncBackupCount())
                    .node("empty-queue-ttl", q.getEmptyQueueTtl());
            appendItemListenerConfigs(gen, q.getItemListenerConfigs());
            QueueStoreConfig storeConfig = q.getQueueStoreConfig();
            if (storeConfig != null) {
                gen.open("queue-store", "enabled", storeConfig.isEnabled())
                        .node("class-name",
                                classNameOrImplClass(storeConfig.getClassName(), storeConfig.getStoreImplementation()))
                        .node("factory-class-name",
                                classNameOrImplClass(storeConfig.getFactoryClassName(), storeConfig.getFactoryImplementation()))
                        .appendProperties(storeConfig.getProperties())
                        .close();
            }
            MergePolicyConfig mergePolicyConfig = q.getMergePolicyConfig();
            gen.node("split-brain-protection-ref", q.getSplitBrainProtectionName())
                    .node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .close();
        }
    }

    public static void listXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        collectionXmlGenerator(gen, "list", config.getListConfigs().values());
    }

    public static void setXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        collectionXmlGenerator(gen, "set", config.getSetConfigs().values());
    }

    public static void multiMapXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (MultiMapConfig mm : config.getMultiMapConfigs().values()) {
            gen.open("multimap", "name", mm.getName())
                    .node("backup-count", mm.getBackupCount())
                    .node("async-backup-count", mm.getAsyncBackupCount())
                    .node("statistics-enabled", mm.isStatisticsEnabled())
                    .node("binary", mm.isBinary())
                    .node("split-brain-protection-ref", mm.getSplitBrainProtectionName())
                    .node("value-collection-type", mm.getValueCollectionType());

            entryListenerConfigXmlGenerator(gen, mm.getEntryListenerConfigs());
            MergePolicyConfig mergePolicyConfig = mm.getMergePolicyConfig();
            gen.node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .close();
        }
    }

    public static void replicatedMapXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (ReplicatedMapConfig r : config.getReplicatedMapConfigs().values()) {
            MergePolicyConfig mergePolicyConfig = r.getMergePolicyConfig();
            gen.open("replicatedmap", "name", r.getName())
                    .node("in-memory-format", r.getInMemoryFormat())
                    .node("async-fillup", r.isAsyncFillup())
                    .node("statistics-enabled", r.isStatisticsEnabled())
                    .node("split-brain-protection-ref", r.getSplitBrainProtectionName())
                    .node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize());

            if (!r.getListenerConfigs().isEmpty()) {
                gen.open("entry-listeners");
                for (ListenerConfig lc : r.getListenerConfigs()) {
                    gen.node("entry-listener", classNameOrImplClass(lc.getClassName(), lc.getImplementation()),
                            "include-value", lc.isIncludeValue(),
                            "local", lc.isLocal());
                }
                gen.close();
            }
            gen.close();
        }
    }

    public static void ringbufferXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        Collection<RingbufferConfig> configs = config.getRingbufferConfigs().values();
        for (RingbufferConfig rbConfig : configs) {
            gen.open("ringbuffer", "name", rbConfig.getName())
                    .node("capacity", rbConfig.getCapacity())
                    .node("time-to-live-seconds", rbConfig.getTimeToLiveSeconds())
                    .node("backup-count", rbConfig.getBackupCount())
                    .node("async-backup-count", rbConfig.getAsyncBackupCount())
                    .node("split-brain-protection-ref", rbConfig.getSplitBrainProtectionName())
                    .node("in-memory-format", rbConfig.getInMemoryFormat());

            RingbufferStoreConfig storeConfig = rbConfig.getRingbufferStoreConfig();
            if (storeConfig != null) {
                gen.open("ringbuffer-store", "enabled", storeConfig.isEnabled())
                        .node("class-name",
                                classNameOrImplClass(storeConfig.getClassName(), storeConfig.getStoreImplementation()))
                        .node("factory-class-name",
                                classNameOrImplClass(storeConfig.getFactoryClassName(), storeConfig.getFactoryImplementation()))
                        .appendProperties(storeConfig.getProperties());
                gen.close();
            }
            MergePolicyConfig mergePolicyConfig = rbConfig.getMergePolicyConfig();
            gen.node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .close();
        }
    }

    public static void topicXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (TopicConfig t : config.getTopicConfigs().values()) {
            gen.open("topic", "name", t.getName())
                    .node("statistics-enabled", t.isStatisticsEnabled())
                    .node("global-ordering-enabled", t.isGlobalOrderingEnabled());

            if (!t.getMessageListenerConfigs().isEmpty()) {
                gen.open("message-listeners");
                for (ListenerConfig lc : t.getMessageListenerConfigs()) {
                    gen.node("message-listener", classNameOrImplClass(lc.getClassName(), lc.getImplementation()));
                }
                gen.close();
            }
            gen.node("multi-threading-enabled", t.isMultiThreadingEnabled());
            gen.close();
        }
    }

    public static void reliableTopicXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (ReliableTopicConfig t : config.getReliableTopicConfigs().values()) {
            gen.open("reliable-topic", "name", t.getName())
                    .node("statistics-enabled", t.isStatisticsEnabled())
                    .node("read-batch-size", t.getReadBatchSize())
                    .node("topic-overload-policy", t.getTopicOverloadPolicy());

            if (!t.getMessageListenerConfigs().isEmpty()) {
                gen.open("message-listeners");
                for (ListenerConfig lc : t.getMessageListenerConfigs()) {
                    gen.node("message-listener", classNameOrImplClass(lc.getClassName(), lc.getImplementation()));
                }
                gen.close();
            }
            gen.close();
        }
    }

    public static void executorXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (ExecutorConfig ex : config.getExecutorConfigs().values()) {
            gen.open("executor-service", "name", ex.getName())
                    .node("statistics-enabled", ex.isStatisticsEnabled())
                    .node("pool-size", ex.getPoolSize())
                    .node("queue-capacity", ex.getQueueCapacity())
                    .node("split-brain-protection-ref", ex.getSplitBrainProtectionName())
                    .close();
        }
    }

    public static void durableExecutorXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (DurableExecutorConfig ex : config.getDurableExecutorConfigs().values()) {
            gen.open("durable-executor-service", "name", ex.getName())
                    .node("pool-size", ex.getPoolSize())
                    .node("durability", ex.getDurability())
                    .node("capacity", ex.getCapacity())
                    .node("split-brain-protection-ref", ex.getSplitBrainProtectionName())
                    .node("statistics-enabled", ex.isStatisticsEnabled())
                    .close();
        }
    }

    public static void scheduledExecutorXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (ScheduledExecutorConfig ex : config.getScheduledExecutorConfigs().values()) {
            MergePolicyConfig mergePolicyConfig = ex.getMergePolicyConfig();

            gen.open("scheduled-executor-service", "name", ex.getName())
                    .node("pool-size", ex.getPoolSize())
                    .node("durability", ex.getDurability())
                    .node("capacity", ex.getCapacity())
                    .node("capacity-policy", ex.getCapacityPolicy().name())
                    .node("split-brain-protection-ref", ex.getSplitBrainProtectionName())
                    .node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .node("statistics-enabled", ex.isStatisticsEnabled())
                    .close();
        }
    }

    public static void cardinalityEstimatorXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (CardinalityEstimatorConfig ex : config.getCardinalityEstimatorConfigs().values()) {
            MergePolicyConfig mergePolicyConfig = ex.getMergePolicyConfig();

            gen.open("cardinality-estimator", "name", ex.getName())
                    .node("backup-count", ex.getBackupCount())
                    .node("async-backup-count", ex.getAsyncBackupCount())
                    .node("split-brain-protection-ref", ex.getSplitBrainProtectionName())
                    .node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .close();
        }
    }

    public static void flakeIdGeneratorXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (FlakeIdGeneratorConfig m : config.getFlakeIdGeneratorConfigs().values()) {
            gen.open("flake-id-generator", "name", m.getName())
                    .node("prefetch-count", m.getPrefetchCount())
                    .node("prefetch-validity-millis", m.getPrefetchValidityMillis())
                    .node("epoch-start", m.getEpochStart())
                    .node("node-id-offset", m.getNodeIdOffset())
                    .node("bits-sequence", m.getBitsSequence())
                    .node("bits-node-id", m.getBitsNodeId())
                    .node("allowed-future-millis", m.getAllowedFutureMillis())
                    .node("statistics-enabled", m.isStatisticsEnabled());
            gen.close();
        }
    }

    public static void pnCounterXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (PNCounterConfig counterConfig : config.getPNCounterConfigs().values()) {
            gen.open("pn-counter", "name", counterConfig.getName())
                    .node("replica-count", counterConfig.getReplicaCount())
                    .node("split-brain-protection-ref", counterConfig.getSplitBrainProtectionName())
                    .node("statistics-enabled", counterConfig.isStatisticsEnabled())
                    .close();
        }
    }

    public static void wanReplicationXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, Config config) {
        for (WanReplicationConfig wan : config.getWanReplicationConfigs().values()) {
            gen.open("wan-replication", "name", wan.getName());
            for (WanBatchPublisherConfig p : wan.getBatchPublisherConfigs()) {
                wanBatchReplicationPublisherXmlGenerator(gen, p);
            }
            for (WanCustomPublisherConfig p : wan.getCustomPublisherConfigs()) {
                wanCustomPublisherXmlGenerator(gen, p);
            }

            WanConsumerConfig consumerConfig = wan.getConsumerConfig();
            if (consumerConfig != null) {
                wanReplicationConsumerGenerator(gen, consumerConfig);
            }
            gen.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static void collectionXmlGenerator(ConfigXmlGenerator.XmlGenerator gen,
                                               String type,
                                               Collection<? extends CollectionConfig> configs) {
        if (CollectionUtil.isNotEmpty(configs)) {
            for (CollectionConfig<? extends CollectionConfig> config : configs) {
                gen.open(type, "name", config.getName())
                        .node("statistics-enabled", config.isStatisticsEnabled())
                        .node("max-size", config.getMaxSize())
                        .node("backup-count", config.getBackupCount())
                        .node("async-backup-count", config.getAsyncBackupCount())
                        .node("split-brain-protection-ref", config.getSplitBrainProtectionName());
                appendItemListenerConfigs(gen, config.getItemListenerConfigs());
                MergePolicyConfig mergePolicyConfig = config.getMergePolicyConfig();
                gen.node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                        .close();
            }
        }
    }

    private static void wanReplicationConsumerGenerator(ConfigXmlGenerator.XmlGenerator gen, WanConsumerConfig consumerConfig) {
        gen.open("consumer");
        String consumerClassName = classNameOrImplClass(
                consumerConfig.getClassName(), consumerConfig.getImplementation());
        if (consumerClassName != null) {
            gen.node("class-name", consumerClassName);
        }
        gen.node("persist-wan-replicated-data", consumerConfig.isPersistWanReplicatedData())
                .appendProperties(consumerConfig.getProperties())
                .close();
    }

    private static void wanBatchReplicationPublisherXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, WanBatchPublisherConfig c) {
        String publisherId = c.getPublisherId();
        gen.open("batch-publisher");
        gen.node("cluster-name", c.getClusterName())
                .node("batch-size", c.getBatchSize())
                .node("batch-max-delay-millis", c.getBatchMaxDelayMillis())
                .node("response-timeout-millis", c.getResponseTimeoutMillis())
                .node("acknowledge-type", c.getAcknowledgeType())
                .node("initial-publisher-state", c.getInitialPublisherState())
                .node("snapshot-enabled", c.isSnapshotEnabled())
                .node("idle-max-park-ns", c.getIdleMaxParkNs())
                .node("idle-min-park-ns", c.getIdleMinParkNs())
                .node("max-concurrent-invocations", c.getMaxConcurrentInvocations())
                .node("discovery-period-seconds", c.getDiscoveryPeriodSeconds())
                .node("use-endpoint-private-address", c.isUseEndpointPrivateAddress())
                .node("queue-full-behavior", c.getQueueFullBehavior())
                .node("max-target-endpoints", c.getMaxTargetEndpoints())
                .node("queue-capacity", c.getQueueCapacity())
                .appendProperties(c.getProperties());

        if (!isNullOrEmptyAfterTrim(publisherId)) {
            gen.node("publisher-id", publisherId);
        }
        if (c.getTargetEndpoints() != null) {
            gen.node("target-endpoints", c.getTargetEndpoints());
        }
        if (c.getEndpoint() != null) {
            gen.node("endpoint", c.getEndpoint());
        }
        wanReplicationSyncGenerator(gen, c.getSyncConfig());
        aliasedDiscoveryConfigsGenerator(gen, aliasedDiscoveryConfigsFrom(c));
        discoveryStrategyConfigXmlGenerator(gen, c.getDiscoveryConfig());
        gen.close();
    }

    private static void wanCustomPublisherXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, WanCustomPublisherConfig c) {
        String publisherId = c.getPublisherId();
        gen.open("custom-publisher")
                .appendProperties(c.getProperties())
                .node("class-name", c.getClassName())
                .node("publisher-id", publisherId)
                .close();
    }

    private static void wanReplicationSyncGenerator(ConfigXmlGenerator.XmlGenerator gen, WanSyncConfig c) {
        gen.open("sync")
                .node("consistency-check-strategy", c.getConsistencyCheckStrategy())
                .close();
    }

    private static void tieredStoreConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, TieredStoreConfig tieredStoreConfig) {
        gen.open("tiered-store", "enabled", tieredStoreConfig.isEnabled());
        appendMemoryTierConfig(gen, tieredStoreConfig.getMemoryTierConfig());
        appendDiskTierConfig(gen, tieredStoreConfig.getDiskTierConfig());
        gen.close();
    }

    private static void appendMemoryTierConfig(ConfigXmlGenerator.XmlGenerator gen, MemoryTierConfig memoryTierConfig) {
        Capacity capacity = memoryTierConfig.getCapacity();
        gen.open("memory-tier")
                .node("capacity", null,
                        "unit", capacity.getUnit(), "value", capacity.getValue())
                .close();
    }

    private static void appendDiskTierConfig(ConfigXmlGenerator.XmlGenerator gen, DiskTierConfig diskTierConfig) {
        gen.open("disk-tier", "enabled", diskTierConfig.isEnabled(),
                        "device-name", diskTierConfig.getDeviceName())
                .close();
    }

    private static void checkAndFillCacheWriterFactoryConfigXml(ConfigXmlGenerator.XmlGenerator gen, String cacheWriter) {
        if (isNullOrEmpty(cacheWriter)) {
            return;
        }
        gen.node("cache-writer-factory", null, "class-name", cacheWriter);
    }

    private static void checkAndFillCacheWriterConfigXml(ConfigXmlGenerator.XmlGenerator gen, String cacheWriter) {
        if (isNullOrEmpty(cacheWriter)) {
            return;
        }
        gen.node("cache-writer", null, "class-name", cacheWriter);
    }

    private static void checkAndFillCacheLoaderFactoryConfigXml(ConfigXmlGenerator.XmlGenerator gen, String cacheLoader) {
        if (isNullOrEmpty(cacheLoader)) {
            return;
        }
        gen.node("cache-loader-factory", null, "class-name", cacheLoader);
    }

    private static void checkAndFillCacheLoaderConfigXml(ConfigXmlGenerator.XmlGenerator gen, String cacheLoader) {
        if (isNullOrEmpty(cacheLoader)) {
            return;
        }
        gen.node("cache-loader", null, "class-name", cacheLoader);
    }

    private static void cacheExpiryPolicyFactoryConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen,
                                                                   CacheSimpleConfig.ExpiryPolicyFactoryConfig config) {
        if (config == null) {
            return;
        }
        if (!isNullOrEmpty(config.getClassName())) {
            gen.node("expiry-policy-factory", null, "class-name", config.getClassName());
        } else {
            CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig timedConfig
                    = config.getTimedExpiryPolicyFactoryConfig();
            if (timedConfig != null && timedConfig.getExpiryPolicyType() != null && timedConfig.getDurationConfig() != null) {
                CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig duration = timedConfig.getDurationConfig();
                gen.open("expiry-policy-factory")
                        .node("timed-expiry-policy-factory", null,
                                "expiry-policy-type", timedConfig.getExpiryPolicyType(),
                                "duration-amount", duration.getDurationAmount(),
                                "time-unit", duration.getTimeUnit().name())
                        .close();
            }
        }
    }

    private static void cachePartitionLostListenerConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen,
                                                                     List<CachePartitionLostListenerConfig> configs) {
        if (configs.isEmpty()) {
            return;
        }
        gen.open("partition-lost-listeners");
        for (CachePartitionLostListenerConfig c : configs) {
            gen.node("partition-lost-listener", classNameOrImplClass(c.getClassName(), c.getImplementation()));
        }
        gen.close();
    }

    private static void mapPartitionStrategyConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, MapConfig m) {
        if (m.getPartitioningStrategyConfig() != null) {
            PartitioningStrategyConfig psc = m.getPartitioningStrategyConfig();
            gen.node("partition-strategy",
                    classNameOrImplClass(psc.getPartitioningStrategyClass(), psc.getPartitioningStrategy()));
        }
    }

    private static void mapQueryCachesConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, MapConfig mapConfig) {
        List<QueryCacheConfig> queryCacheConfigs = mapConfig.getQueryCacheConfigs();
        if (queryCacheConfigs != null && !queryCacheConfigs.isEmpty()) {
            gen.open("query-caches");
            for (QueryCacheConfig queryCacheConfig : queryCacheConfigs) {
                gen.open("query-cache", "name", queryCacheConfig.getName());
                gen.node("include-value", queryCacheConfig.isIncludeValue());
                gen.node("in-memory-format", queryCacheConfig.getInMemoryFormat());
                gen.node("populate", queryCacheConfig.isPopulate());
                gen.node("coalesce", queryCacheConfig.isCoalesce());
                gen.node("delay-seconds", queryCacheConfig.getDelaySeconds());
                gen.node("batch-size", queryCacheConfig.getBatchSize());
                gen.node("buffer-size", queryCacheConfig.getBufferSize());
                gen.node("serialize-keys", queryCacheConfig.isSerializeKeys());

                evictionConfigXmlGenerator(gen, queryCacheConfig.getEvictionConfig());
                IndexUtils.generateXml(gen, queryCacheConfig.getIndexConfigs());
                mapQueryCachePredicateConfigXmlGenerator(gen, queryCacheConfig);

                entryListenerConfigXmlGenerator(gen, queryCacheConfig.getEntryListenerConfigs());
                gen.close();
            }
            gen.close();
        }
    }

    private static void entryListenerConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, MapConfig m) {
        entryListenerConfigXmlGenerator(gen, m.getEntryListenerConfigs());
    }

    private static void mapPartitionLostListenerConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, MapConfig m) {
        if (!m.getPartitionLostListenerConfigs().isEmpty()) {
            gen.open("partition-lost-listeners");
            for (MapPartitionLostListenerConfig c : m.getPartitionLostListenerConfigs()) {
                gen.node("partition-lost-listener",
                        classNameOrImplClass(c.getClassName(), c.getImplementation()));
            }
            gen.close();
        }
    }

    private static void mapQueryCachePredicateConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen,
                                                                 QueryCacheConfig queryCacheConfig) {
        PredicateConfig predicateConfig = queryCacheConfig.getPredicateConfig();

        String type = predicateConfig.getClassName() != null ? "class-name" : "sql";
        String content = predicateConfig.getClassName() != null ? predicateConfig.getClassName() : predicateConfig
                .getSql();
        gen.node("predicate", content, "type", type);
    }

    private static void appendMerkleTreeConfig(ConfigXmlGenerator.XmlGenerator gen, MerkleTreeConfig c) {
        gen.open("merkle-tree", "enabled", TRUE.equals(c.getEnabled()))
                .node("depth", c.getDepth())
                .close();
    }

    private static void appendDataPersistenceConfig(ConfigXmlGenerator.XmlGenerator gen, DataPersistenceConfig p) {
        gen.open("data-persistence", "enabled", p != null && p.isEnabled())
                .node("fsync", p != null && p.isFsync())
                .close();
    }

    private static void appendEventJournalConfig(ConfigXmlGenerator.XmlGenerator gen, EventJournalConfig c) {
        gen.open("event-journal", "enabled", c.isEnabled())
                .node("capacity", c.getCapacity())
                .node("time-to-live-seconds", c.getTimeToLiveSeconds())
                .close();
    }

    private static void indexConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, MapConfig m) {
        IndexUtils.generateXml(gen, m.getIndexConfigs());
    }

    private static void attributeConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, MapConfig m) {
        if (!m.getAttributeConfigs().isEmpty()) {
            gen.open("attributes");
            for (AttributeConfig attributeCfg : m.getAttributeConfigs()) {
                gen.node("attribute", attributeCfg.getName(), "extractor-class-name", attributeCfg.getExtractorClassName());
            }
            gen.close();
        }
    }

    private static void wanReplicationConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, WanReplicationRef wan) {
        if (wan != null) {
            gen.open("wan-replication-ref", "name", wan.getName());

            String mergePolicy = wan.getMergePolicyClassName();
            if (!isNullOrEmpty(mergePolicy)) {
                gen.node("merge-policy-class-name", mergePolicy);
            }

            List<String> filters = wan.getFilters();
            if (CollectionUtil.isNotEmpty(filters)) {
                gen.open("filters");
                for (String f : filters) {
                    gen.node("filter-impl", f);
                }
                gen.close();
            }
            gen.node("republishing-enabled", wan.isRepublishingEnabled())
                    .close();
        }
    }

    private static void mapStoreConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, MapConfig m) {
        if (m.getMapStoreConfig() != null) {
            MapStoreConfig s = m.getMapStoreConfig();
            String clazz = s.getImplementation()
                    != null ? s.getImplementation().getClass().getName() : s.getClassName();
            String factoryClass = s.getFactoryImplementation() != null
                    ? s.getFactoryImplementation().getClass().getName()
                    : s.getFactoryClassName();
            MapStoreConfig.InitialLoadMode initialMode = s.getInitialLoadMode();

            gen.open("map-store", "enabled", s.isEnabled(), "initial-mode", initialMode.toString())
                    .node("class-name", clazz)
                    .node("factory-class-name", factoryClass)
                    .node("write-coalescing", s.isWriteCoalescing())
                    .node("write-delay-seconds", s.getWriteDelaySeconds())
                    .node("write-batch-size", s.getWriteBatchSize())
                    .appendProperties(s.getProperties())
                    .close();
        }
    }

    private static void mapNearCacheConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, NearCacheConfig n) {
        if (n != null) {
            if (n.getName() != null) {
                gen.open("near-cache", "name", n.getName());
            } else {
                gen.open("near-cache");
            }

            gen.node("in-memory-format", n.getInMemoryFormat())
                    .node("invalidate-on-change", n.isInvalidateOnChange())
                    .node("time-to-live-seconds", n.getTimeToLiveSeconds())
                    .node("max-idle-seconds", n.getMaxIdleSeconds())
                    .node("serialize-keys", n.isSerializeKeys())
                    .node("cache-local-entries", n.isCacheLocalEntries());

            evictionConfigXmlGenerator(gen, n.getEvictionConfig());
            gen.close();
        }
    }

    private static void evictionConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, EvictionConfig e) {
        if (e == null) {
            return;
        }

        String comparatorClassName = !isNullOrEmpty(e.getComparatorClassName()) ? e.getComparatorClassName() : null;
        gen.node("eviction", null,
                "size", e.getSize(),
                "max-size-policy", e.getMaxSizePolicy(),
                "eviction-policy", e.getEvictionPolicy(),
                "comparator-class-name", comparatorClassName);
    }

    private static void entryListenerConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen,
                                                        List<EntryListenerConfig> entryListenerConfigs) {
        if (!entryListenerConfigs.isEmpty()) {
            gen.open("entry-listeners");
            for (EntryListenerConfig lc : entryListenerConfigs) {
                gen.node("entry-listener", classNameOrImplClass(lc.getClassName(), lc.getImplementation()),
                        "include-value", lc.isIncludeValue(), "local", lc.isLocal());
            }
            gen.close();
        }
    }

    public static void aliasedDiscoveryConfigsGenerator(ConfigXmlGenerator.XmlGenerator gen,
                                                        List<AliasedDiscoveryConfig<?>> configs) {
        if (configs == null) {
            return;
        }
        for (AliasedDiscoveryConfig<?> c : configs) {
            gen.open(AliasedDiscoveryConfigUtils.tagFor(c), "enabled", c.isEnabled());
            if (c.isUsePublicIp()) {
                gen.node("use-public-ip", "true");
            }
            for (String key : c.getProperties().keySet()) {
                gen.node(key, c.getProperties().get(key));
            }
            gen.close();
        }
    }

    public static void discoveryStrategyConfigXmlGenerator(ConfigXmlGenerator.XmlGenerator gen, DiscoveryConfig c) {
        if (c == null) {
            return;
        }
        gen.open("discovery-strategies");
        String nodeFilterClass = classNameOrImplClass(c.getNodeFilterClass(), c.getNodeFilter());
        if (nodeFilterClass != null) {
            gen.node("node-filter", null, "class", nodeFilterClass);
        }

        Collection<DiscoveryStrategyConfig> configs = c.getDiscoveryStrategyConfigs();
        if (CollectionUtil.isNotEmpty(configs)) {
            for (DiscoveryStrategyConfig config : configs) {
                gen.open("discovery-strategy",
                                "class", classNameOrImplClass(config.getClassName(), config.getDiscoveryStrategyFactory()),
                                "enabled", "true")
                        .appendProperties(config.getProperties())
                        .close();
            }
        }
        gen.close();
    }

    public static String classNameOrImplClass(String className, Object impl) {
        return !isNullOrEmpty(className) ? className
                : impl != null ? impl.getClass().getName()
                : null;
    }

    private static void appendItemListenerConfigs(ConfigXmlGenerator.XmlGenerator gen, Collection<ItemListenerConfig> configs) {
        if (CollectionUtil.isNotEmpty(configs)) {
            gen.open("item-listeners");
            for (ItemListenerConfig lc : configs) {
                gen.node("item-listener", classNameOrImplClass(lc.getClassName(), lc.getImplementation()),
                        "include-value", lc.isIncludeValue());
            }
            gen.close();
        }
    }
}
