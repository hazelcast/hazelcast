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

package com.hazelcast.config;

import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.MapUtil;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.Preconditions.isNotNull;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;

/**
 * The ConfigXmlGenerator is responsible for transforming a {@link Config} to a Hazelcast XML string.
 */
public class ConfigXmlGenerator {

    protected static final String MASK_FOR_SESITIVE_DATA = "****";

    private static final ILogger LOGGER = Logger.getLogger(ConfigXmlGenerator.class);

    private static final int INDENT = 5;

    private final boolean formatted;

    /**
     * Creates a ConfigXmlGenerator that will format the code.
     */
    public ConfigXmlGenerator() {
        this(true);
    }

    /**
     * Creates a ConfigXmlGenerator.
     *
     * @param formatted true if the XML should be formatted, false otherwise.
     */
    public ConfigXmlGenerator(boolean formatted) {
        this.formatted = formatted;
    }

    /**
     * Generates the XML string based on some Config.
     *
     * @param config the configuration.
     * @return the XML string.
     */
    public String generate(Config config) {
        isNotNull(config, "Config");

        final StringBuilder xml = new StringBuilder();
        final XmlGenerator gen = new XmlGenerator(xml);

        xml.append("<hazelcast ")
                .append("xmlns=\"http://www.hazelcast.com/schema/config\"\n")
                .append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n")
                .append("xsi:schemaLocation=\"http://www.hazelcast.com/schema/config ")
                .append("http://www.hazelcast.com/schema/config/hazelcast-config-3.9.xsd\">");
        gen.open("group")
                .node("name", config.getGroupConfig().getName())
                .node("password", MASK_FOR_SESITIVE_DATA)
                .close()
                .node("license-key", MASK_FOR_SESITIVE_DATA)
                .node("instance-name", config.getInstanceName());

        if (config.getManagementCenterConfig() != null) {
            final ManagementCenterConfig mcConfig = config.getManagementCenterConfig();
            gen.node("management-center", mcConfig.getUrl(),
                    "enabled", mcConfig.isEnabled(),
                    "update-interval", mcConfig.getUpdateInterval());
        }
        gen.appendProperties(config.getProperties());
        wanReplicationXmlGenerator(gen, config);
        networkConfigXmlGenerator(gen, config);
        mapConfigXmlGenerator(gen, config);
        replicatedMapConfigXmlGenerator(gen, config);
        cacheConfigXmlGenerator(gen, config);
        queueXmlGenerator(gen, config);
        multiMapXmlGenerator(gen, config);
        collectionXmlGenerator(gen, "list", config.getListConfigs().values());
        collectionXmlGenerator(gen, "set", config.getSetConfigs().values());
        topicXmlGenerator(gen, config);
        semaphoreXmlGenerator(gen, config);
        lockXmlGenerator(gen, config);
        ringbufferXmlGenerator(gen, config);
        executorXmlGenerator(gen, config);
        durableExecutorXmlGenerator(gen, config);
        scheduledExecutorXmlGenerator(gen, config);
        partitionGroupXmlGenerator(gen, config);
        cardinalityEstimatorXmlGenerator(gen, config);
        listenerXmlGenerator(gen, config);
        serializationXmlGenerator(gen, config);
        reliableTopicXmlGenerator(gen, config);
        liteMemberXmlGenerator(gen, config);
        nativeMemoryXmlGenerator(gen, config);
        servicesXmlGenerator(gen, config);
        hotRestartXmlGenerator(gen, config);

        xml.append("</hazelcast>");

        return format(xml.toString(), INDENT);
    }

    private static void collectionXmlGenerator(XmlGenerator gen, String type, Collection<? extends CollectionConfig> configs) {
        if (CollectionUtil.isNotEmpty(configs)) {
            for (CollectionConfig c : configs) {
                gen.open(type, "name", c.getName())
                        .node("statistics-enabled", c.isStatisticsEnabled())
                        .node("max-size", c.getMaxSize())
                        .node("backup-count", c.getBackupCount())
                        .node("async-backup-count", c.getAsyncBackupCount());
                appendItemListenerConfigs(gen, c.getItemListenerConfigs());
                gen.close();
            }
        }
    }

    private static void replicatedMapConfigXmlGenerator(XmlGenerator gen, Config config) {
        for (ReplicatedMapConfig r : config.getReplicatedMapConfigs().values()) {
            gen.open("replicatedmap", "name", r.getName())
                    .node("in-memory-format", r.getInMemoryFormat())
                    .node("concurrency-level", r.getConcurrencyLevel())
                    .node("replication-delay-millis", r.getReplicationDelayMillis())
                    .node("async-fillup", r.isAsyncFillup())
                    .node("statistics-enabled", r.isStatisticsEnabled());

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

    private static void listenerXmlGenerator(XmlGenerator gen, Config config) {
        if (config.getListenerConfigs().isEmpty()) {
            return;
        }
        gen.open("listeners");
        for (ListenerConfig lc : config.getListenerConfigs()) {
            gen.node("listener", classNameOrImplClass(lc.getClassName(), lc.getImplementation()));
        }
        gen.close();
    }

    @SuppressWarnings({"checkstyle:npathcomplexity"})
    private static void serializationXmlGenerator(XmlGenerator gen, Config config) {
        final SerializationConfig c = config.getSerializationConfig();
        if (c == null) {
            return;
        }
        gen.open("serialization")
                .node("portable-version", c.getPortableVersion())
                .node("use-native-byte-order", c.isUseNativeByteOrder())
                .node("byte-order", c.getByteOrder())
                .node("enable-compression", c.isEnableCompression())
                .node("enable-shared-object", c.isEnableSharedObject())
                .node("allow-unsafe", c.isAllowUnsafe());

        final Map<Integer, String> dsfClasses = c.getDataSerializableFactoryClasses();
        final Map<Integer, DataSerializableFactory> dsfImpls = c.getDataSerializableFactories();
        if (!MapUtil.isNullOrEmpty(dsfClasses) || !MapUtil.isNullOrEmpty(dsfImpls)) {
            gen.open("data-serializable-factories");
            appendSerializationFactory(gen, "data-serializable-factory", dsfClasses);
            appendSerializationFactory(gen, "data-serializable-factory", dsfImpls);
            gen.close();
        }

        final Map<Integer, String> portableClasses = c.getPortableFactoryClasses();
        final Map<Integer, PortableFactory> portableImpls = c.getPortableFactories();
        if (!MapUtil.isNullOrEmpty(portableClasses) || !MapUtil.isNullOrEmpty(portableImpls)) {
            gen.open("portable-factories");
            appendSerializationFactory(gen, "portable-factory", portableClasses);
            appendSerializationFactory(gen, "portable-factory", portableImpls);
            gen.close();
        }

        final Collection<SerializerConfig> serializers = c.getSerializerConfigs();
        final GlobalSerializerConfig globalSerializerConfig = c.getGlobalSerializerConfig();
        if (CollectionUtil.isNotEmpty(serializers) || globalSerializerConfig != null) {
            gen.open("serializers");

            if (globalSerializerConfig != null) {
                gen.node("global-serializer",
                        classNameOrImplClass(
                                globalSerializerConfig.getClassName(), globalSerializerConfig.getImplementation()),
                        "override-java-serialization", globalSerializerConfig.isOverrideJavaSerialization());
            }

            if (CollectionUtil.isNotEmpty(serializers)) {
                for (SerializerConfig serializer : serializers) {
                    gen.node("serializer", null,
                            "type-class", classNameOrClass(serializer.getTypeClassName(), serializer.getTypeClass()),
                            "class-name", classNameOrImplClass(serializer.getClassName(), serializer.getImplementation()));
                }
            }
            gen.close();
        }
        gen.node("check-class-def-errors", c.isCheckClassDefErrors())
                .close();
    }

    private static String classNameOrClass(String className, Class clazz) {
        return !isNullOrEmpty(className) ? className
                : clazz != null ? clazz.getName()
                : null;
    }

    private static String classNameOrImplClass(String className, Object impl) {
        return !isNullOrEmpty(className) ? className
                : impl != null ? impl.getClass().getName()
                : null;
    }

    private static void partitionGroupXmlGenerator(XmlGenerator gen, Config config) {
        final PartitionGroupConfig pg = config.getPartitionGroupConfig();
        if (pg == null) {
            return;
        }
        gen.open("partition-group",
                "enabled", pg.isEnabled(), "group-type", pg.getGroupType());

        final Collection<MemberGroupConfig> configs = pg.getMemberGroupConfigs();
        if (CollectionUtil.isNotEmpty(configs)) {
            for (MemberGroupConfig mgConfig : configs) {
                gen.open("member-group");
                for (String iface : mgConfig.getInterfaces()) {
                    gen.node("interface", iface);
                }
                gen.close();
            }
        }
        gen.close();
    }

    private static void executorXmlGenerator(XmlGenerator gen, Config config) {
        for (ExecutorConfig ex : config.getExecutorConfigs().values()) {
            gen.open("executor-service", "name", ex.getName())
                    .node("statistics-enabled", ex.isStatisticsEnabled())
                    .node("pool-size", ex.getPoolSize())
                    .node("queue-capacity", ex.getQueueCapacity())
                    .close();
        }
    }

    private static void durableExecutorXmlGenerator(XmlGenerator gen, Config config) {
        for (DurableExecutorConfig ex : config.getDurableExecutorConfigs().values()) {
            gen.open("durable-executor-service", "name", ex.getName())
                    .node("pool-size", ex.getPoolSize())
                    .node("durability", ex.getDurability())
                    .node("capacity", ex.getCapacity())
                    .close();
        }
    }

    private static void scheduledExecutorXmlGenerator(XmlGenerator gen, Config config) {
        for (ScheduledExecutorConfig ex : config.getScheduledExecutorConfigs().values()) {
            gen.open("scheduled-executor-service", "name", ex.getName())
                    .node("pool-size", ex.getPoolSize())
                    .node("durability", ex.getDurability())
                    .node("capacity", ex.getCapacity())
                    .close();
        }
    }

    private static void cardinalityEstimatorXmlGenerator(XmlGenerator gen, Config config) {
        for (CardinalityEstimatorConfig ex : config.getCardinalityEstimatorConfigs().values()) {
            gen.open("cardinality-estimator", "name", ex.getName())
                    .node("backup-count", ex.getBackupCount())
                    .node("async-backup-count", ex.getAsyncBackupCount())
                    .close();
        }
    }

    private static void semaphoreXmlGenerator(XmlGenerator gen, Config config) {
        for (SemaphoreConfig sc : config.getSemaphoreConfigs()) {
            gen.open("semaphore", "name", sc.getName())
                    .node("initial-permits", sc.getInitialPermits())
                    .node("backup-count", sc.getBackupCount())
                    .node("async-backup-count", sc.getAsyncBackupCount())
                    .close();
        }
    }

    private static void topicXmlGenerator(XmlGenerator gen, Config config) {
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

    private static void reliableTopicXmlGenerator(XmlGenerator gen, Config config) {
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


    private static void multiMapXmlGenerator(XmlGenerator gen, Config config) {
        for (MultiMapConfig mm : config.getMultiMapConfigs().values()) {
            gen.open("multimap", "name", mm.getName())
                    .node("backup-count", mm.getBackupCount())
                    .node("async-backup-count", mm.getAsyncBackupCount())
                    .node("statistics-enabled", mm.isStatisticsEnabled())
                    .node("binary", mm.isBinary())
                    .node("value-collection-type", mm.getValueCollectionType());

            if (!mm.getEntryListenerConfigs().isEmpty()) {
                gen.open("entry-listeners");
                for (EntryListenerConfig lc : mm.getEntryListenerConfigs()) {
                    gen.node("entry-listener", classNameOrImplClass(lc.getClassName(), lc.getImplementation()),
                            "include-value", lc.isIncludeValue(),
                            "local", lc.isLocal());
                }
                gen.close();
            }
//            if (mm.getPartitioningStrategyConfig() != null) {
//                xml.append("<partition-strategy>");
//                PartitioningStrategyConfig psc = mm.getPartitioningStrategyConfig();
//                if (psc.getPartitioningStrategy() != null) {
//                    xml.append(psc.getPartitioningStrategy().getClass().getName());
//                } else {
//                    xml.append(psc.getPartitioningStrategyClass());
//                }
//                xml.append("</partition-strategy>");
//            }
            gen.close();
        }
    }

    private static void queueXmlGenerator(XmlGenerator gen, Config config) {
        final Collection<QueueConfig> qCfgs = config.getQueueConfigs().values();
        for (QueueConfig q : qCfgs) {
            gen.open("queue", "name", q.getName())
                    .node("statistics-enabled", q.isStatisticsEnabled())
                    .node("max-size", q.getMaxSize())
                    .node("backup-count", q.getBackupCount())
                    .node("async-backup-count", q.getAsyncBackupCount())
                    .node("empty-queue-ttl", q.getEmptyQueueTtl());
            appendItemListenerConfigs(gen, q.getItemListenerConfigs());
            final QueueStoreConfig storeConfig = q.getQueueStoreConfig();
            if (storeConfig != null) {
                gen.open("queue-store", "enabled", storeConfig.isEnabled())
                        .node("class-name", storeConfig.getClassName())
                        .node("factory-class-name", storeConfig.getFactoryClassName())
                        .appendProperties(storeConfig.getProperties())
                        .close();
            }
            gen.node("quorum-ref", q.getQuorumName())
                    .close();
        }
    }

    private static void lockXmlGenerator(XmlGenerator gen, Config config) {
        for (LockConfig c : config.getLockConfigs().values()) {
            gen.open("lock", "name", c.getName())
                    .node("quorum-ref", c.getQuorumName())
                    .close();
        }
    }

    private static void ringbufferXmlGenerator(XmlGenerator gen, Config config) {
        final Collection<RingbufferConfig> configs = config.getRingbufferConfigs().values();
        for (RingbufferConfig rbConfig : configs) {
            gen.open("ringbuffer", "name", rbConfig.getName())
                    .node("capacity", rbConfig.getCapacity())
                    .node("time-to-live-seconds", rbConfig.getTimeToLiveSeconds())
                    .node("backup-count", rbConfig.getBackupCount())
                    .node("async-backup-count", rbConfig.getAsyncBackupCount())
                    .node("in-memory-format", rbConfig.getInMemoryFormat());

            final RingbufferStoreConfig storeConfig = rbConfig.getRingbufferStoreConfig();
            if (storeConfig != null) {
                gen.open("ringbuffer-store", "enabled", storeConfig.isEnabled())
                        .node("class-name", storeConfig.getClassName())
                        .node("factory-class-name", storeConfig.getFactoryClassName())
                        .appendProperties(storeConfig.getProperties());
                gen.close();
            }
            gen.close();
        }
    }

    private static void wanReplicationXmlGenerator(XmlGenerator gen, Config config) {
        for (WanReplicationConfig wan : config.getWanReplicationConfigs().values()) {
            gen.open("wan-replication", "name", wan.getName());
            for (WanPublisherConfig p : wan.getWanPublisherConfigs()) {
                gen.open("wan-publisher", "group-name", p.getGroupName())
                   .node("class-name", p.getClassName())
                   .node("queue-full-behavior", p.getQueueFullBehavior())
                   .node("queue-capacity", p.getQueueCapacity())
                   .appendProperties(p.getProperties());
                awsConfigXmlGenerator(gen, p.getAwsConfig());
                discoveryStrategyConfigXmlGenerator(gen, p.getDiscoveryConfig());
                gen.close();
            }

            final WanConsumerConfig consumerConfig = wan.getWanConsumerConfig();
            if (consumerConfig != null) {
                gen.open("wan-consumer")
                        .node("class-name", classNameOrImplClass(consumerConfig.getClassName(),
                                consumerConfig.getImplementation()))
                        .appendProperties(consumerConfig.getProperties())
                        .close();
            }
            gen.close();
        }
    }

    private static void networkConfigXmlGenerator(XmlGenerator gen, Config config) {
        final NetworkConfig netCfg = config.getNetworkConfig();
        gen.open("network")
                .node("public-address", netCfg.getPublicAddress())
                .node("port", netCfg.getPort(),
                        "port-count", netCfg.getPortCount(), "auto-increment", netCfg.isPortAutoIncrement())
                .node("reuse-address", netCfg.isReuseAddress());

        final Collection<String> outboundPortDefinitions = netCfg.getOutboundPortDefinitions();
        if (CollectionUtil.isNotEmpty(outboundPortDefinitions)) {
            gen.open("outbound-ports");
            for (String def : outboundPortDefinitions) {
                gen.node("ports", def);
            }
            gen.close();
        }


        final JoinConfig join = netCfg.getJoin();
        gen.open("join");
        multicastConfigXmlGenerator(gen, join);
        tcpConfigXmlGenerator(gen, join);
        awsConfigXmlGenerator(gen, join.getAwsConfig());
        discoveryStrategyConfigXmlGenerator(gen, join.getDiscoveryConfig());
        gen.close();

        interfacesConfigXmlGenerator(gen, netCfg);
        sslConfigXmlGenerator(gen, netCfg);
        socketInterceptorConfigXmlGenerator(gen, netCfg);
        symmetricEncInterceptorConfigXmlGenerator(gen, netCfg);
        gen.close();
    }

    private static void mapConfigXmlGenerator(XmlGenerator gen, Config config) {
        final Collection<MapConfig> mCfgs = config.getMapConfigs().values();
        for (MapConfig m : mCfgs) {
            final String cacheDeserializedVal = m.getCacheDeserializedValues() != null
                    ? m.getCacheDeserializedValues().name().replaceAll("_", "-") : null;
            gen.open("map", "name", m.getName())
                    .node("in-memory-format", m.getInMemoryFormat())
                    .node("statistics-enabled", m.isStatisticsEnabled())
                    .node("optimize-queries", m.isOptimizeQueries())
                    .node("cache-deserialized-values", cacheDeserializedVal)
                    .node("backup-count", m.getBackupCount())
                    .node("async-backup-count", m.getAsyncBackupCount())
                    .node("time-to-live-seconds", m.getTimeToLiveSeconds())
                    .node("max-idle-seconds", m.getMaxIdleSeconds())
                    .node("eviction-policy", m.getEvictionPolicy())
                    .node("max-size", m.getMaxSizeConfig().getSize(), "policy", m.getMaxSizeConfig().getMaxSizePolicy())
                    .node("eviction-percentage", m.getEvictionPercentage())
                    .node("min-eviction-check-millis", m.getMinEvictionCheckMillis())
                    .node("merge-policy", m.getMergePolicy())
                    .node("read-backup-data", m.isReadBackupData());

            appendHotRestartConfig(gen, m.getHotRestartConfig());
            mapStoreConfigXmlGenerator(gen, m);
            mapNearCacheConfigXmlGenerator(gen, m.getNearCacheConfig());
            wanReplicationConfigXmlGenerator(gen, m.getWanReplicationRef());
            mapIndexConfigXmlGenerator(gen, m);
            mapAttributeConfigXmlGenerator(gen, m);
            mapEntryListenerConfigXmlGenerator(gen, m);
            mapPartitionLostListenerConfigXmlGenerator(gen, m);
            mapPartitionStrategyConfigXmlGenerator(gen, m);
            gen.close();
        }
    }

    private static void appendHotRestartConfig(XmlGenerator gen, HotRestartConfig m) {
        gen.open("hot-restart", "enabled", m != null && m.isEnabled())
                .node("fsync", m != null && m.isFsync())
                .close();
    }

    private static void cacheConfigXmlGenerator(XmlGenerator gen, Config config) {
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

            gen.node("quorum-ref", c.getQuorumName());
            cachePartitionLostListenerConfigXmlGenerator(gen, c.getPartitionLostListenerConfigs());

            gen.node("merge-policy", c.getMergePolicy());
            appendHotRestartConfig(gen, c.getHotRestartConfig());

            gen.node("disable-per-entry-invalidation-events", c.isDisablePerEntryInvalidationEvents())
                    .close();
        }
    }

    private static void checkAndFillCacheWriterFactoryConfigXml(XmlGenerator gen, String cacheWriter) {
        if (isNullOrEmpty(cacheWriter)) {
            return;
        }
        gen.node("cache-writer-factory", null, "class-name", cacheWriter);
    }

    private static void checkAndFillCacheWriterConfigXml(XmlGenerator gen, String cacheWriter) {
        if (isNullOrEmpty(cacheWriter)) {
            return;
        }
        gen.node("cache-writer", null, "class-name", cacheWriter);
    }

    private static void checkAndFillCacheLoaderFactoryConfigXml(XmlGenerator gen, String cacheLoader) {
        if (isNullOrEmpty(cacheLoader)) {
            return;
        }
        gen.node("cache-loader-factory", null, "class-name", cacheLoader);
    }

    private static void checkAndFillCacheLoaderConfigXml(XmlGenerator gen, String cacheLoader) {
        if (isNullOrEmpty(cacheLoader)) {
            return;
        }
        gen.node("cache-loader", null, "class-name", cacheLoader);
    }

    private static void cacheExpiryPolicyFactoryConfigXmlGenerator(XmlGenerator gen,
                                                                   ExpiryPolicyFactoryConfig config) {
        if (config == null) {
            return;
        }
        if (!isNullOrEmpty(config.getClassName())) {
            gen.node("expiry-policy-factory", null, "class-name", config.getClassName());
        } else {
            final TimedExpiryPolicyFactoryConfig timedConfig = config.getTimedExpiryPolicyFactoryConfig();
            if (timedConfig != null
                    && timedConfig.getExpiryPolicyType() != null && timedConfig.getDurationConfig() != null) {
                final DurationConfig duration = timedConfig.getDurationConfig();
                gen.open("expiry-policy-factory")
                        .node("timed-expiry-policy-factory", null,
                                "expiry-policy-type", timedConfig.getExpiryPolicyType(),
                                "duration-amount", duration.getDurationAmount(),
                                "time-unit", duration.getTimeUnit().name())
                        .close();
            }
        }
    }

    private static void cachePartitionLostListenerConfigXmlGenerator(XmlGenerator gen,
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


    private static void mapPartitionStrategyConfigXmlGenerator(XmlGenerator gen, MapConfig m) {
        if (m.getPartitioningStrategyConfig() != null) {
            final PartitioningStrategyConfig psc = m.getPartitioningStrategyConfig();
            gen.node("partition-strategy",
                    classNameOrImplClass(psc.getPartitioningStrategyClass(), psc.getPartitioningStrategy()));
        }
    }

    private static void mapEntryListenerConfigXmlGenerator(XmlGenerator gen, MapConfig m) {
        if (!m.getEntryListenerConfigs().isEmpty()) {
            gen.open("entry-listeners");
            for (EntryListenerConfig lc : m.getEntryListenerConfigs()) {
                gen.node("entry-listener", classNameOrImplClass(lc.getClassName(), lc.getImplementation()),
                        "include-value", lc.isIncludeValue(), "local", lc.isLocal());
            }
            gen.close();
        }
    }

    private static void mapPartitionLostListenerConfigXmlGenerator(XmlGenerator gen, MapConfig m) {
        if (!m.getPartitionLostListenerConfigs().isEmpty()) {
            gen.open("partition-lost-listeners");
            for (MapPartitionLostListenerConfig c : m.getPartitionLostListenerConfigs()) {
                gen.node("partition-lost-listener",
                        classNameOrImplClass(c.getClassName(), c.getImplementation()));
            }
            gen.close();
        }
    }

    private static void mapIndexConfigXmlGenerator(XmlGenerator gen, MapConfig m) {
        if (!m.getMapIndexConfigs().isEmpty()) {
            gen.open("indexes");
            for (MapIndexConfig indexCfg : m.getMapIndexConfigs()) {
                gen.node("index", indexCfg.getAttribute(), "ordered", indexCfg.isOrdered());
            }
            gen.close();
        }
    }

    private static void mapAttributeConfigXmlGenerator(XmlGenerator gen, MapConfig m) {
        if (!m.getMapAttributeConfigs().isEmpty()) {
            gen.open("attributes");
            for (MapAttributeConfig attributeCfg : m.getMapAttributeConfigs()) {
                gen.node("attribute", attributeCfg.getName(), "extractor", attributeCfg.getExtractor());
            }
            gen.close();
        }
    }

    private static void wanReplicationConfigXmlGenerator(XmlGenerator gen, WanReplicationRef wan) {
        if (wan != null) {
            gen.open("wan-replication-ref", "name", wan.getName())
                    .node("merge-policy", wan.getMergePolicy());

            final List<String> filters = wan.getFilters();
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

    private static void mapStoreConfigXmlGenerator(XmlGenerator gen, MapConfig m) {
        if (m.getMapStoreConfig() != null) {
            final MapStoreConfig s = m.getMapStoreConfig();
            final String clazz = s.getImplementation()
                    != null ? s.getImplementation().getClass().getName() : s.getClassName();
            final String factoryClass = s.getFactoryImplementation() != null
                    ? s.getFactoryImplementation().getClass().getName()
                    : s.getFactoryClassName();

            gen.open("map-store", "enabled", s.isEnabled())
                    .node("class-name", clazz)
                    .node("factory-class-name", factoryClass)
                    .node("write-delay-seconds", s.getWriteDelaySeconds())
                    .node("write-batch-size", s.getWriteBatchSize())
                    .appendProperties(s.getProperties())
                    .close();
        }
    }

    private static void mapNearCacheConfigXmlGenerator(XmlGenerator gen, NearCacheConfig n) {
        if (n != null) {
            gen.open("near-cache")
                    .node("in-memory-format", n.getInMemoryFormat())
                    .node("invalidate-on-change", n.isInvalidateOnChange())
                    .node("time-to-live-seconds", n.getTimeToLiveSeconds())
                    .node("max-idle-seconds", n.getMaxIdleSeconds());
            evictionConfigXmlGenerator(gen, n.getEvictionConfig());
            gen
                    .node("eviction-policy", n.getEvictionPolicy())
                    .node("max-size", n.getMaxSize())
                    .node("cache-local-entries", n.isCacheLocalEntries());
            gen.close();
        }
    }

    private static void evictionConfigXmlGenerator(XmlGenerator gen, EvictionConfig e) {
        if (e == null) {
            return;
        }
        final String comparatorClassName = !isNullOrEmpty(e.getComparatorClassName()) ? e.getComparatorClassName() : null;
        gen.node("eviction", null,
                "size", e.getSize(),
                "max-size-policy", e.getMaximumSizePolicy(),
                "eviction-policy", e.getEvictionPolicy(),
                "comparator-class-name", comparatorClassName);
    }

    private static void multicastConfigXmlGenerator(XmlGenerator gen, JoinConfig join) {
        final MulticastConfig mcast = join.getMulticastConfig();
        gen.open("multicast", "enabled", mcast.isEnabled(), "loopbackModeEnabled", mcast.isLoopbackModeEnabled())
                .node("multicast-group", mcast.getMulticastGroup())
                .node("multicast-port", mcast.getMulticastPort())
                .node("multicast-timeout-seconds", mcast.getMulticastTimeoutSeconds())
                .node("multicast-time-to-live", mcast.getMulticastTimeToLive());

        if (!mcast.getTrustedInterfaces().isEmpty()) {
            gen.open("trusted-interfaces");
            for (String trustedInterface : mcast.getTrustedInterfaces()) {
                gen.node("interface", trustedInterface);
            }
            gen.close();
        }
        gen.close();
    }

    private static void tcpConfigXmlGenerator(XmlGenerator gen, JoinConfig join) {
        final TcpIpConfig c = join.getTcpIpConfig();
        gen.open("tcp-ip", "enabled", c.isEnabled(), "connection-timeout-seconds", c.getConnectionTimeoutSeconds())
                .open("member-list");
        for (String m : c.getMembers()) {
            gen.node("member", m);
        }
        gen.close()
                .node("required-member", c.getRequiredMember())
                .close();
    }

    private static void awsConfigXmlGenerator(XmlGenerator gen, AwsConfig c) {
        if (c == null) {
            return;
        }
        gen.open("aws", "enabled", c.isEnabled())
                .node("access-key", c.getAccessKey())
                .node("secret-key", c.getSecretKey())
                .node("iam-role", c.getIamRole())
                .node("region", c.getRegion())
                .node("host-header", c.getHostHeader())
                .node("security-group-name", c.getSecurityGroupName())
                .node("tag-key", c.getTagKey())
                .node("tag-value", c.getTagValue())
                .close();
    }

    private static void discoveryStrategyConfigXmlGenerator(XmlGenerator gen, DiscoveryConfig c) {
        if (c == null) {
            return;
        }
        gen.open("discovery-strategies");
        final String nodeFilterClass = classNameOrImplClass(c.getNodeFilterClass(), c.getNodeFilter());
        if (nodeFilterClass != null) {
            gen.node("node-filter", null, "class", nodeFilterClass);
        }

        final Collection<DiscoveryStrategyConfig> configs = c.getDiscoveryStrategyConfigs();
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

    private static void interfacesConfigXmlGenerator(XmlGenerator gen, NetworkConfig netCfg) {
        final InterfacesConfig interfaces = netCfg.getInterfaces();
        gen.open("interfaces", "enabled", interfaces.isEnabled());
        for (String i : interfaces.getInterfaces()) {
            gen.node("interface", i);
        }
        gen.close();
    }

    private static void sslConfigXmlGenerator(XmlGenerator gen, NetworkConfig netCfg) {
        final SSLConfig ssl = netCfg.getSSLConfig();
        gen.open("ssl", "enabled", ssl != null && ssl.isEnabled());
        if (ssl != null) {

            Properties props = new Properties();
            props.putAll(ssl.getProperties());

            if (props.containsKey("keyStorePassword")) {
                props.setProperty("keyStorePassword", MASK_FOR_SESITIVE_DATA);
            }

            gen.node("factory-class-name",
                    classNameOrImplClass(ssl.getFactoryClassName(), ssl.getFactoryImplementation()))
               .appendProperties(props);
        }
        gen.close();
    }

    private static void socketInterceptorConfigXmlGenerator(XmlGenerator gen, NetworkConfig netCfg) {
        final SocketInterceptorConfig socket = netCfg.getSocketInterceptorConfig();
        gen.open("socket-interceptor", "enabled", socket != null && socket.isEnabled());
        if (socket != null) {
            gen.node("class-name", classNameOrImplClass(socket.getClassName(), socket.getImplementation()))
                    .appendProperties(socket.getProperties());
        }
        gen.close();
    }

    private static void symmetricEncInterceptorConfigXmlGenerator(XmlGenerator gen, NetworkConfig netCfg) {
        final SymmetricEncryptionConfig sec = netCfg.getSymmetricEncryptionConfig();
        if (sec == null) {
            return;
        }
        gen.open("symmetric-encryption", "enabled", sec.isEnabled())
                .node("algorithm", sec.getAlgorithm())
                .node("salt", MASK_FOR_SESITIVE_DATA)
                .node("password", MASK_FOR_SESITIVE_DATA)
                .node("iteration-count", sec.getIterationCount())
                .close();
    }

    private static void hotRestartXmlGenerator(XmlGenerator gen, Config config) {
        HotRestartPersistenceConfig hrCfg = config.getHotRestartPersistenceConfig();
        if (hrCfg == null) {
            gen.node("hot-restart-persistence", "enabled", "false");
            return;
        }
        gen.open("hot-restart-persistence", "enabled", hrCfg.isEnabled())
                .node("base-dir", hrCfg.getBaseDir().getAbsolutePath());
        if (hrCfg.getBackupDir() != null) {
            gen.node("backup-dir", hrCfg.getBackupDir().getAbsolutePath());
        }
        gen.node("parallelism", hrCfg.getParallelism())
                .node("validation-timeout-seconds", hrCfg.getValidationTimeoutSeconds())
                .node("data-load-timeout-seconds", hrCfg.getDataLoadTimeoutSeconds())
                .node("cluster-data-recovery-policy", hrCfg.getClusterDataRecoveryPolicy())
                .close();
    }

    private static void nativeMemoryXmlGenerator(XmlGenerator gen, Config config) {
        NativeMemoryConfig nativeMemoryConfig = config.getNativeMemoryConfig();
        if (nativeMemoryConfig == null) {
            gen.node("native-memory", null, "enabled", "false");
            return;
        }
        gen.open("native-memory",
                "enabled", nativeMemoryConfig.isEnabled(),
                "allocator-type", nativeMemoryConfig.getAllocatorType())
                .node("size", null,
                        "unit", nativeMemoryConfig.getSize().getUnit(),
                        "value", nativeMemoryConfig.getSize().getValue())
                .node("min-block-size", nativeMemoryConfig.getMinBlockSize())
                .node("page-size", nativeMemoryConfig.getPageSize())
                .node("metadata-space-percentage", nativeMemoryConfig.getMetadataSpacePercentage())
                .close();
    }

    private static void servicesXmlGenerator(XmlGenerator gen, Config config) {
        final ServicesConfig c = config.getServicesConfig();
        if (c == null) {
            return;
        }
        gen.open("services", "enable-defaults", c.isEnableDefaults());
        if (CollectionUtil.isNotEmpty(c.getServiceConfigs())) {
            for (ServiceConfig serviceConfig : c.getServiceConfigs()) {
                gen.open("service", "enabled", serviceConfig.isEnabled())
                        .node("name", serviceConfig.getName())
                        .node("class-name",
                                classNameOrImplClass(serviceConfig.getClassName(), serviceConfig.getImplementation()))
                        .appendProperties(serviceConfig.getProperties())
                        .close();
            }
        }
        gen.close();
    }

    private static void liteMemberXmlGenerator(XmlGenerator gen, Config config) {
        gen.node("lite-member", null, "enabled", config.isLiteMember());
    }

    private String format(final String input, int indent) {
        if (!formatted) {
            return input;
        }
        StreamResult xmlOutput = null;
        try {
            final Source xmlInput = new StreamSource(new StringReader(input));
            xmlOutput = new StreamResult(new StringWriter());
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            /* Older versions of Xalan still use this method of setting indent values.
            * Attempt to make this work but don't completely fail if it's a problem.
            */
            try {
                transformerFactory.setAttribute("indent-number", indent);
            } catch (IllegalArgumentException e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Failed to set indent-number attribute; cause: " + e.getMessage());
                }
            }
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            /* Newer versions of Xalan will look for a fully-qualified output property in order to specify amount of
            * indentation to use.  Attempt to make this work as well but again don't completely fail if it's a problem.
            */
            try {
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", Integer.toString(indent));
            } catch (IllegalArgumentException e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Failed to set indent-amount property; cause: " + e.getMessage());
                }
            }
            transformer.transform(xmlInput, xmlOutput);
            return xmlOutput.getWriter().toString();
        } catch (Exception e) {
            LOGGER.warning(e);
            return input;
        } finally {
            if (xmlOutput != null) {
                closeResource(xmlOutput.getWriter());
            }
        }
    }

    private static void appendItemListenerConfigs(XmlGenerator gen, Collection<ItemListenerConfig> configs) {
        if (CollectionUtil.isNotEmpty(configs)) {
            gen.open("item-listeners");
            for (ItemListenerConfig lc : configs) {
                gen.node("item-listener", lc.getClassName(), "include-value", lc.isIncludeValue());
            }
            gen.close();
        }
    }

    private static void appendSerializationFactory(XmlGenerator gen, String elementName, Map<Integer, ?> factoryMap) {
        if (MapUtil.isNullOrEmpty(factoryMap)) {
            return;
        }
        for (Entry<Integer, ?> factory : factoryMap.entrySet()) {
            final Object value = factory.getValue();
            final String className = value instanceof String ? (String) value : value.getClass().getName();
            gen.node(elementName, className, "factory-id", factory.getKey().toString());
        }
    }

    private static final class XmlGenerator {
        private final StringBuilder xml;
        private final ArrayDeque<String> openNodes = new ArrayDeque<String>();

        private XmlGenerator(StringBuilder xml) {
            this.xml = xml;
        }

        XmlGenerator open(String name, Object... attributes) {
            appendOpenNode(xml, name, attributes);
            openNodes.addLast(name);
            return this;
        }

        XmlGenerator node(String name, Object contents, Object... attributes) {
            appendNode(xml, name, contents, attributes);
            return this;
        }

        XmlGenerator close() {
            appendCloseNode(xml, openNodes.pollLast());
            return this;
        }

        XmlGenerator appendProperties(Properties props) {
            if (!props.isEmpty()) {
                open("properties");
                Set keys = props.keySet();
                for (Object key : keys) {
                    node("property", props.getProperty(key.toString()), "name", key.toString());
                }
                close();
            }
            return this;
        }

        XmlGenerator appendProperties(Map<String, Comparable> props) {
            if (!MapUtil.isNullOrEmpty(props)) {
                open("properties");
                for (Entry<String, Comparable> entry : props.entrySet()) {
                    node("property", entry.getValue(), "name", entry.getKey());
                }
                close();
            }
            return this;
        }

        private static void appendOpenNode(StringBuilder xml, String name, Object... attributes) {
            xml.append('<').append(name);
            appendAttributes(xml, attributes);
            xml.append('>');
        }

        private static void appendCloseNode(StringBuilder xml, String name) {
            xml.append("</").append(name).append('>');
        }

        private static void appendAttributes(StringBuilder xml, Object... attributes) {
            for (int i = 0; i < attributes.length; ) {
                xml.append(" ").append(attributes[i++]).append("=\"").append(attributes[i++]).append("\"");
            }
        }

        private static void appendNode(StringBuilder xml, String name, Object contents, Object... attributes) {
            if (contents != null || attributes.length > 0) {
                xml.append('<').append(name);
                for (int i = 0; i < attributes.length; ) {
                    final Object key = attributes[i++];
                    final Object val = attributes[i++];
                    if (val != null) {
                        xml.append(" ").append(key).append("=\"").append(val).append("\"");
                    }
                }
                if (contents != null) {
                    xml.append('>').append(contents).append("</").append(name).append('>');
                } else {
                    xml.append("/>");
                }
            }
        }
    }
}
