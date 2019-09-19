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

import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.splitbrainprotection.impl.ProbabilisticSplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.impl.RecentlyActiveSplitBrainProtectionFunction;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.internal.util.MapUtil;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.config.AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom;
import static com.hazelcast.config.PermissionConfig.PermissionType.ALL;
import static com.hazelcast.config.PermissionConfig.PermissionType.CONFIG;
import static com.hazelcast.config.PermissionConfig.PermissionType.TRANSACTION;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.Preconditions.isNotNull;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;
import static java.util.Arrays.asList;

/**
 * The ConfigXmlGenerator is responsible for transforming a {@link Config} to a Hazelcast XML string.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class ConfigXmlGenerator {

    protected static final String MASK_FOR_SENSITIVE_DATA = "****";

    private static final int INDENT = 5;

    private static final ILogger LOGGER = Logger.getLogger(ConfigXmlGenerator.class);

    private final boolean formatted;
    private final boolean maskSensitiveFields;

    /**
     * Creates a ConfigXmlGenerator that will format the code.
     */
    public ConfigXmlGenerator() {
        this(true);
    }

    /**
     * Creates a ConfigXmlGenerator.
     *
     * @param formatted {@code true} if the XML should be formatted, {@code false} otherwise
     */
    public ConfigXmlGenerator(boolean formatted) {
        this(formatted, true);
    }

    /**
     * Creates a ConfigXmlGenerator.
     *
     * @param formatted           {@code true} if the XML should be formatted, {@code false} otherwise
     * @param maskSensitiveFields {@code true} if the sensitive fields (like passwords) should be masked in the
     *                            output XML, {@code false} otherwise
     */
    public ConfigXmlGenerator(boolean formatted, boolean maskSensitiveFields) {
        this.formatted = formatted;
        this.maskSensitiveFields = maskSensitiveFields;
    }

    /**
     * Generates the XML string based on some Config.
     *
     * @param config the configuration
     * @return the XML string
     */
    public String generate(Config config) {
        isNotNull(config, "Config");

        StringBuilder xml = new StringBuilder();
        XmlGenerator gen = new XmlGenerator(xml);

        xml.append("<hazelcast ")
                .append("xmlns=\"http://www.hazelcast.com/schema/config\"\n")
                .append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n")
                .append("xsi:schemaLocation=\"http://www.hazelcast.com/schema/config ")
                .append("http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd\">");
        gen.open("group")
                .node("name", config.getGroupConfig().getName())
                .node("password", getOrMaskValue(config.getGroupConfig().getPassword()))
                .close()
                .node("license-key", getOrMaskValue(config.getLicenseKey()))
                .node("instance-name", config.getInstanceName());

        manCenterXmlGenerator(gen, config);
        gen.appendProperties(config.getProperties());
        securityXmlGenerator(gen, config);
        wanReplicationXmlGenerator(gen, config);
        networkConfigXmlGenerator(gen, config);
        advancedNetworkConfigXmlGenerator(gen, config);
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
        atomicLongXmlGenerator(gen, config);
        atomicReferenceXmlGenerator(gen, config);
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
        flakeIdGeneratorXmlGenerator(gen, config);
        crdtReplicationXmlGenerator(gen, config);
        pnCounterXmlGenerator(gen, config);
        splitBrainProtectionXmlGenerator(gen, config);
        cpSubsystemConfig(gen, config);
        userCodeDeploymentConfig(gen, config);

        xml.append("</hazelcast>");

        return format(xml.toString(), INDENT);
    }

    private String getOrMaskValue(String value) {
        return maskSensitiveFields ? MASK_FOR_SENSITIVE_DATA : value;
    }

    private void manCenterXmlGenerator(XmlGenerator gen, Config config) {
        if (config.getManagementCenterConfig() != null) {
            ManagementCenterConfig mcConfig = config.getManagementCenterConfig();
            gen.open("management-center",
                    "enabled", mcConfig.isEnabled(),
                    "scripting-enabled", mcConfig.isScriptingEnabled(),
                    "update-interval", mcConfig.getUpdateInterval());
            gen.node("url", mcConfig.getUrl());
            if (mcConfig.getUrl() != null) {
                mcMutualAuthConfigXmlGenerator(gen, config.getManagementCenterConfig());
            }
            gen.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static void collectionXmlGenerator(XmlGenerator gen, String type, Collection<? extends CollectionConfig> configs) {
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

    @SuppressWarnings("deprecation")
    private static void replicatedMapConfigXmlGenerator(XmlGenerator gen, Config config) {
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

    private static void securityXmlGenerator(XmlGenerator gen, Config config) {
        SecurityConfig c = config.getSecurityConfig();
        if (c == null) {
            return;
        }

        gen.open("security", "enabled", c.isEnabled())
                .node("client-block-unmapped-actions", c.getClientBlockUnmappedActions());

        PermissionPolicyConfig ppc = c.getClientPolicyConfig();
        if (ppc.getClassName() != null) {
            gen.open("client-permission-policy", "class-name", ppc.getClassName())
                    .appendProperties(ppc.getProperties())
                    .close();
        }

        appendLoginModules(gen, "client-login-modules", c.getClientLoginModuleConfigs());
        appendLoginModules(gen, "member-login-modules", c.getMemberLoginModuleConfigs());

        CredentialsFactoryConfig cfc = c.getMemberCredentialsConfig();
        if (cfc.getClassName() != null) {
            gen.open("member-credentials-factory", "class-name", cfc.getClassName())
                    .appendProperties(cfc.getProperties())
                    .close();
        }

        List<SecurityInterceptorConfig> sic = c.getSecurityInterceptorConfigs();
        if (!sic.isEmpty()) {
            gen.open("security-interceptors");
            for (SecurityInterceptorConfig s : sic) {
                gen.open("interceptor", "class-name", s.getClassName())
                        .close();
            }
            gen.close();
        }

        appendSecurityPermissions(gen, "client-permissions", c.getClientPermissionConfigs(),
                "on-join-operation", c.getOnJoinPermissionOperation());
        gen.close();
    }

    private static void appendSecurityPermissions(XmlGenerator gen, String tag, Set<PermissionConfig> cpc, Object... attributes) {
        final List<PermissionConfig.PermissionType> clusterPermTypes = asList(ALL, CONFIG, TRANSACTION);

        if (!cpc.isEmpty()) {
            gen.open(tag, attributes);
            for (PermissionConfig p : cpc) {
                if (clusterPermTypes.contains(p.getType())) {
                    gen.open(p.getType().getNodeName(), "principal", p.getPrincipal());
                } else {
                    gen.open(p.getType().getNodeName(), "principal", p.getPrincipal(), "name", p.getName());
                }

                if (!p.getEndpoints().isEmpty()) {
                    gen.open("endpoints");
                    for (String endpoint : p.getEndpoints()) {
                        gen.node("endpoint", endpoint);
                    }
                    gen.close();
                }

                if (!p.getActions().isEmpty()) {
                    gen.open("actions");
                    for (String action : p.getActions()) {
                        gen.node("action", action);
                    }
                    gen.close();
                }
                gen.close();
            }
            gen.close();
        }
    }

    private static void appendLoginModules(XmlGenerator gen, String tag, List<LoginModuleConfig> loginModuleConfigs) {
        if (!loginModuleConfigs.isEmpty()) {
            gen.open(tag);
            for (LoginModuleConfig lm : loginModuleConfigs) {
                List<String> attrs = new ArrayList<String>();
                attrs.add("class-name");
                attrs.add(lm.getClassName());

                if (lm.getUsage() != null) {
                    attrs.add("usage");
                    attrs.add(lm.getUsage().name());
                }
                gen.open("login-module", attrs.toArray())
                        .appendProperties(lm.getProperties())
                        .close();
            }
            gen.close();
        }
    }

    @SuppressWarnings({"checkstyle:npathcomplexity"})
    private static void serializationXmlGenerator(XmlGenerator gen, Config config) {
        SerializationConfig c = config.getSerializationConfig();
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

        Map<Integer, String> dsfClasses = c.getDataSerializableFactoryClasses();
        Map<Integer, DataSerializableFactory> dsfImpls = c.getDataSerializableFactories();
        if (!MapUtil.isNullOrEmpty(dsfClasses) || !MapUtil.isNullOrEmpty(dsfImpls)) {
            gen.open("data-serializable-factories");
            appendSerializationFactory(gen, "data-serializable-factory", dsfClasses);
            appendSerializationFactory(gen, "data-serializable-factory", dsfImpls);
            gen.close();
        }

        Map<Integer, String> portableClasses = c.getPortableFactoryClasses();
        Map<Integer, PortableFactory> portableImpls = c.getPortableFactories();
        if (!MapUtil.isNullOrEmpty(portableClasses) || !MapUtil.isNullOrEmpty(portableImpls)) {
            gen.open("portable-factories");
            appendSerializationFactory(gen, "portable-factory", portableClasses);
            appendSerializationFactory(gen, "portable-factory", portableImpls);
            gen.close();
        }

        Collection<SerializerConfig> serializers = c.getSerializerConfigs();
        GlobalSerializerConfig globalSerializerConfig = c.getGlobalSerializerConfig();
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
        gen.node("check-class-def-errors", c.isCheckClassDefErrors());
        JavaSerializationFilterConfig javaSerializationFilterConfig = c.getJavaSerializationFilterConfig();
        if (javaSerializationFilterConfig != null) {
            gen.open("java-serialization-filter", "defaults-disabled", javaSerializationFilterConfig.isDefaultsDisabled());
            appendFilterList(gen, "blacklist", javaSerializationFilterConfig.getBlacklist());
            appendFilterList(gen, "whitelist", javaSerializationFilterConfig.getWhitelist());
            gen.close();
        }
        gen.close();
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
        PartitionGroupConfig pg = config.getPartitionGroupConfig();
        if (pg == null) {
            return;
        }
        gen.open("partition-group", "enabled", pg.isEnabled(), "group-type", pg.getGroupType());

        Collection<MemberGroupConfig> configs = pg.getMemberGroupConfigs();
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
                    .node("split-brain-protection-ref", ex.getSplitBrainProtectionName())
                    .close();
        }
    }

    private static void durableExecutorXmlGenerator(XmlGenerator gen, Config config) {
        for (DurableExecutorConfig ex : config.getDurableExecutorConfigs().values()) {
            gen.open("durable-executor-service", "name", ex.getName())
                    .node("pool-size", ex.getPoolSize())
                    .node("durability", ex.getDurability())
                    .node("capacity", ex.getCapacity())
                    .node("split-brain-protection-ref", ex.getSplitBrainProtectionName())
                    .close();
        }
    }

    private static void scheduledExecutorXmlGenerator(XmlGenerator gen, Config config) {
        for (ScheduledExecutorConfig ex : config.getScheduledExecutorConfigs().values()) {
            MergePolicyConfig mergePolicyConfig = ex.getMergePolicyConfig();

            gen.open("scheduled-executor-service", "name", ex.getName())
                    .node("pool-size", ex.getPoolSize())
                    .node("durability", ex.getDurability())
                    .node("capacity", ex.getCapacity())
                    .node("split-brain-protection-ref", ex.getSplitBrainProtectionName())
                    .node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .close();
        }
    }

    private static void cardinalityEstimatorXmlGenerator(XmlGenerator gen, Config config) {
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

    private static void pnCounterXmlGenerator(XmlGenerator gen, Config config) {
        for (PNCounterConfig counterConfig : config.getPNCounterConfigs().values()) {
            gen.open("pn-counter", "name", counterConfig.getName())
                    .node("replica-count", counterConfig.getReplicaCount())
                    .node("split-brain-protection-ref", counterConfig.getSplitBrainProtectionName())
                    .node("statistics-enabled", counterConfig.isStatisticsEnabled())
                    .close();
        }
    }

    private static void semaphoreXmlGenerator(XmlGenerator gen, Config config) {
        for (SemaphoreConfig sc : config.getSemaphoreConfigs()) {
            gen.open("semaphore", "name", sc.getName())
                    .node("initial-permits", sc.getInitialPermits())
                    .node("backup-count", sc.getBackupCount())
                    .node("async-backup-count", sc.getAsyncBackupCount())
                    .node("split-brain-protection-ref", sc.getSplitBrainProtectionName())
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
                    .node("split-brain-protection-ref", mm.getSplitBrainProtectionName())
                    .node("value-collection-type", mm.getValueCollectionType());

            entryListenerConfigXmlGenerator(gen, mm.getEntryListenerConfigs());
            MergePolicyConfig mergePolicyConfig = mm.getMergePolicyConfig();
            gen.node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .close();
        }
    }

    private static void queueXmlGenerator(XmlGenerator gen, Config config) {
        Collection<QueueConfig> qCfgs = config.getQueueConfigs().values();
        for (QueueConfig q : qCfgs) {
            gen.open("queue", "name", q.getName())
                    .node("statistics-enabled", q.isStatisticsEnabled())
                    .node("max-size", q.getMaxSize())
                    .node("backup-count", q.getBackupCount())
                    .node("async-backup-count", q.getAsyncBackupCount())
                    .node("empty-queue-ttl", q.getEmptyQueueTtl());
            appendItemListenerConfigs(gen, q.getItemListenerConfigs());
            QueueStoreConfig storeConfig = q.getQueueStoreConfig();
            if (storeConfig != null) {
                gen.open("queue-store", "enabled", storeConfig.isEnabled())
                        .node("class-name", storeConfig.getClassName())
                        .node("factory-class-name", storeConfig.getFactoryClassName())
                        .appendProperties(storeConfig.getProperties())
                        .close();
            }
            MergePolicyConfig mergePolicyConfig = q.getMergePolicyConfig();
            gen.node("split-brain-protection-ref", q.getSplitBrainProtectionName())
                    .node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .close();
        }
    }

    private static void lockXmlGenerator(XmlGenerator gen, Config config) {
        for (LockConfig c : config.getLockConfigs().values()) {
            gen.open("lock", "name", c.getName())
                    .node("split-brain-protection-ref", c.getSplitBrainProtectionName())
                    .close();
        }
    }

    private static void ringbufferXmlGenerator(XmlGenerator gen, Config config) {
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
                        .node("class-name", storeConfig.getClassName())
                        .node("factory-class-name", storeConfig.getFactoryClassName())
                        .appendProperties(storeConfig.getProperties());
                gen.close();
            }
            MergePolicyConfig mergePolicyConfig = rbConfig.getMergePolicyConfig();
            gen.node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .close();
        }
    }

    private static void atomicLongXmlGenerator(XmlGenerator gen, Config config) {
        Collection<AtomicLongConfig> configs = config.getAtomicLongConfigs().values();
        for (AtomicLongConfig atomicLongConfig : configs) {
            MergePolicyConfig mergePolicyConfig = atomicLongConfig.getMergePolicyConfig();
            gen.open("atomic-long", "name", atomicLongConfig.getName())
                    .node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .node("split-brain-protection-ref", atomicLongConfig.getSplitBrainProtectionName())
                    .close();
        }
    }

    private static void atomicReferenceXmlGenerator(XmlGenerator gen, Config config) {
        Collection<AtomicReferenceConfig> configs = config.getAtomicReferenceConfigs().values();
        for (AtomicReferenceConfig atomicReferenceConfig : configs) {
            MergePolicyConfig mergePolicyConfig = atomicReferenceConfig.getMergePolicyConfig();
            gen.open("atomic-reference", "name", atomicReferenceConfig.getName())
                    .node("merge-policy", mergePolicyConfig.getPolicy(), "batch-size", mergePolicyConfig.getBatchSize())
                    .node("split-brain-protection-ref", atomicReferenceConfig.getSplitBrainProtectionName())
                    .close();
        }
    }

    private static void wanReplicationXmlGenerator(XmlGenerator gen, Config config) {
        for (WanReplicationConfig wan : config.getWanReplicationConfigs().values()) {
            gen.open("wan-replication", "name", wan.getName());
            for (WanBatchReplicationPublisherConfig p : wan.getBatchPublisherConfigs()) {
                wanBatchReplicationPublisherXmlGenerator(gen, p);
            }
            for (CustomWanPublisherConfig p : wan.getCustomPublisherConfigs()) {
                wanCustomPublisherXmlGenerator(gen, p);
            }

            WanConsumerConfig consumerConfig = wan.getWanConsumerConfig();
            if (consumerConfig != null) {
                wanReplicationConsumerGenerator(gen, consumerConfig);
            }
            gen.close();
        }
    }

    private static void wanReplicationConsumerGenerator(XmlGenerator gen, WanConsumerConfig consumerConfig) {
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

    private static void wanBatchReplicationPublisherXmlGenerator(XmlGenerator gen, WanBatchReplicationPublisherConfig c) {
        String publisherId = c.getPublisherId();
        gen.open("batch-publisher");
        gen.node("group-name", c.getGroupName())
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
        wanReplicationSyncGenerator(gen, c.getWanSyncConfig());
        aliasedDiscoveryConfigsGenerator(gen, aliasedDiscoveryConfigsFrom(c));
        discoveryStrategyConfigXmlGenerator(gen, c.getDiscoveryConfig());
        gen.close();
    }

    private static void wanCustomPublisherXmlGenerator(XmlGenerator gen, CustomWanPublisherConfig c) {
        String publisherId = c.getPublisherId();
        gen.open("custom-publisher")
           .appendProperties(c.getProperties())
           .node("class-name", c.getClassName())
           .node("publisher-id", publisherId)
           .close();
    }

    private static void wanReplicationSyncGenerator(XmlGenerator gen, WanSyncConfig c) {
        gen.open("wan-sync")
                .node("consistency-check-strategy", c.getConsistencyCheckStrategy())
                .close();
    }

    private void networkConfigXmlGenerator(XmlGenerator gen, Config config) {
        if (config.getAdvancedNetworkConfig().isEnabled()) {
            return;
        }

        NetworkConfig netCfg = config.getNetworkConfig();
        gen.open("network")
                .node("public-address", netCfg.getPublicAddress())
                .node("port", netCfg.getPort(),
                        "port-count", netCfg.getPortCount(),
                        "auto-increment", netCfg.isPortAutoIncrement())
                .node("reuse-address", netCfg.isReuseAddress());

        Collection<String> outboundPortDefinitions = netCfg.getOutboundPortDefinitions();
        if (CollectionUtil.isNotEmpty(outboundPortDefinitions)) {
            gen.open("outbound-ports");
            for (String def : outboundPortDefinitions) {
                gen.node("ports", def);
            }
            gen.close();
        }

        JoinConfig join = netCfg.getJoin();
        gen.open("join");
        multicastConfigXmlGenerator(gen, join);
        tcpConfigXmlGenerator(gen, join);
        aliasedDiscoveryConfigsGenerator(gen, aliasedDiscoveryConfigsFrom(join));
        discoveryStrategyConfigXmlGenerator(gen, join.getDiscoveryConfig());
        gen.close();

        interfacesConfigXmlGenerator(gen, netCfg.getInterfaces());
        sslConfigXmlGenerator(gen, netCfg.getSSLConfig());
        socketInterceptorConfigXmlGenerator(gen, netCfg.getSocketInterceptorConfig());
        symmetricEncInterceptorConfigXmlGenerator(gen, netCfg.getSymmetricEncryptionConfig());
        memberAddressProviderConfigXmlGenerator(gen, netCfg.getMemberAddressProviderConfig());
        failureDetectorConfigXmlGenerator(gen, netCfg.getIcmpFailureDetectorConfig());
        restApiXmlGenerator(gen, netCfg);
        memcacheProtocolXmlGenerator(gen, netCfg);
        gen.close();
    }

    private void advancedNetworkConfigXmlGenerator(XmlGenerator gen, Config config) {
        AdvancedNetworkConfig netCfg = config.getAdvancedNetworkConfig();
        if (!netCfg.isEnabled()) {
            return;
        }

        gen.open("advanced-network", "enabled", netCfg.isEnabled());

        JoinConfig join = netCfg.getJoin();
        gen.open("join");
        multicastConfigXmlGenerator(gen, join);
        tcpConfigXmlGenerator(gen, join);
        aliasedDiscoveryConfigsGenerator(gen, aliasedDiscoveryConfigsFrom(join));
        discoveryStrategyConfigXmlGenerator(gen, join.getDiscoveryConfig());
        gen.close();

        failureDetectorConfigXmlGenerator(gen, netCfg.getIcmpFailureDetectorConfig());
        memberAddressProviderConfigXmlGenerator(gen, netCfg.getMemberAddressProviderConfig());
        for (EndpointConfig endpointConfig : netCfg.getEndpointConfigs().values()) {
            endpointConfigXmlGenerator(gen, endpointConfig);
        }
        gen.close();
    }

    private void endpointConfigXmlGenerator(XmlGenerator gen, EndpointConfig endpointConfig) {
        if (endpointConfig.getName() != null) {
            gen.open(endpointConfigElementName(endpointConfig), "name", endpointConfig.getName());
        } else {
            gen.open(endpointConfigElementName(endpointConfig));
        }

        Collection<String> outboundPortDefinitions = endpointConfig.getOutboundPortDefinitions();
        if (CollectionUtil.isNotEmpty(outboundPortDefinitions)) {
            gen.open("outbound-ports");
            for (String def : outboundPortDefinitions) {
                gen.node("ports", def);
            }
            gen.close();
        }

        interfacesConfigXmlGenerator(gen, endpointConfig.getInterfaces());
        sslConfigXmlGenerator(gen, endpointConfig.getSSLConfig());
        socketInterceptorConfigXmlGenerator(gen, endpointConfig.getSocketInterceptorConfig());
        symmetricEncInterceptorConfigXmlGenerator(gen, endpointConfig.getSymmetricEncryptionConfig());

        if (endpointConfig instanceof RestServerEndpointConfig) {
            RestServerEndpointConfig rsec = (RestServerEndpointConfig) endpointConfig;
            gen.open("endpoint-groups");
            for (RestEndpointGroup group : RestEndpointGroup.values()) {
                gen.node("endpoint-group", null, "name", group.name(),
                        "enabled", rsec.isGroupEnabled(group));
            }
            gen.close();
        }

        // socket-options
        gen.open("socket-options");
        gen.node("buffer-direct", endpointConfig.isSocketBufferDirect());
        gen.node("tcp-no-delay", endpointConfig.isSocketTcpNoDelay());
        gen.node("keep-alive", endpointConfig.isSocketKeepAlive());
        gen.node("connect-timeout-seconds", endpointConfig.getSocketConnectTimeoutSeconds());
        gen.node("send-buffer-size-kb", endpointConfig.getSocketSendBufferSizeKb());
        gen.node("receive-buffer-size-kb", endpointConfig.getSocketRcvBufferSizeKb());
        gen.node("linger-seconds", endpointConfig.getSocketLingerSeconds());
        gen.close();

        if (endpointConfig instanceof ServerSocketEndpointConfig) {
            ServerSocketEndpointConfig serverSocketEndpointConfig = (ServerSocketEndpointConfig) endpointConfig;
            gen.node("port", serverSocketEndpointConfig.getPort(),
                    "port-count", serverSocketEndpointConfig.getPortCount(),
                    "auto-increment", serverSocketEndpointConfig.isPortAutoIncrement())
                    .node("public-address", serverSocketEndpointConfig.getPublicAddress())
                    .node("reuse-address", serverSocketEndpointConfig.isReuseAddress());
        }
        gen.close();
    }

    private String endpointConfigElementName(EndpointConfig endpointConfig) {
        if (endpointConfig instanceof ServerSocketEndpointConfig) {
            switch (endpointConfig.getProtocolType()) {
                case REST:
                    return "rest-server-socket-endpoint-config";
                case WAN:
                    return "wan-server-socket-endpoint-config";
                case CLIENT:
                    return "client-server-socket-endpoint-config";
                case MEMBER:
                    return "member-server-socket-endpoint-config";
                case MEMCACHE:
                    return "memcache-server-socket-endpoint-config";
                default:
                    throw new IllegalStateException("Not recognised protocol type");
            }
        }

        return "wan-endpoint-config";
    }

    @SuppressWarnings("deprecation")
    private static void mapConfigXmlGenerator(XmlGenerator gen, Config config) {
        Collection<MapConfig> mapConfigs = config.getMapConfigs().values();
        for (MapConfig m : mapConfigs) {
            String cacheDeserializedVal = m.getCacheDeserializedValues() != null
                    ? m.getCacheDeserializedValues().name().replaceAll("_", "-") : null;
            MergePolicyConfig mergePolicyConfig = m.getMergePolicyConfig();
            gen.open("map", "name", m.getName())
                    .node("in-memory-format", m.getInMemoryFormat())
                    .node("statistics-enabled", m.isStatisticsEnabled())
                    .node("cache-deserialized-values", cacheDeserializedVal)
                    .node("backup-count", m.getBackupCount())
                    .node("async-backup-count", m.getAsyncBackupCount())
                    .node("time-to-live-seconds", m.getTimeToLiveSeconds())
                    .node("max-idle-seconds", m.getMaxIdleSeconds())
                    .node("eviction-policy", m.getEvictionPolicy())
                    .node("max-size", m.getMaxSizeConfig().getSize(),
                            "policy", m.getMaxSizeConfig().getMaxSizePolicy())
                    .node("merge-policy", mergePolicyConfig.getPolicy(),
                            "batch-size", mergePolicyConfig.getBatchSize())
                    .node("split-brain-protection-ref", m.getSplitBrainProtectionName())
                    .node("read-backup-data", m.isReadBackupData())
                    .node("metadata-policy", m.getMetadataPolicy());

            appendMerkleTreeConfig(gen, m.getMerkleTreeConfig());
            appendEventJournalConfig(gen, m.getEventJournalConfig());
            appendHotRestartConfig(gen, m.getHotRestartConfig());
            mapStoreConfigXmlGenerator(gen, m);
            mapNearCacheConfigXmlGenerator(gen, m.getNearCacheConfig());
            wanReplicationConfigXmlGenerator(gen, m.getWanReplicationRef());
            mapIndexConfigXmlGenerator(gen, m);
            attributeConfigXmlGenerator(gen, m);
            entryListenerConfigXmlGenerator(gen, m);
            mapPartitionLostListenerConfigXmlGenerator(gen, m);
            mapPartitionStrategyConfigXmlGenerator(gen, m);
            mapQueryCachesConfigXmlGenerator(gen, m);
            gen.close();
        }
    }

    private static void appendMerkleTreeConfig(XmlGenerator gen, MerkleTreeConfig c) {
        gen.open("merkle-tree", "enabled", c.isEnabled())
                .node("depth", c.getDepth())
                .close();
    }

    private static void appendHotRestartConfig(XmlGenerator gen, HotRestartConfig m) {
        gen.open("hot-restart", "enabled", m != null && m.isEnabled())
                .node("fsync", m != null && m.isFsync())
                .close();
    }

    private static void appendEventJournalConfig(XmlGenerator gen, EventJournalConfig c) {
        gen.open("event-journal", "enabled", c.isEnabled())
                .node("capacity", c.getCapacity())
                .node("time-to-live-seconds", c.getTimeToLiveSeconds())
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

            gen.node("split-brain-protection-ref", c.getSplitBrainProtectionName());
            cachePartitionLostListenerConfigXmlGenerator(gen, c.getPartitionLostListenerConfigs());

            gen.node("merge-policy", c.getMergePolicyConfig().getPolicy());
            appendEventJournalConfig(gen, c.getEventJournalConfig());
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

    private static void cacheExpiryPolicyFactoryConfigXmlGenerator(XmlGenerator gen, ExpiryPolicyFactoryConfig config) {
        if (config == null) {
            return;
        }
        if (!isNullOrEmpty(config.getClassName())) {
            gen.node("expiry-policy-factory", null, "class-name", config.getClassName());
        } else {
            TimedExpiryPolicyFactoryConfig timedConfig = config.getTimedExpiryPolicyFactoryConfig();
            if (timedConfig != null && timedConfig.getExpiryPolicyType() != null && timedConfig.getDurationConfig() != null) {
                DurationConfig duration = timedConfig.getDurationConfig();
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
            PartitioningStrategyConfig psc = m.getPartitioningStrategyConfig();
            gen.node("partition-strategy",
                    classNameOrImplClass(psc.getPartitioningStrategyClass(), psc.getPartitioningStrategy()));
        }
    }

    private static void mapQueryCachesConfigXmlGenerator(XmlGenerator gen, MapConfig mapConfig) {
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

                evictionConfigXmlGenerator(gen, queryCacheConfig.getEvictionConfig());
                mapIndexConfigXmlGenerator(gen, queryCacheConfig.getIndexConfigs());
                mapQueryCachePredicateConfigXmlGenerator(gen, queryCacheConfig);

                entryListenerConfigXmlGenerator(gen, queryCacheConfig.getEntryListenerConfigs());
                gen.close();
            }
            gen.close();
        }
    }

    private static void mapQueryCachePredicateConfigXmlGenerator(XmlGenerator gen,
                                                                 QueryCacheConfig queryCacheConfig) {
        PredicateConfig predicateConfig = queryCacheConfig.getPredicateConfig();

        String type = predicateConfig.getClassName() != null ? "class-name" : "sql";
        String content = predicateConfig.getClassName() != null ? predicateConfig.getClassName() : predicateConfig
                .getSql();
        gen.node("predicate", content, "type", type);
    }

    private static void entryListenerConfigXmlGenerator(XmlGenerator gen, MapConfig m) {
        entryListenerConfigXmlGenerator(gen, m.getEntryListenerConfigs());
    }

    private static void entryListenerConfigXmlGenerator(XmlGenerator gen,
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
        mapIndexConfigXmlGenerator(gen, m.getMapIndexConfigs());
    }

    private static void mapIndexConfigXmlGenerator(XmlGenerator gen, List<MapIndexConfig> mapIndexConfigs) {
        if (!mapIndexConfigs.isEmpty()) {
            gen.open("indexes");
            for (MapIndexConfig indexCfg : mapIndexConfigs) {
                gen.node("index", indexCfg.getAttribute(), "ordered", indexCfg.isOrdered());
            }
            gen.close();
        }
    }

    private static void attributeConfigXmlGenerator(XmlGenerator gen, MapConfig m) {
        if (!m.getAttributeConfigs().isEmpty()) {
            gen.open("attributes");
            for (AttributeConfig attributeCfg : m.getAttributeConfigs()) {
                gen.node("attribute", attributeCfg.getName(), "extractor-class-name", attributeCfg.getExtractorClassName());
            }
            gen.close();
        }
    }

    private static void wanReplicationConfigXmlGenerator(XmlGenerator gen, WanReplicationRef wan) {
        if (wan != null) {
            gen.open("wan-replication-ref", "name", wan.getName());

            String mergePolicy = wan.getMergePolicy();
            if (!isNullOrEmpty(mergePolicy)) {
                gen.node("merge-policy", mergePolicy);
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

    private static void mapStoreConfigXmlGenerator(XmlGenerator gen, MapConfig m) {
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
                    .node("write-delay-seconds", s.getWriteDelaySeconds())
                    .node("write-batch-size", s.getWriteBatchSize())
                    .appendProperties(s.getProperties())
                    .close();
        }
    }

    private static void mapNearCacheConfigXmlGenerator(XmlGenerator gen, NearCacheConfig n) {
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

    private static void evictionConfigXmlGenerator(XmlGenerator gen, EvictionConfig e) {
        if (e == null) {
            return;
        }

        String comparatorClassName = !isNullOrEmpty(e.getComparatorClassName()) ? e.getComparatorClassName() : null;
        gen.node("eviction", null,
                "size", e.getSize(),
                "max-size-policy", e.getMaximumSizePolicy(),
                "eviction-policy", e.getEvictionPolicy(),
                "comparator-class-name", comparatorClassName);
    }

    private static void multicastConfigXmlGenerator(XmlGenerator gen, JoinConfig join) {
        MulticastConfig mcConfig = join.getMulticastConfig();
        gen.open("multicast", "enabled", mcConfig.isEnabled(), "loopbackModeEnabled", mcConfig.isLoopbackModeEnabled())
                .node("multicast-group", mcConfig.getMulticastGroup())
                .node("multicast-port", mcConfig.getMulticastPort())
                .node("multicast-timeout-seconds", mcConfig.getMulticastTimeoutSeconds())
                .node("multicast-time-to-live", mcConfig.getMulticastTimeToLive());

        if (!mcConfig.getTrustedInterfaces().isEmpty()) {
            gen.open("trusted-interfaces");
            for (String trustedInterface : mcConfig.getTrustedInterfaces()) {
                gen.node("interface", trustedInterface);
            }
            gen.close();
        }
        gen.close();
    }

    private static void tcpConfigXmlGenerator(XmlGenerator gen, JoinConfig join) {
        TcpIpConfig c = join.getTcpIpConfig();
        gen.open("tcp-ip", "enabled", c.isEnabled(), "connection-timeout-seconds", c.getConnectionTimeoutSeconds())
                .open("member-list");
        for (String m : c.getMembers()) {
            gen.node("member", m);
        }
        gen.close()
                .node("required-member", c.getRequiredMember())
                .close();
    }

    private static void aliasedDiscoveryConfigsGenerator(XmlGenerator gen, List<AliasedDiscoveryConfig<?>> configs) {
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

    private static void discoveryStrategyConfigXmlGenerator(XmlGenerator gen, DiscoveryConfig c) {
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

    private static void interfacesConfigXmlGenerator(XmlGenerator gen, InterfacesConfig interfaces) {
        gen.open("interfaces", "enabled", interfaces.isEnabled());
        for (String i : interfaces.getInterfaces()) {
            gen.node("interface", i);
        }
        gen.close();
    }

    private void sslConfigXmlGenerator(XmlGenerator gen, SSLConfig ssl) {
        gen.open("ssl", "enabled", ssl != null && ssl.isEnabled());
        if (ssl != null) {
            Properties props = new Properties();
            props.putAll(ssl.getProperties());

            if (maskSensitiveFields && props.containsKey("trustStorePassword")) {
                props.setProperty("trustStorePassword", MASK_FOR_SENSITIVE_DATA);
            }

            if (maskSensitiveFields && props.containsKey("keyStorePassword")) {
                props.setProperty("keyStorePassword", MASK_FOR_SENSITIVE_DATA);
            }

            gen.node("factory-class-name",
                    classNameOrImplClass(ssl.getFactoryClassName(), ssl.getFactoryImplementation()))
                    .appendProperties(props);
        }
        gen.close();
    }

    private void mcMutualAuthConfigXmlGenerator(XmlGenerator gen, ManagementCenterConfig mcConfig) {
        MCMutualAuthConfig mutualAuthConfig = mcConfig.getMutualAuthConfig();
        gen.open("mutual-auth", "enabled", mutualAuthConfig != null && mutualAuthConfig.isEnabled());
        if (mutualAuthConfig != null) {
            Properties props = new Properties();
            props.putAll(mutualAuthConfig.getProperties());

            if (maskSensitiveFields && props.containsKey("trustStorePassword")) {
                props.setProperty("trustStorePassword", MASK_FOR_SENSITIVE_DATA);
            }

            if (maskSensitiveFields && props.containsKey("keyStorePassword")) {
                props.setProperty("keyStorePassword", MASK_FOR_SENSITIVE_DATA);
            }

            gen.node("factory-class-name",
                    classNameOrImplClass(mutualAuthConfig.getFactoryClassName(), mutualAuthConfig.getFactoryImplementation()))
                    .appendProperties(props);
        }
        gen.close();
    }

    private static void socketInterceptorConfigXmlGenerator(XmlGenerator gen, SocketInterceptorConfig socket) {
        gen.open("socket-interceptor", "enabled", socket != null && socket.isEnabled());
        if (socket != null) {
            gen.node("class-name", classNameOrImplClass(socket.getClassName(), socket.getImplementation()))
                    .appendProperties(socket.getProperties());
        }
        gen.close();
    }

    private void symmetricEncInterceptorConfigXmlGenerator(XmlGenerator gen, SymmetricEncryptionConfig sec) {
        if (sec == null) {
            return;
        }
        gen.open("symmetric-encryption", "enabled", sec.isEnabled())
                .node("algorithm", sec.getAlgorithm())
                .node("salt", getOrMaskValue(sec.getSalt()))
                .node("password", getOrMaskValue(sec.getPassword()))
                .node("iteration-count", sec.getIterationCount())
                .close();
    }

    private static void memberAddressProviderConfigXmlGenerator(XmlGenerator gen,
                                                                MemberAddressProviderConfig memberAddressProviderConfig) {
        if (memberAddressProviderConfig == null) {
            return;
        }
        String className = classNameOrImplClass(memberAddressProviderConfig.getClassName(),
                memberAddressProviderConfig.getImplementation());
        if (isNullOrEmpty(className)) {
            return;
        }
        gen.open("member-address-provider", "enabled", memberAddressProviderConfig.isEnabled())
                .node("class-name", className)
                .appendProperties(memberAddressProviderConfig.getProperties())
                .close();
    }

    private static void failureDetectorConfigXmlGenerator(XmlGenerator gen,
                                                          IcmpFailureDetectorConfig icmpFailureDetectorConfig) {
        if (icmpFailureDetectorConfig == null) {
            return;
        }

        gen.open("failure-detector");
        gen.open("icmp", "enabled", icmpFailureDetectorConfig.isEnabled())
                .node("ttl", icmpFailureDetectorConfig.getTtl())
                .node("interval-milliseconds", icmpFailureDetectorConfig.getIntervalMilliseconds())
                .node("max-attempts", icmpFailureDetectorConfig.getMaxAttempts())
                .node("timeout-milliseconds", icmpFailureDetectorConfig.getTimeoutMilliseconds())
                .node("fail-fast-on-startup", icmpFailureDetectorConfig.isFailFastOnStartup())
                .node("parallel-mode", icmpFailureDetectorConfig.isParallelMode())
                .close();
        gen.close();
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
                .node("auto-remove-stale-data", hrCfg.isAutoRemoveStaleData())
                .close();
    }

    private static void flakeIdGeneratorXmlGenerator(XmlGenerator gen, Config config) {
        for (FlakeIdGeneratorConfig m : config.getFlakeIdGeneratorConfigs().values()) {
            gen.open("flake-id-generator", "name", m.getName())
                    .node("prefetch-count", m.getPrefetchCount())
                    .node("prefetch-validity-millis", m.getPrefetchValidityMillis())
                    .node("id-offset", m.getIdOffset())
                    .node("node-id-offset", m.getNodeIdOffset())
                    .node("statistics-enabled", m.isStatisticsEnabled());
            gen.close();
        }
    }

    private static void crdtReplicationXmlGenerator(XmlGenerator gen, Config config) {
        CRDTReplicationConfig replicationConfig = config.getCRDTReplicationConfig();
        gen.open("crdt-replication");
        if (replicationConfig != null) {
            gen.node("replication-period-millis", replicationConfig.getReplicationPeriodMillis())
                    .node("max-concurrent-replication-targets", replicationConfig.getMaxConcurrentReplicationTargets());
        }
        gen.close();
    }

    private static void splitBrainProtectionXmlGenerator(XmlGenerator gen, Config config) {
        for (SplitBrainProtectionConfig splitBrainProtectionConfig : config.getSplitBrainProtectionConfigs().values()) {
            gen.open("split-brain-protection", "name", splitBrainProtectionConfig.getName(),
                    "enabled", splitBrainProtectionConfig.isEnabled())
                    .node("minimum-cluster-size", splitBrainProtectionConfig.getMinimumClusterSize())
                    .node("protect-on", splitBrainProtectionConfig.getProtectOn());
            if (!splitBrainProtectionConfig.getListenerConfigs().isEmpty()) {
                gen.open("listeners");
                for (SplitBrainProtectionListenerConfig listenerConfig : splitBrainProtectionConfig.getListenerConfigs()) {
                    gen.node("listener", classNameOrImplClass(listenerConfig.getClassName(),
                            listenerConfig.getImplementation()));
                }
                gen.close();
            }
            handleSplitBrainProtectionFunction(gen, splitBrainProtectionConfig);
            gen.close();
        }
    }

    private static void cpSubsystemConfig(XmlGenerator gen, Config config) {
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        gen.open("cp-subsystem")
                .node("cp-member-count", cpSubsystemConfig.getCPMemberCount())
                .node("group-size", cpSubsystemConfig.getGroupSize())
                .node("session-time-to-live-seconds", cpSubsystemConfig.getSessionTimeToLiveSeconds())
                .node("session-heartbeat-interval-seconds", cpSubsystemConfig.getSessionHeartbeatIntervalSeconds())
                .node("missing-cp-member-auto-removal-seconds", cpSubsystemConfig.getMissingCPMemberAutoRemovalSeconds())
                .node("fail-on-indeterminate-operation-state", cpSubsystemConfig.isFailOnIndeterminateOperationState());

        RaftAlgorithmConfig raftAlgorithmConfig = cpSubsystemConfig.getRaftAlgorithmConfig();
        gen.open("raft-algorithm")
                .node("leader-election-timeout-in-millis", raftAlgorithmConfig.getLeaderElectionTimeoutInMillis())
                .node("leader-heartbeat-period-in-millis", raftAlgorithmConfig.getLeaderHeartbeatPeriodInMillis())
                .node("max-missed-leader-heartbeat-count", raftAlgorithmConfig.getMaxMissedLeaderHeartbeatCount())
                .node("append-request-max-entry-count", raftAlgorithmConfig.getAppendRequestMaxEntryCount())
                .node("commit-index-advance-count-to-snapshot", raftAlgorithmConfig.getCommitIndexAdvanceCountToSnapshot())
                .node("uncommitted-entry-count-to-reject-new-appends",
                        raftAlgorithmConfig.getUncommittedEntryCountToRejectNewAppends())
                .node("append-request-backoff-timeout-in-millis", raftAlgorithmConfig.getAppendRequestBackoffTimeoutInMillis())
                .close();

        gen.open("semaphores");

        for (CPSemaphoreConfig semaphoreConfig : cpSubsystemConfig.getSemaphoreConfigs().values()) {
            gen.open("cp-semaphore")
                    .node("name", semaphoreConfig.getName())
                    .node("jdk-compatible", semaphoreConfig.isJDKCompatible())
                    .close();
        }

        gen.close().open("locks");

        for (FencedLockConfig lockConfig : cpSubsystemConfig.getLockConfigs().values()) {
            gen.open("fenced-lock")
                    .node("name", lockConfig.getName())
                    .node("lock-acquire-limit", lockConfig.getLockAcquireLimit())
                    .close();
        }

        gen.close().close();
    }

    private static void userCodeDeploymentConfig(XmlGenerator gen, Config config) {
        UserCodeDeploymentConfig ucdConfig = config.getUserCodeDeploymentConfig();
        gen.open("user-code-deployment", "enabled", ucdConfig.isEnabled())
                .node("class-cache-mode", ucdConfig.getClassCacheMode())
                .node("provider-mode", ucdConfig.getProviderMode())
                .node("blacklist-prefixes", ucdConfig.getBlacklistedPrefixes())
                .node("whitelist-prefixes", ucdConfig.getWhitelistedPrefixes())
                .node("provider-filter", ucdConfig.getProviderFilter())
                .close();
    }

    private static void handleSplitBrainProtectionFunction(XmlGenerator gen,
                                                           SplitBrainProtectionConfig splitBrainProtectionConfig) {
        if (splitBrainProtectionConfig.
                getFunctionImplementation() instanceof ProbabilisticSplitBrainProtectionFunction) {
            ProbabilisticSplitBrainProtectionFunction qf =
                    (ProbabilisticSplitBrainProtectionFunction)
                            splitBrainProtectionConfig.getFunctionImplementation();
            long acceptableHeartbeatPause = qf.getAcceptableHeartbeatPauseMillis();
            double threshold = qf.getSuspicionThreshold();
            int maxSampleSize = qf.getMaxSampleSize();
            long minStdDeviation = qf.getMinStdDeviationMillis();
            long firstHeartbeatEstimate = qf.getHeartbeatIntervalMillis();
            gen.open("probabilistic-split-brain-protection", "acceptable-heartbeat-pause-millis", acceptableHeartbeatPause,
                    "suspicion-threshold", threshold,
                    "max-sample-size", maxSampleSize,
                    "min-std-deviation-millis", minStdDeviation,
                    "heartbeat-interval-millis", firstHeartbeatEstimate);
            gen.close();
        } else if (splitBrainProtectionConfig.
                getFunctionImplementation() instanceof RecentlyActiveSplitBrainProtectionFunction) {
            RecentlyActiveSplitBrainProtectionFunction qf =
                    (RecentlyActiveSplitBrainProtectionFunction)
                            splitBrainProtectionConfig.getFunctionImplementation();
            gen.open("recently-active-split-brain-protection", "heartbeat-tolerance-millis",
                    qf.getHeartbeatToleranceMillis());
            gen.close();
        } else {
            gen.node("function-class-name",
                    classNameOrImplClass(splitBrainProtectionConfig.getFunctionClassName(),
                    splitBrainProtectionConfig.getFunctionImplementation()));
        }
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
                .node("persistent-memory-directory", nativeMemoryConfig.getPersistentMemoryDirectory())
                .close();
    }

    private static void servicesXmlGenerator(XmlGenerator gen, Config config) {
        ServicesConfig c = config.getServicesConfig();
        if (c == null) {
            return;
        }
        gen.open("services", "enable-defaults", c.isEnableDefaults());
        if (CollectionUtil.isNotEmpty(c.getServiceConfigs())) {
            for (ServiceConfig serviceConfig : c.getServiceConfigs()) {
                gen.open("service", "enabled", serviceConfig.isEnabled())
                        .node("name", serviceConfig.getName())
                        .node("class-name", classNameOrImplClass(serviceConfig.getClassName(), serviceConfig.getImplementation()))
                        .appendProperties(serviceConfig.getProperties())
                        .close();
            }
        }
        gen.close();
    }

    private static void liteMemberXmlGenerator(XmlGenerator gen, Config config) {
        gen.node("lite-member", null, "enabled", config.isLiteMember());
    }

    private static void restApiXmlGenerator(XmlGenerator gen, NetworkConfig config) {
        RestApiConfig c = config.getRestApiConfig();
        if (c == null) {
            return;
        }
        gen.open("rest-api", "enabled", c.isEnabled());
        for (RestEndpointGroup group : RestEndpointGroup.values()) {
            gen.node("endpoint-group", null, "name", group.name(), "enabled", c.isGroupEnabled(group));
        }
        gen.close();
    }

    private static void memcacheProtocolXmlGenerator(XmlGenerator gen, NetworkConfig config) {
        MemcacheProtocolConfig c = config.getMemcacheProtocolConfig();
        if (c == null) {
            return;
        }
        gen.node("memcache-protocol", null, "enabled", c.isEnabled());
    }

    private String format(String input, int indent) {
        if (!formatted) {
            return input;
        }
        StreamResult xmlOutput = null;
        try {
            Source xmlInput = new StreamSource(new StringReader(input));
            xmlOutput = new StreamResult(new StringWriter());
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            /*
             * Older versions of Xalan still use this method of setting indent values.
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
            /*
             * Newer versions of Xalan will look for a fully-qualified output property in order to specify amount of
             * indentation to use. Attempt to make this work as well but again don't completely fail if it's a problem.
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
            Object value = factory.getValue();
            String className = value instanceof String ? (String) value : value.getClass().getName();
            gen.node(elementName, className, "factory-id", factory.getKey().toString());
        }
    }

    private static void appendFilterList(XmlGenerator gen, String listName, ClassFilter classFilterList) {
        if (classFilterList.isEmpty()) {
            return;
        }
        gen.open(listName);
        for (String className : classFilterList.getClasses()) {
            gen.node("class", className);
        }
        for (String packageName : classFilterList.getPackages()) {
            gen.node("package", packageName);
        }
        for (String prefix : classFilterList.getPrefixes()) {
            gen.node("prefix", prefix);
        }
        gen.close();
    }

    /**
     * Utility class to build xml using a {@link StringBuilder}.
     */
    public static final class XmlGenerator {

        private static final int CAPACITY = 64;

        private final StringBuilder xml;
        private final ArrayDeque<String> openNodes = new ArrayDeque<String>();

        public XmlGenerator(StringBuilder xml) {
            this.xml = xml;
        }

        public XmlGenerator open(String name, Object... attributes) {
            appendOpenNode(xml, name, attributes);
            openNodes.addLast(name);
            return this;
        }

        public XmlGenerator node(String name, Object contents, Object... attributes) {
            appendNode(xml, name, contents, attributes);
            return this;
        }

        public XmlGenerator close() {
            appendCloseNode(xml, openNodes.pollLast());
            return this;
        }

        public XmlGenerator appendLabels(Set<String> labels) {
            if (!labels.isEmpty()) {
                open("client-labels");
                for (String label : labels) {
                    node("label", label);
                }
                close();
            }
            return this;
        }

        public XmlGenerator appendProperties(Properties props) {
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

        public XmlGenerator appendProperties(Map<String, ? extends Comparable> props) {
            if (!MapUtil.isNullOrEmpty(props)) {
                open("properties");
                for (Entry<String, ? extends Comparable> entry : props.entrySet()) {
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

        private static void appendNode(StringBuilder xml, String name, Object contents, Object... attributes) {
            if (contents != null || attributes.length > 0) {
                xml.append('<').append(name);
                appendAttributes(xml, attributes);
                if (contents != null) {
                    xml.append('>');
                    escapeXml(contents, xml);
                    xml.append("</").append(name).append('>');
                } else {
                    xml.append("/>");
                }
            }
        }

        private static void appendAttributes(StringBuilder xml, Object... attributes) {
            for (int i = 0; i < attributes.length; ) {
                Object attributeName = attributes[i++];
                Object attributeValue = attributes[i++];
                if (attributeValue == null) {
                    continue;
                }
                xml.append(" ").append(attributeName).append("=\"");
                escapeXmlAttr(attributeValue, xml);
                xml.append("\"");
            }
        }

        /**
         * Escapes special characters in XML element contents and appends the result to <code>appendTo</code>.
         */
        private static void escapeXml(Object o, StringBuilder appendTo) {
            if (o == null) {
                appendTo.append("null");
                return;
            }
            String s = o.toString();
            int length = s.length();
            appendTo.ensureCapacity(appendTo.length() + length + CAPACITY);
            for (int i = 0; i < length; i++) {
                char ch = s.charAt(i);
                if (ch == '<') {
                    appendTo.append("&lt;");
                } else if (ch == '&') {
                    appendTo.append("&amp;");
                } else {
                    appendTo.append(ch);
                }
            }
        }

        /**
         * Escapes special characters in XML attribute value and appends the result to <code>appendTo</code>.
         */
        private static void escapeXmlAttr(Object o, StringBuilder appendTo) {
            if (o == null) {
                appendTo.append("null");
                return;
            }
            String s = o.toString();
            int length = s.length();
            appendTo.ensureCapacity(appendTo.length() + length + CAPACITY);
            for (int i = 0; i < length; i++) {
                char ch = s.charAt(i);
                switch (ch) {
                    case '"':
                        appendTo.append("&quot;");
                        break;
                    case '\'':
                        appendTo.append("&#39;");
                        break;
                    case '&':
                        appendTo.append("&amp;");
                        break;
                    case '<':
                        appendTo.append("&lt;");
                        break;
                    default:
                        appendTo.append(ch);
                }
            }
        }
    }
}
