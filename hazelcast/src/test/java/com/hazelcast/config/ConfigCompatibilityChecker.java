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

package com.hazelcast.config;

import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.KerberosAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.TlsAuthenticationConfig;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.config.security.UsernamePasswordIdentityConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.internal.config.ServicesConfig;
import com.hazelcast.internal.util.CollectionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.internal.config.AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom;
import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static java.text.MessageFormat.format;
import static java.util.Collections.singletonMap;
import static org.codehaus.groovy.runtime.InvokerHelper.asList;
import static org.junit.Assert.assertEquals;

public class ConfigCompatibilityChecker {

    /**
     * Checks if two {@link Config} instances are compatible.
     * <p>
     * This mostly means that the config values will have the same impact on the behaviour of the system,
     * but are not necessarily the same (e.g. a {@code null} value is sometimes the same as an empty
     * collection or a disabled config).
     *
     * <b>Note:</b> This method checks MOST but NOT ALL configuration. As such it is best used in test
     * scenarios to cover as much config checks as possible automatically.
     *
     * @param c1 the {@link Config} to check
     * @param c2 the {@link Config} to check
     * @return {@code true} if the configs are compatible, {@code false} otherwise
     * @throws HazelcastException       if configs are incompatible
     * @throws IllegalArgumentException if one of the configs is {@code null}
     */
    static boolean isCompatible(Config c1, Config c2) {
        if (c1 == c2) {
            return true;
        }
        if (c1 == null || c2 == null) {
            throw new IllegalArgumentException("One of the two configs is null");
        }
        if (!nullSafeEqual(c1.getClusterName(), c2.getClusterName())) {
            return false;
        }

        checkWanConfigs(c1.getWanReplicationConfigs(), c2.getWanReplicationConfigs());
        checkCompatibleConfigs("partition group", c1.getPartitionGroupConfig(), c2.getPartitionGroupConfig(),
                new PartitionGroupConfigChecker());
        checkCompatibleConfigs("serialization", c1.getSerializationConfig(), c2.getSerializationConfig(),
                new SerializationConfigChecker());
        checkCompatibleConfigs("management center", c1.getManagementCenterConfig(), c2.getManagementCenterConfig(),
                new ManagementCenterConfigChecker());
        checkCompatibleConfigs("hot restart", c1.getHotRestartPersistenceConfig(), c2.getHotRestartPersistenceConfig(),
                new HotRestartConfigChecker());
        checkCompatibleConfigs("persistence", c1.getPersistenceConfig(), c2.getPersistenceConfig(),
                new PersistenceConfigChecker());
        checkCompatibleConfigs("device", c1, c2, c1.getDeviceConfigs(), c2.getDeviceConfigs(), new DeviceConfigChecker());
        checkCompatibleConfigs("CRDT replication", c1.getCRDTReplicationConfig(), c2.getCRDTReplicationConfig(),
                new CRDTReplicationConfigChecker());
        checkCompatibleConfigs("network", c1.getNetworkConfig(), c2.getNetworkConfig(), new NetworkConfigChecker());
        checkCompatibleConfigs("advanced network", c1.getAdvancedNetworkConfig(), c2.getAdvancedNetworkConfig(),
                new AdvancedNetworkConfigChecker());
        checkCompatibleConfigs("map", c1, c2, c1.getMapConfigs(), c2.getMapConfigs(), new MapConfigChecker());
        checkCompatibleConfigs("ringbuffer", c1, c2, c1.getRingbufferConfigs(), c2.getRingbufferConfigs(),
                new RingbufferConfigChecker());
        checkCompatibleConfigs("queue", c1, c2, c1.getQueueConfigs(), c2.getQueueConfigs(), new QueueConfigChecker());
        checkCompatibleConfigs("topic", c1, c2, c1.getTopicConfigs(), c2.getTopicConfigs(), new TopicConfigChecker());
        checkCompatibleConfigs("reliable topic", c1, c2, c1.getReliableTopicConfigs(), c2.getReliableTopicConfigs(),
                new ReliableTopicConfigChecker());
        checkCompatibleConfigs("cache", c1, c2, c1.getCacheConfigs(), c2.getCacheConfigs(), new CacheSimpleConfigChecker());
        checkCompatibleConfigs("executor", c1, c2, c1.getExecutorConfigs(), c2.getExecutorConfigs(), new ExecutorConfigChecker());
        checkCompatibleConfigs("durable executor", c1, c2, c1.getDurableExecutorConfigs(), c2.getDurableExecutorConfigs(),
                new DurableExecutorConfigChecker());
        checkCompatibleConfigs("scheduled executor", c1, c2, c1.getScheduledExecutorConfigs(), c2.getScheduledExecutorConfigs(),
                new ScheduledExecutorConfigChecker());
        checkCompatibleConfigs("multimap", c1, c2, c1.getMultiMapConfigs(), c2.getMultiMapConfigs(), new MultimapConfigChecker());
        checkCompatibleConfigs("replicated map", c1, c2, c1.getReplicatedMapConfigs(), c2.getReplicatedMapConfigs(),
                new ReplicatedMapConfigChecker());
        checkCompatibleConfigs("list", c1, c2, c1.getListConfigs(), c2.getListConfigs(), new ListConfigChecker());
        checkCompatibleConfigs("set", c1, c2, c1.getSetConfigs(), c2.getSetConfigs(), new SetConfigChecker());
        checkCompatibleConfigs("flake id generator", c1, c2, c1.getFlakeIdGeneratorConfigs(), c2.getFlakeIdGeneratorConfigs(),
                new FlakeIdGeneratorConfigChecker());
        checkCompatibleConfigs("cardinality estimator", c1, c2, c1.getCardinalityEstimatorConfigs(),
                c2.getCardinalityEstimatorConfigs(), new CardinalityEstimatorConfigChecker());
        checkCompatibleConfigs("pn counter", c1, c2, c1.getPNCounterConfigs(), c2.getPNCounterConfigs(),
                new PNCounterConfigChecker());
        checkCompatibleConfigs("split-brain-protection", c1, c2, c1.getSplitBrainProtectionConfigs(), c2.getSplitBrainProtectionConfigs(), new SplitBrainProtectionConfigChecker());
        checkCompatibleConfigs("security", c1, c2, singletonMap("", c1.getSecurityConfig()),
                singletonMap("", c2.getSecurityConfig()), new SecurityConfigChecker());
        checkCompatibleConfigs("cp subsystem", c1, c2, singletonMap("", c1.getCPSubsystemConfig()),
                singletonMap("", c2.getCPSubsystemConfig()), new CPSubsystemConfigChecker());
        checkCompatibleConfigs("auditlog", c1, c2, singletonMap("", c1.getAuditlogConfig()),
                singletonMap("", c2.getAuditlogConfig()), new AuditlogConfigChecker());

        checkCompatibleConfigs("metrics", c1.getMetricsConfig(), c2.getMetricsConfig(), new MetricsConfigChecker());
        checkCompatibleConfigs("sql", c1.getSqlConfig(), c2.getSqlConfig(), new SqlConfigChecker());

        checkCompatibleConfigs("instance-tracking", c1.getInstanceTrackingConfig(), c2.getInstanceTrackingConfig(),
                new InstanceTrackingConfigChecker());
        checkCompatibleConfigs("native memory", c1.getNativeMemoryConfig(), c2.getNativeMemoryConfig(),
                new NativeMemoryConfigChecker());

        return true;
    }

    public static void checkWanConfigs(Map<String, WanReplicationConfig> c1, Map<String, WanReplicationConfig> c2) {
        if ((c1 != c2 && (c1 == null || c2 == null)) || c1.size() != c2.size()) {
            throw new HazelcastException(format("Incompatible wan replication config :\n{0}\n vs \n{1}", c1, c2));
        }
        WanReplicationConfigChecker checker = new WanReplicationConfigChecker();
        for (Entry<String, WanReplicationConfig> entry : c1.entrySet()) {
            checkCompatibleConfigs("wan replication", entry.getValue(), c2.get(entry.getKey()), checker);
        }
    }

    public static void checkEndpointConfigCompatible(EndpointConfig c1, EndpointConfig c2) {
        checkCompatibleConfigs("endpoint-config", c1, c2, new EndpointConfigChecker());
    }

    private static <T> void checkCompatibleConfigs(String type, T c1, T c2, ConfigChecker<T> checker) {
        if (!checker.check(c1, c2)) {
            throw new HazelcastException(format("Incompatible " + type + " config :\n{0}\n vs \n{1}", c1, c2));
        }
    }

    private static <T> void checkCompatibleConfigs(String type, Config c1, Config c2, Map<String, T> configs1,
                                                   Map<String, T> configs2, ConfigChecker<T> checker) {
        Set<String> configNames = new HashSet<>(configs1.keySet());
        configNames.addAll(configs2.keySet());

        for (String name : configNames) {
            T config1 = lookupByPattern(c1.getConfigPatternMatcher(), configs1, name);
            T config2 = lookupByPattern(c2.getConfigPatternMatcher(), configs2, name);
            if (config1 != null && config2 != null && !checker.check(config1, config2)) {
                throw new HazelcastException(format("Incompatible " + type + " config :\n{0}\n vs \n{1}", config1, config2));
            }
        }
        T config1 = checker.getDefault(c1);
        T config2 = checker.getDefault(c2);
        if (!checker.check(config1, config2)) {
            throw new HazelcastException(format("Incompatible default " + type + " config :\n{0}\n vs \n{1}", config1, config2));
        }
    }

    private static boolean nullSafeEqual(Object a, Object b) {
        return Objects.equals(a, b);
    }

    private static <T> boolean isCollectionCompatible(Collection<T> c1, Collection<T> c2, ConfigChecker<T> checker) {
        if (c1 == c2) {
            return true;
        }
        if (c1 == null || c2 == null || c1.size() != c2.size()) {
            return false;
        }

        Iterator<T> i1 = c1.iterator();
        Iterator<T> i2 = c2.iterator();
        while (i1.hasNext() && i2.hasNext()) {
            T config1 = i1.next();
            T config2 = i2.next();
            if (!checker.check(config1, config2)) {
                return false;
            }
        }
        return !(i1.hasNext() || i2.hasNext());
    }

    private static boolean isCompatible(EvictionConfig c1, EvictionConfig c2) {
        return new EvictionConfigChecker().check(c1, c2);
    }

    private static boolean isCompatible(PredicateConfig c1, PredicateConfig c2) {
        return new PredicateConfigChecker().check(c1, c2);
    }

    private static boolean isCompatible(CollectionConfig c1, CollectionConfig c2) {
        return c1 == c2 || !(c1 == null || c2 == null)
                && nullSafeEqual(c1.getName(), c2.getName())
                && nullSafeEqual(c1.getItemListenerConfigs(), c2.getItemListenerConfigs())
                && nullSafeEqual(c1.getBackupCount(), c2.getBackupCount())
                && nullSafeEqual(c1.getAsyncBackupCount(), c2.getAsyncBackupCount())
                && nullSafeEqual(c1.getMaxSize(), c2.getMaxSize())
                && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName())
                && isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig());
    }

    private static boolean isCompatible(MerkleTreeConfig c1, MerkleTreeConfig c2) {
        boolean c1Disabled = c1 == null || !c1.isEnabled();
        boolean c2Disabled = c2 == null || !c2.isEnabled();
        return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                && c1.getDepth() == c2.getDepth());
    }

    private static boolean isCompatible(HotRestartConfig c1, HotRestartConfig c2) {
        boolean c1Disabled = c1 == null || !c1.isEnabled();
        boolean c2Disabled = c2 == null || !c2.isEnabled();
        return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null && nullSafeEqual(c1.isFsync(), c2.isFsync()));
    }

    private static boolean isCompatible(EventJournalConfig c1, EventJournalConfig c2) {
        boolean c1Disabled = c1 == null || !c1.isEnabled();
        boolean c2Disabled = c2 == null || !c2.isEnabled();
        return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                && nullSafeEqual(c1.getCapacity(), c2.getCapacity())
                && nullSafeEqual(c1.getTimeToLiveSeconds(), c2.getTimeToLiveSeconds()));
    }

    private static boolean isCompatible(MergePolicyConfig c1, MergePolicyConfig c2) {
        return c1 == c2 || !(c1 == null || c2 == null)
                && c1.getBatchSize() == c2.getBatchSize()
                && nullSafeEqual(c1.getPolicy(), c2.getPolicy());
    }

    public static void checkQueueConfig(QueueConfig expectedConfig, QueueConfig actualConfig) {
        checkCompatibleConfigs("queue-config", expectedConfig, actualConfig, new QueueConfigChecker());
    }

    public static void checkListenerConfigs(List<ListenerConfig> expectedConfigs, List<ListenerConfig> actualConfigs) {
        assertEquals(expectedConfigs.size(), actualConfigs.size());
        for (int i = 0; i < expectedConfigs.size(); i++) {
            checkCompatibleConfigs("listener-config", expectedConfigs.get(i), actualConfigs.get(i), new ReplicatedMapListenerConfigChecker());
        }
    }

    public static void checkSocketInterceptorConfig(SocketInterceptorConfig expectedConfig, SocketInterceptorConfig actualConfig) {
        checkCompatibleConfigs("socket-interceptor-config", expectedConfig, actualConfig, new SocketInterceptorConfigChecker());
    }

    public static void checkMapConfig(MapConfig expectedConfig, MapConfig actualConfig) {
        checkCompatibleConfigs("map-config", expectedConfig, actualConfig, new MapConfigChecker());
    }

    public static void checkDynamicConfigurationConfig(DynamicConfigurationConfig expectedConfig, DynamicConfigurationConfig actualConfig) {
        checkCompatibleConfigs("dynamic-configuration-config", expectedConfig, actualConfig, new DynamicConfigurationConfigChecker());
    }

    public static void checkDeviceConfig(LocalDeviceConfig expectedConfig, LocalDeviceConfig actualConfig) {
        checkCompatibleConfigs("device-config", expectedConfig, actualConfig, new DeviceConfigChecker());
    }

    public static void checkRingbufferConfig(RingbufferConfig expectedConfig, RingbufferConfig actualConfig) {
        checkCompatibleConfigs("ringbuffer", expectedConfig, actualConfig, new RingbufferConfigChecker());
    }

    public static void checkMemberAddressProviderConfig(MemberAddressProviderConfig expected, MemberAddressProviderConfig actual) {
        checkCompatibleConfigs("member-address-provider", expected, actual, new MemberAddressProviderConfigChecker());
    }

    public static void checkSerializerConfigs(Collection<SerializerConfig> expected, Collection<SerializerConfig> actual) {
        assertEquals(expected.size(), actual.size());
        Iterator<SerializerConfig> expectedIterator = expected.iterator();
        Iterator<SerializerConfig> actualIterator = actual.iterator();
        SerializerConfigChecker checker = new SerializerConfigChecker();
        while (expectedIterator.hasNext()) {
            checkCompatibleConfigs("serializer", expectedIterator.next(), actualIterator.next(), checker);
        }
    }

    public static void checkSSLConfig(SSLConfig expected, SSLConfig actual) {
        checkCompatibleConfigs("ssl", expected, actual, new SSLConfigChecker());
    }

    public static void checkQueryCacheConfig(QueryCacheConfig expected, QueryCacheConfig actual) {
        checkCompatibleConfigs("query-cache", expected, actual, new QueryCacheConfigChecker());
    }

    public static void checkNearCacheConfig(NearCacheConfig expected, NearCacheConfig actual) {
        checkCompatibleConfigs("near-cache", expected, actual, new NearCacheConfigChecker());
    }

    private abstract static class ConfigChecker<T> {

        abstract boolean check(T t1, T t2);

        T getDefault(Config c) {
            return null;
        }
    }

    private static class NearCacheConfigChecker extends ConfigChecker<NearCacheConfig> {
        @Override
        boolean check(NearCacheConfig c1, NearCacheConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null
                && c1.isCacheLocalEntries() == c2.isCacheLocalEntries()
                && c1.isSerializeKeys() == c2.isSerializeKeys()
                && c1.isInvalidateOnChange() == c2.isInvalidateOnChange()
                && c1.getTimeToLiveSeconds() == c2.getTimeToLiveSeconds()
                && c1.getMaxIdleSeconds() == c2.getMaxIdleSeconds()
                && c1.getInMemoryFormat() == c2.getInMemoryFormat()
                && c1.getLocalUpdatePolicy() == c2.getLocalUpdatePolicy()
                && isCompatible(c1.getEvictionConfig(), c2.getEvictionConfig())
                && nullSafeEqual(c1.getPreloaderConfig(), c2.getPreloaderConfig())
            );
        }
    }

    private static class QueryCacheConfigChecker extends ConfigChecker<QueryCacheConfig> {
        @Override
        boolean check(QueryCacheConfig c1, QueryCacheConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null
                && c1.getBatchSize() == c2.getBatchSize()
                && c1.getBufferSize() == c2.getBufferSize()
                && c1.getDelaySeconds() == c2.getDelaySeconds()
                && c1.isIncludeValue() == c2.isIncludeValue()
                && c1.isPopulate() == c2.isPopulate()
                && c1.isCoalesce() == c2.isCoalesce()
                && c1.getInMemoryFormat() == c2.getInMemoryFormat()
                && isCompatible(c1.getEvictionConfig(), c2.getEvictionConfig())
                && isCollectionCompatible(c1.getEntryListenerConfigs(), c2.getEntryListenerConfigs(), new EntryListenerConfigChecker())
                && isCollectionCompatible(c1.getIndexConfigs(), c2.getIndexConfigs(), new IndexConfigChecker())
                && isCompatible(c1.getPredicateConfig(), c2.getPredicateConfig()));
        }
    }

    private static class PredicateConfigChecker extends ConfigChecker<PredicateConfig> {
        @Override
        boolean check(PredicateConfig c1, PredicateConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null
                && nullSafeEqual(
                    classNameOrImpl(c1.getClassName(), c1.getImplementation()),
                    classNameOrImpl(c2.getClassName(), c2.getImplementation()))
                && nullSafeEqual(c1.getSql(), c2.getSql()));
        }
    }

    private static class EvictionConfigChecker extends ConfigChecker<EvictionConfig> {
        @Override
        boolean check(EvictionConfig c1, EvictionConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null
                && c1.getSize() == c2.getSize()
                && c1.getMaxSizePolicy() == c2.getMaxSizePolicy()
                && c1.getEvictionPolicy() == c2.getEvictionPolicy()
                && nullSafeEqual(
                    classNameOrImpl(c1.getComparatorClassName(), c1.getComparator()),
                    classNameOrImpl(c2.getComparatorClassName(), c2.getComparator())));
        }
    }

    private static class RingbufferConfigChecker extends ConfigChecker<RingbufferConfig> {
        @Override
        boolean check(RingbufferConfig c1, RingbufferConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getBackupCount(), c2.getBackupCount())
                    && nullSafeEqual(c1.getAsyncBackupCount(), c2.getAsyncBackupCount())
                    && nullSafeEqual(c1.getCapacity(), c2.getCapacity())
                    && nullSafeEqual(c1.getTimeToLiveSeconds(), c2.getTimeToLiveSeconds())
                    && nullSafeEqual(c1.getInMemoryFormat(), c2.getInMemoryFormat())
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName())
                    && isCompatible(c1.getRingbufferStoreConfig(), c2.getRingbufferStoreConfig())
                    && ConfigCompatibilityChecker.isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig());
        }

        private static boolean isCompatible(RingbufferStoreConfig c1, RingbufferStoreConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                && nullSafeEqual(
                    classNameOrImpl(c1.getClassName(), c1.getStoreImplementation()),
                    classNameOrImpl(c2.getClassName(), c2.getStoreImplementation()))
                && nullSafeEqual(
                    classNameOrImpl(c1.getFactoryClassName(), c1.getFactoryImplementation()),
                    classNameOrImpl(c2.getFactoryClassName(), c2.getFactoryImplementation()))
                && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }

        @Override
        RingbufferConfig getDefault(Config c) {
            return c.getRingbufferConfig("default");
        }
    }

    private static class QueueConfigChecker extends ConfigChecker<QueueConfig> {
        @Override
        boolean check(QueueConfig c1, QueueConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getItemListenerConfigs(), c2.getItemListenerConfigs())
                    && nullSafeEqual(c1.getBackupCount(), c2.getBackupCount())
                    && nullSafeEqual(c1.getAsyncBackupCount(), c2.getAsyncBackupCount())
                    && nullSafeEqual(c1.getMaxSize(), c2.getMaxSize())
                    && nullSafeEqual(c1.getEmptyQueueTtl(), c2.getEmptyQueueTtl())
                    && isCompatible(c1.getQueueStoreConfig(), c2.getQueueStoreConfig())
                    && ConfigCompatibilityChecker.isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName())
                    && nullSafeEqual(c1.getPriorityComparatorClassName(), c2.getPriorityComparatorClassName());
        }

        private static boolean isCompatible(QueueStoreConfig c1, QueueStoreConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                && nullSafeEqual(
                    classNameOrImpl(c1.getClassName(), c1.getStoreImplementation()),
                    classNameOrImpl(c2.getClassName(), c2.getStoreImplementation()))
                && nullSafeEqual(
                    classNameOrImpl(c1.getFactoryClassName(), c1.getFactoryImplementation()),
                    classNameOrImpl(c2.getFactoryClassName(), c2.getFactoryImplementation()))
                && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }

        @Override
        QueueConfig getDefault(Config c) {
            return c.getQueueConfig("default");
        }
    }

    private static class ListConfigChecker extends ConfigChecker<ListConfig> {
        @Override
        boolean check(ListConfig c1, ListConfig c2) {
            return isCompatible(c1, c2);
        }

        @Override
        ListConfig getDefault(Config c) {
            return c.getListConfig("default");
        }
    }

    private static class SetConfigChecker extends ConfigChecker<SetConfig> {
        @Override
        boolean check(SetConfig c1, SetConfig c2) {
            return isCompatible(c1, c2);
        }

        @Override
        SetConfig getDefault(Config c) {
            return c.getSetConfig("default");
        }
    }

    private static class TopicConfigChecker extends ConfigChecker<TopicConfig> {
        @Override
        boolean check(TopicConfig c1, TopicConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.isGlobalOrderingEnabled(), c2.isGlobalOrderingEnabled())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                    && nullSafeEqual(c1.isMultiThreadingEnabled(), c2.isMultiThreadingEnabled())
                    && nullSafeEqual(c1.getMessageListenerConfigs(), c2.getMessageListenerConfigs());
        }

        @Override
        TopicConfig getDefault(Config c) {
            return c.getTopicConfig("default");
        }
    }

    private static class ReliableTopicConfigChecker extends ConfigChecker<ReliableTopicConfig> {
        @Override
        boolean check(ReliableTopicConfig c1, ReliableTopicConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getReadBatchSize(), c2.getReadBatchSize())
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                    && nullSafeEqual(c1.getMessageListenerConfigs(), c2.getMessageListenerConfigs())
                    && nullSafeEqual(c1.getTopicOverloadPolicy(), c2.getTopicOverloadPolicy());
        }

        @Override
        ReliableTopicConfig getDefault(Config c) {
            return c.getReliableTopicConfig("default");
        }
    }

    private static class ExecutorConfigChecker extends ConfigChecker<ExecutorConfig> {
        @Override
        boolean check(ExecutorConfig c1, ExecutorConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }
            int cap1 = c1.getQueueCapacity();
            int cap2 = c2.getQueueCapacity();
            return nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getPoolSize(), c2.getPoolSize())
                    && (nullSafeEqual(cap1, cap2) || (Math.min(cap1, cap2) == 0 && Math.max(cap1, cap2) == Integer.MAX_VALUE))
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled());
        }

        @Override
        ExecutorConfig getDefault(Config c) {
            return c.getExecutorConfig("default");
        }
    }

    private static class DurableExecutorConfigChecker extends ConfigChecker<DurableExecutorConfig> {
        @Override
        boolean check(DurableExecutorConfig c1, DurableExecutorConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getPoolSize(), c2.getPoolSize())
                    && nullSafeEqual(c1.getDurability(), c2.getDurability())
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName())
                    && nullSafeEqual(c1.getCapacity(), c2.getCapacity())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled());
        }

        @Override
        DurableExecutorConfig getDefault(Config c) {
            return c.getDurableExecutorConfig("default");
        }
    }

    private static class ScheduledExecutorConfigChecker extends ConfigChecker<ScheduledExecutorConfig> {
        @Override
        boolean check(ScheduledExecutorConfig c1, ScheduledExecutorConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getDurability(), c2.getDurability())
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName())
                    && isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig())
                    && nullSafeEqual(c1.getPoolSize(), c2.getPoolSize())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled());
        }

        @Override
        ScheduledExecutorConfig getDefault(Config c) {
            return c.getScheduledExecutorConfig("default");
        }
    }

    private static class MultimapConfigChecker extends ConfigChecker<MultiMapConfig> {
        @Override
        boolean check(MultiMapConfig c1, MultiMapConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getValueCollectionType(), c2.getValueCollectionType())
                    && nullSafeEqual(c1.getEntryListenerConfigs(), c2.getEntryListenerConfigs())
                    && nullSafeEqual(c1.isBinary(), c2.isBinary())
                    && nullSafeEqual(c1.getBackupCount(), c2.getBackupCount())
                    && nullSafeEqual(c1.getAsyncBackupCount(), c2.getAsyncBackupCount())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName())
                    && ConfigCompatibilityChecker.isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig());
        }

        @Override
        MultiMapConfig getDefault(Config c) {
            return c.getMultiMapConfig("default");
        }
    }

    private static class ReplicatedMapConfigChecker extends ConfigChecker<ReplicatedMapConfig> {
        @Override
        boolean check(ReplicatedMapConfig c1, ReplicatedMapConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getInMemoryFormat(), c2.getInMemoryFormat())
                    && nullSafeEqual(c1.isAsyncFillup(), c2.isAsyncFillup())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName())
                    && ConfigCompatibilityChecker.isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig())
                    && isCollectionCompatible(c1.getListenerConfigs(), c2.getListenerConfigs(),
                    new ReplicatedMapListenerConfigChecker());
        }

        @Override
        ReplicatedMapConfig getDefault(Config c) {
            return c.getReplicatedMapConfig("default");
        }
    }

    private static class ReplicatedMapListenerConfigChecker extends ConfigChecker<ListenerConfig> {
        @Override
        boolean check(ListenerConfig c1, ListenerConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                && nullSafeEqual(
                    classNameOrImpl(c1.getClassName(), c1.getImplementation()),
                    classNameOrImpl(c2.getClassName(), c2.getImplementation()));
        }
    }

    private static class CardinalityEstimatorConfigChecker extends ConfigChecker<CardinalityEstimatorConfig> {
        @Override
        boolean check(CardinalityEstimatorConfig c1, CardinalityEstimatorConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }
            return nullSafeEqual(c1.getName(), c2.getName())
                    && c1.getBackupCount() == c2.getBackupCount()
                    && c1.getAsyncBackupCount() == c2.getAsyncBackupCount()
                    && c1.getAsyncBackupCount() == c2.getAsyncBackupCount()
                    && isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig())
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName());
        }

        @Override
        CardinalityEstimatorConfig getDefault(Config c) {
            return c.getCardinalityEstimatorConfig("default");
        }
    }

    private static class FlakeIdGeneratorConfigChecker extends ConfigChecker<FlakeIdGeneratorConfig> {
        @Override
        boolean check(FlakeIdGeneratorConfig c1, FlakeIdGeneratorConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }
            return nullSafeEqual(c1.getName(), c2.getName())
                    && c1.getPrefetchCount() == c2.getPrefetchCount()
                    && c1.getPrefetchValidityMillis() == c2.getPrefetchValidityMillis()
                    && c1.getEpochStart() == c2.getEpochStart()
                    && c1.getNodeIdOffset() == c2.getNodeIdOffset()
                    && c1.getBitsSequence() == c2.getBitsSequence()
                    && c1.getBitsNodeId() == c2.getBitsNodeId()
                    && c1.getAllowedFutureMillis() == c2.getAllowedFutureMillis()
                    && c1.isStatisticsEnabled() == c2.isStatisticsEnabled();
        }

        @Override
        FlakeIdGeneratorConfig getDefault(Config c) {
            return c.getFlakeIdGeneratorConfig("default");
        }
    }

    private static class PNCounterConfigChecker extends ConfigChecker<PNCounterConfig> {
        @Override
        boolean check(PNCounterConfig c1, PNCounterConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }
            return nullSafeEqual(c1.getName(), c2.getName())
                    && c1.getReplicaCount() == c2.getReplicaCount()
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName());
        }

        @Override
        PNCounterConfig getDefault(Config c) {
            return c.getPNCounterConfig("default");
        }
    }

    public static class CPSubsystemConfigChecker extends ConfigChecker<CPSubsystemConfig> {

        @Override
        boolean check(CPSubsystemConfig c1, CPSubsystemConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }

            boolean cpSubsystemConfigValuesEqual =
                    (c1.getCPMemberCount() == c2.getCPMemberCount() && c1.getGroupSize() == c2.getGroupSize()
                            && c1.getSessionTimeToLiveSeconds() == c2.getSessionTimeToLiveSeconds()
                            && c1.getSessionHeartbeatIntervalSeconds() == c2.getSessionHeartbeatIntervalSeconds()
                            && c1.getMissingCPMemberAutoRemovalSeconds() == c2.getMissingCPMemberAutoRemovalSeconds()
                            && c1.isFailOnIndeterminateOperationState() == c2.isFailOnIndeterminateOperationState())
                            && c1.isPersistenceEnabled() == c2.isPersistenceEnabled()
                            && c1.getBaseDir().getAbsoluteFile().equals(c2.getBaseDir().getAbsoluteFile())
                            && c1.getDataLoadTimeoutSeconds() == c2.getDataLoadTimeoutSeconds();

            if (!cpSubsystemConfigValuesEqual) {
                return false;
            }

            RaftAlgorithmConfig r1 = c1.getRaftAlgorithmConfig();
            RaftAlgorithmConfig r2 = c2.getRaftAlgorithmConfig();

            final boolean raftAlgorithmConfigEqual =
                    (r1.getLeaderElectionTimeoutInMillis() == r2.getLeaderElectionTimeoutInMillis()
                            && r1.getLeaderHeartbeatPeriodInMillis() == r2.getLeaderHeartbeatPeriodInMillis()
                            && r1.getAppendRequestMaxEntryCount() == r2.getAppendRequestMaxEntryCount()
                            && r1.getMaxMissedLeaderHeartbeatCount() == r2.getMaxMissedLeaderHeartbeatCount()
                            && r1.getCommitIndexAdvanceCountToSnapshot() == r2.getCommitIndexAdvanceCountToSnapshot()
                            && r1.getAppendRequestBackoffTimeoutInMillis() == r2.getAppendRequestBackoffTimeoutInMillis()
                            && r1.getUncommittedEntryCountToRejectNewAppends() == r2.getUncommittedEntryCountToRejectNewAppends());

            if (!raftAlgorithmConfigEqual) {
                return false;
            }

            Map<String, SemaphoreConfig> semaphores1 = c1.getSemaphoreConfigs();

            if (semaphores1.size() != c2.getSemaphoreConfigs().size()) {
                return false;
            }

            for (Entry<String, SemaphoreConfig> e : semaphores1.entrySet()) {
                SemaphoreConfig s2 = c2.findSemaphoreConfig(e.getKey());
                if (s2 == null) {
                    return false;
                }
                if (e.getValue().isJDKCompatible() != s2.isJDKCompatible()) {
                    return false;
                }

                if (e.getValue().getInitialPermits() != s2.getInitialPermits()) {
                    return false;
                }
            }

            Map<String, FencedLockConfig> locks1 = c1.getLockConfigs();

            if (locks1.size() != c2.getLockConfigs().size()) {
                return false;
            }

            for (Entry<String, FencedLockConfig> e : locks1.entrySet()) {
                FencedLockConfig s2 = c2.findLockConfig(e.getKey());
                if (s2 == null) {
                    return false;
                }
                if (e.getValue().getLockAcquireLimit() != s2.getLockAcquireLimit()) {
                    return false;
                }
            }

            return true;
        }

        @Override
        CPSubsystemConfig getDefault(Config c) {
            return c.getCPSubsystemConfig();
        }
    }

    public static class MetricsConfigChecker extends ConfigChecker<MetricsConfig> {

        @Override
        boolean check(MetricsConfig c1, MetricsConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }

            return c1.isEnabled() == c2.isEnabled()
                    && c1.getManagementCenterConfig().isEnabled() == c2.getManagementCenterConfig().isEnabled()
                    && c1.getManagementCenterConfig().getRetentionSeconds() == c2.getManagementCenterConfig()
                                                                                 .getRetentionSeconds()
                    && c1.getJmxConfig().isEnabled() == c2.getJmxConfig().isEnabled()
                    && c1.getCollectionFrequencySeconds() == c2.getCollectionFrequencySeconds();
        }

        @Override
        MetricsConfig getDefault(Config c) {
            return c.getMetricsConfig();
        }
    }

    public static class NativeMemoryConfigChecker extends ConfigChecker<NativeMemoryConfig> {

        @Override
        boolean check(NativeMemoryConfig c1, NativeMemoryConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }

            return c1.isEnabled() == c2.isEnabled()
                    && c1.getAllocatorType() == c2.getAllocatorType()
                    && c1.getMetadataSpacePercentage() == c2.getMetadataSpacePercentage()
                    && c1.getMinBlockSize() == c2.getMinBlockSize()
                    && c1.getPageSize() == c2.getPageSize()
                    && c1.getPersistentMemoryConfig().isEnabled() == c2.getPersistentMemoryConfig().isEnabled()
                    && c1.getPersistentMemoryConfig().getMode() == c2.getPersistentMemoryConfig().getMode()
                    && c1.getPersistentMemoryConfig().getDirectoryConfigs()
                         .equals(c2.getPersistentMemoryConfig().getDirectoryConfigs());
        }

        @Override
        NativeMemoryConfig getDefault(Config c) {
            return c.getNativeMemoryConfig();
        }
    }

    public static class SqlConfigChecker extends ConfigChecker<SqlConfig> {

        @Override
        boolean check(SqlConfig c1, SqlConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }

            return c1.getStatementTimeoutMillis() == c2.getStatementTimeoutMillis();
        }

        @Override
        SqlConfig getDefault(Config c) {
            return c.getSqlConfig();
        }
    }

    public static class InstanceTrackingConfigChecker extends ConfigChecker<InstanceTrackingConfig> {

        @Override
        boolean check(InstanceTrackingConfig c1, InstanceTrackingConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }

            return c1.isEnabled() == c2.isEnabled()
                    && nullSafeEqual(c1.getFileName(), c2.getFileName())
                    && nullSafeEqual(c1.getFormatPattern(), c2.getFormatPattern());
        }

        @Override
        InstanceTrackingConfig getDefault(Config c) {
            return c.getInstanceTrackingConfig();
        }
    }

    private static class CacheSimpleConfigChecker extends ConfigChecker<CacheSimpleConfig> {
        @Override
        boolean check(CacheSimpleConfig c1, CacheSimpleConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getKeyType(), c2.getKeyType())
                    && nullSafeEqual(c1.getValueType(), c2.getValueType())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                    && nullSafeEqual(c1.isManagementEnabled(), c2.isManagementEnabled())
                    && nullSafeEqual(c1.isReadThrough(), c2.isReadThrough())
                    && nullSafeEqual(c1.isWriteThrough(), c2.isWriteThrough())
                    && nullSafeEqual(c1.getCacheLoaderFactory(), c2.getCacheLoaderFactory())
                    && nullSafeEqual(c1.getCacheWriterFactory(), c2.getCacheWriterFactory())
                    && nullSafeEqual(c1.getCacheLoader(), c2.getCacheLoader())
                    && nullSafeEqual(c1.getCacheWriter(), c2.getCacheWriter())
                    && isCompatible(c1.getExpiryPolicyFactoryConfig(), c2.getExpiryPolicyFactoryConfig())
                    && isCollectionCompatible(c1.getCacheEntryListeners(), c2.getCacheEntryListeners(),
                    new CacheSimpleEntryListenerConfigChecker())
                    && nullSafeEqual(c1.getAsyncBackupCount(), c2.getAsyncBackupCount())
                    && nullSafeEqual(c1.getBackupCount(), c2.getBackupCount())
                    && nullSafeEqual(c1.getInMemoryFormat(), c2.getInMemoryFormat())
                    && isCompatible(c1.getEvictionConfig(), c2.getEvictionConfig())
                    && isCompatible(c1.getWanReplicationRef(), c2.getWanReplicationRef())
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName())
                    && nullSafeEqual(c1.getPartitionLostListenerConfigs(), c2.getPartitionLostListenerConfigs())
                    && nullSafeEqual(c1.getMergePolicyConfig(), c2.getMergePolicyConfig())
                    && ConfigCompatibilityChecker.isCompatible(c1.getHotRestartConfig(), c2.getHotRestartConfig())
                    && ConfigCompatibilityChecker.isCompatible(c1.getEventJournalConfig(), c2.getEventJournalConfig());
        }

        private static boolean isCompatible(ExpiryPolicyFactoryConfig c1, ExpiryPolicyFactoryConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && isCompatible(c1.getTimedExpiryPolicyFactoryConfig(), c2.getTimedExpiryPolicyFactoryConfig());
        }

        private static boolean isCompatible(TimedExpiryPolicyFactoryConfig c1, TimedExpiryPolicyFactoryConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getExpiryPolicyType(), c2.getExpiryPolicyType())
                    && isCompatible(c1.getDurationConfig(), c2.getDurationConfig());
        }

        private static boolean isCompatible(DurationConfig c1, DurationConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getDurationAmount(), c2.getDurationAmount())
                    && nullSafeEqual(c1.getTimeUnit(), c2.getTimeUnit());
        }

        private static boolean isCompatible(EvictionConfig c1, EvictionConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getSize(), c2.getSize())
                    && nullSafeEqual(c1.getMaxSizePolicy(), c2.getMaxSizePolicy())
                    && nullSafeEqual(c1.getEvictionPolicy(), c2.getEvictionPolicy())
                    && nullSafeEqual(c1.getComparatorClassName(), c2.getComparatorClassName());
        }

        private static boolean isCompatible(WanReplicationRef c1, WanReplicationRef c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getMergePolicyClassName(), c2.getMergePolicyClassName())
                    && nullSafeEqual(c1.getFilters(), c2.getFilters())
                    && nullSafeEqual(c1.isRepublishingEnabled(), c2.isRepublishingEnabled());
        }

        @Override
        CacheSimpleConfig getDefault(Config c) {
            return c.getCacheConfig("default");
        }
    }

    public static class MapConfigChecker extends ConfigChecker<MapConfig> {
        @Override
        public boolean check(MapConfig c1, MapConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }
            int maxSize1 = c1.getEvictionConfig().getSize();
            int maxSize2 = c2.getEvictionConfig().getSize();

            return nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getInMemoryFormat(), c2.getInMemoryFormat())
                    && nullSafeEqual(c1.getMetadataPolicy(), c2.getMetadataPolicy())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                    && nullSafeEqual(c1.getCacheDeserializedValues(), c2.getCacheDeserializedValues())
                    && nullSafeEqual(c1.getBackupCount(), c2.getBackupCount())
                    && nullSafeEqual(c1.getAsyncBackupCount(), c2.getAsyncBackupCount())
                    && nullSafeEqual(c1.getTimeToLiveSeconds(), c2.getTimeToLiveSeconds())
                    && nullSafeEqual(c1.getMaxIdleSeconds(), c2.getMaxIdleSeconds())
                    && nullSafeEqual(c1.getEvictionConfig().getEvictionPolicy(),
                    c2.getEvictionConfig().getEvictionPolicy())
                    && (nullSafeEqual(maxSize1, maxSize2)
                    || (Math.min(maxSize1, maxSize2) == 0 && Math.max(maxSize1, maxSize2) == Integer.MAX_VALUE))
                    && ConfigCompatibilityChecker.isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig())
                    && nullSafeEqual(c1.isReadBackupData(), c2.isReadBackupData())
                    && ConfigCompatibilityChecker.isCompatible(c1.getMerkleTreeConfig(), c2.getMerkleTreeConfig())
                    && ConfigCompatibilityChecker.isCompatible(c1.getHotRestartConfig(), c2.getHotRestartConfig())
                    && ConfigCompatibilityChecker.isCompatible(c1.getEventJournalConfig(), c2.getEventJournalConfig())
                    && isCompatible(c1.getMapStoreConfig(), c2.getMapStoreConfig())
                    && isCompatible(c1.getNearCacheConfig(), c2.getNearCacheConfig())
                    && isCompatible(c1.getWanReplicationRef(), c2.getWanReplicationRef())
                    && isCollectionCompatible(c1.getIndexConfigs(), c2.getIndexConfigs(), new IndexConfigChecker())
                    && isCollectionCompatible(c1.getAttributeConfigs(), c2.getAttributeConfigs(),
                    new AttributeConfigChecker())
                    && isCollectionCompatible(c1.getEntryListenerConfigs(), c2.getEntryListenerConfigs(),
                    new EntryListenerConfigChecker())
                    && nullSafeEqual(c1.getPartitionLostListenerConfigs(), c2.getPartitionLostListenerConfigs())
                    && nullSafeEqual(c1.getSplitBrainProtectionName(), c2.getSplitBrainProtectionName())
                    && nullSafeEqual(c1.getPartitioningStrategyConfig(), c2.getPartitioningStrategyConfig())
                    && nullSafeEqual(c1.getTieredStoreConfig(), c2.getTieredStoreConfig());
        }

        private static boolean isCompatible(WanReplicationRef c1, WanReplicationRef c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getMergePolicyClassName(), c2.getMergePolicyClassName())
                    && nullSafeEqual(c1.getFilters(), c2.getFilters())
                    && nullSafeEqual(c1.isRepublishingEnabled(), c2.isRepublishingEnabled());
        }

        private static boolean isCompatible(NearCacheConfig c1, NearCacheConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getTimeToLiveSeconds(), c2.getTimeToLiveSeconds())
                    && isCompatible(c1.getEvictionConfig(), c2.getEvictionConfig());
        }

        private static boolean isCompatible(EvictionConfig c1, EvictionConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getSize(), c2.getSize())
                    && nullSafeEqual(c1.getMaxSizePolicy(), c2.getMaxSizePolicy())
                    && nullSafeEqual(c1.getEvictionPolicy(), c2.getEvictionPolicy())
                    && nullSafeEqual(c1.getEvictionStrategyType(), c2.getEvictionStrategyType())
                    && nullSafeEqual(c1.getComparatorClassName(), c2.getComparatorClassName());
        }

        private static boolean isCompatible(MapStoreConfig c1, MapStoreConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                && nullSafeEqual(
                    classNameOrImpl(c1.getClassName(), c1.getImplementation()),
                    classNameOrImpl(c2.getClassName(), c2.getImplementation()))
                && nullSafeEqual(
                    classNameOrImpl(c1.getFactoryClassName(), c1.getFactoryImplementation()),
                    classNameOrImpl(c2.getFactoryClassName(), c2.getFactoryImplementation()))
                && nullSafeEqual(c1.getProperties(), c2.getProperties()))
                && nullSafeEqual(c1.isWriteCoalescing(), c2.isWriteCoalescing())
                && nullSafeEqual(c1.getWriteBatchSize(), c2.getWriteBatchSize())
                && nullSafeEqual(c1.getWriteDelaySeconds(), c2.getWriteDelaySeconds())
                && nullSafeEqual(c1.getInitialLoadMode(), c2.getInitialLoadMode());
        }

        @Override
        MapConfig getDefault(Config c) {
            return c.getMapConfig("default");
        }
    }

    private static class IndexConfigChecker extends ConfigChecker<IndexConfig> {
        @Override
        boolean check(IndexConfig c1, IndexConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null) && nullSafeEqual(c1, c2);
        }
    }

    private static class DynamicConfigurationConfigChecker extends ConfigChecker<DynamicConfigurationConfig> {
        @Override
        boolean check(DynamicConfigurationConfig t1, DynamicConfigurationConfig t2) {
            return Objects.equals(t1, t2);
        }
    }

    private static class DeviceConfigChecker extends ConfigChecker<DeviceConfig> {
        @Override
        boolean check(DeviceConfig t1, DeviceConfig t2) {
            return Objects.equals(t1, t2);
        }
    }

    private static class CacheSimpleEntryListenerConfigChecker extends ConfigChecker<CacheSimpleEntryListenerConfig> {
        @Override
        boolean check(CacheSimpleEntryListenerConfig c1, CacheSimpleEntryListenerConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getCacheEntryListenerFactory(), c2.getCacheEntryListenerFactory())
                    && nullSafeEqual(c1.getCacheEntryEventFilterFactory(), c2.getCacheEntryEventFilterFactory())
                    && nullSafeEqual(c1.isOldValueRequired(), c2.isOldValueRequired())
                    && nullSafeEqual(c1.isSynchronous(), c2.isSynchronous());
        }
    }

    private static class EntryListenerConfigChecker extends ConfigChecker<EntryListenerConfig> {
        @Override
        boolean check(EntryListenerConfig c1, EntryListenerConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.isLocal(), c2.isLocal())
                    && nullSafeEqual(c1.isIncludeValue(), c2.isIncludeValue())
                    && nullSafeEqual(
                        classNameOrImpl(c1.getClassName(), c1.getImplementation()),
                        classNameOrImpl(c2.getClassName(), c2.getImplementation()));
        }
    }

    private static class AttributeConfigChecker extends ConfigChecker<AttributeConfig> {
        @Override
        boolean check(AttributeConfig c1, AttributeConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getExtractorClassName(), c2.getExtractorClassName());
        }
    }

    public static class DiscoveryStrategyConfigChecker extends ConfigChecker<DiscoveryStrategyConfig> {
        @Override
        public boolean check(DiscoveryStrategyConfig c1, DiscoveryStrategyConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && discoveryStrategyClassNameEqual(c1, c2)
                    && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }

        private boolean discoveryStrategyClassNameEqual(DiscoveryStrategyConfig c1, DiscoveryStrategyConfig c2) {
            return nullSafeEqual(
                classNameOrImpl(c1.getClassName(), c1.getDiscoveryStrategyFactory()),
                classNameOrImpl(c2.getClassName(), c2.getDiscoveryStrategyFactory()));
        }
    }

    private static class MemberGroupConfigChecker extends ConfigChecker<MemberGroupConfig> {
        @Override
        boolean check(MemberGroupConfig c1, MemberGroupConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(new ArrayList<>(c1.getInterfaces()), new ArrayList<>(c2.getInterfaces()));
        }
    }

    private static class SerializerConfigChecker extends ConfigChecker<SerializerConfig> {
        @Override
        boolean check(SerializerConfig c1, SerializerConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null
                && nullSafeEqual(
                    classNameOrImpl(c1.getClassName(), c1.getImplementation()),
                    classNameOrImpl(c2.getClassName(), c2.getImplementation()))
                && nullSafeEqual(
                    classNameOrClass(c1.getTypeClassName(), c1.getTypeClass()),
                    classNameOrClass(c2.getTypeClassName(), c2.getTypeClass())));
        }
    }

    public static class WanSyncConfigChecker extends ConfigChecker<WanSyncConfig> {
        @Override
        public boolean check(WanSyncConfig c1, WanSyncConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getConsistencyCheckStrategy(), c2.getConsistencyCheckStrategy());
        }
    }

    private static class NetworkConfigChecker extends ConfigChecker<NetworkConfig> {
        private static final JoinConfigChecker JOIN_CONFIG_CHECKER = new JoinConfigChecker();
        private static final IcmpFailureDetectorConfigChecker FAILURE_DETECTOR_CONFIG_CHECKER
                = new IcmpFailureDetectorConfigChecker();
        private static final MemberAddressProviderConfigChecker MEMBER_ADDRESS_PROVIDER_CONFIG_CHECKER
                = new MemberAddressProviderConfigChecker();
        private static final OutboundPortDefinitionsChecker OUTBOUND_PORT_DEFINITIONS_CHECKER
                = new OutboundPortDefinitionsChecker();
        private static final InterfacesConfigChecker INTERFACES_CONFIG_CHECKER
                = new InterfacesConfigChecker();
        private static final SSLConfigChecker SSL_CONFIG_CHECKER
                = new SSLConfigChecker();
        private static final SocketInterceptorConfigChecker SOCKET_INTERCEPTOR_CONFIG_CHECKER
                = new SocketInterceptorConfigChecker();
        private static final SymmetricEncryptionConfigChecker SYMMETRIC_ENCRYPTION_CONFIG_CHECKER
                = new SymmetricEncryptionConfigChecker();
        private static final RestApiConfigChecker REST_API_CONFIG_CHECKER = new RestApiConfigChecker();
        private static final MemcacheProtocolConfigChecker MEMCACHE_PROTOCOL_CONFIG_CHECKER
                = new MemcacheProtocolConfigChecker();

        @Override
        boolean check(NetworkConfig c1, NetworkConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getPort(), c2.getPort())
                    && nullSafeEqual(c1.getPortCount(), c2.getPortCount())
                    && nullSafeEqual(c1.isPortAutoIncrement(), c2.isPortAutoIncrement())
                    && nullSafeEqual(c1.isReuseAddress(), c2.isReuseAddress())
                    && nullSafeEqual(c1.getPublicAddress(), c2.getPublicAddress())
                    && OUTBOUND_PORT_DEFINITIONS_CHECKER.check(c1.getOutboundPortDefinitions(),
                    c2.getOutboundPortDefinitions())
                    && nullSafeEqual(c1.getOutboundPorts(), c2.getOutboundPorts())
                    && INTERFACES_CONFIG_CHECKER.check(c1.getInterfaces(), c2.getInterfaces())
                    && JOIN_CONFIG_CHECKER.check(c1.getJoin(), c2.getJoin())
                    && FAILURE_DETECTOR_CONFIG_CHECKER.check(c1.getIcmpFailureDetectorConfig(),
                    c2.getIcmpFailureDetectorConfig())
                    && MEMBER_ADDRESS_PROVIDER_CONFIG_CHECKER.check(c1.getMemberAddressProviderConfig(),
                    c2.getMemberAddressProviderConfig())
                    && SYMMETRIC_ENCRYPTION_CONFIG_CHECKER.check(c1.getSymmetricEncryptionConfig(),
                    c2.getSymmetricEncryptionConfig())
                    && SOCKET_INTERCEPTOR_CONFIG_CHECKER.check(c1.getSocketInterceptorConfig(),
                    c2.getSocketInterceptorConfig())
                    && SSL_CONFIG_CHECKER.check(c1.getSSLConfig(), c2.getSSLConfig())
                    && REST_API_CONFIG_CHECKER.check(c1.getRestApiConfig(), c2.getRestApiConfig())
                    && MEMCACHE_PROTOCOL_CONFIG_CHECKER.check(
                    c1.getMemcacheProtocolConfig(),
                    c2.getMemcacheProtocolConfig());
        }
    }

    private static class AdvancedNetworkConfigChecker extends ConfigChecker<AdvancedNetworkConfig> {

        private static final JoinConfigChecker JOIN_CONFIG_CHECKER = new JoinConfigChecker();
        private static final IcmpFailureDetectorConfigChecker FAILURE_DETECTOR_CONFIG_CHECKER
                = new IcmpFailureDetectorConfigChecker();
        private static final MemberAddressProviderConfigChecker MEMBER_ADDRESS_PROVIDER_CONFIG_CHECKER
                = new MemberAddressProviderConfigChecker();
        private static final EndpointConfigChecker ENDPOINT_CONFIG_CHECKER
                = new EndpointConfigChecker();

        @Override
        boolean check(AdvancedNetworkConfig t1, AdvancedNetworkConfig t2) {
            boolean t1Disabled = t1 == null || !t1.isEnabled();
            boolean t2Disabled = t1 == null || !t2.isEnabled();
            if (t1Disabled && t2Disabled) {
                return true;
            }
            boolean compatible = JOIN_CONFIG_CHECKER.check(t1.getJoin(), t2.getJoin())
                    && FAILURE_DETECTOR_CONFIG_CHECKER.check(t1.getIcmpFailureDetectorConfig(),
                    t2.getIcmpFailureDetectorConfig())
                    && MEMBER_ADDRESS_PROVIDER_CONFIG_CHECKER.check(t1.getMemberAddressProviderConfig(),
                    t2.getMemberAddressProviderConfig());

            if (!compatible || (t1.getEndpointConfigs().size() != t2.getEndpointConfigs().size())) {
                return false;
            }
            Set<EndpointQualifier> t1EndpointQualifiers = t1.getEndpointConfigs().keySet();
            Set<EndpointQualifier> t2EndpointQualifiers = t2.getEndpointConfigs().keySet();
            isCollectionCompatible(t1EndpointQualifiers, t2EndpointQualifiers,
                    new ConfigChecker<EndpointQualifier>() {
                        @Override
                        boolean check(EndpointQualifier t1, EndpointQualifier t2) {
                            return t1.equals(t2);
                        }
                    });

            boolean endpointConfigsCompatible = true;
            for (EndpointQualifier endpointQualifier : t1EndpointQualifiers) {
                EndpointConfig t1Config = t1.getEndpointConfigs().get(endpointQualifier);
                EndpointConfig t2Config = t2.getEndpointConfigs().get(endpointQualifier);
                endpointConfigsCompatible = endpointConfigsCompatible
                        && (t1Config.getClass().equals(t2Config.getClass()))
                        && ENDPOINT_CONFIG_CHECKER.check(t1Config, t2Config);
            }
            return endpointConfigsCompatible;
        }
    }

    private static class JoinConfigChecker extends ConfigChecker<JoinConfig> {
        private static final AliasedDiscoveryConfigsChecker ALIASED_DISCOVERY_CONFIGS_CHECKER
                = new AliasedDiscoveryConfigsChecker();
        private static final DiscoveryConfigChecker DISCOVERY_CONFIG_CHECKER = new DiscoveryConfigChecker();

        @Override
        boolean check(JoinConfig t1, JoinConfig t2) {
            return isCompatible(t1, t2);
        }

        private static boolean isCompatible(JoinConfig c1, JoinConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && isCompatible(c1.getMulticastConfig(), c2.getMulticastConfig())
                    && isCompatible(c1.getTcpIpConfig(), c2.getTcpIpConfig())
                    && ALIASED_DISCOVERY_CONFIGS_CHECKER.check(
                    AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(c1),
                    AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(c2))
                    && DISCOVERY_CONFIG_CHECKER.check(c1.getDiscoveryConfig(), c2.getDiscoveryConfig());
        }

        private static boolean isCompatible(TcpIpConfig c1, TcpIpConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getConnectionTimeoutSeconds(), c2.getConnectionTimeoutSeconds())
                    && nullSafeEqual(c1.getMembers(), c2.getMembers()))
                    && nullSafeEqual(c1.getRequiredMember(), c2.getRequiredMember());
        }

        private static boolean isCompatible(MulticastConfig c1, MulticastConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getMulticastGroup(), c2.getMulticastGroup())
                    && nullSafeEqual(c1.getMulticastPort(), c2.getMulticastPort()))
                    && nullSafeEqual(c1.getMulticastTimeoutSeconds(), c2.getMulticastTimeoutSeconds())
                    && nullSafeEqual(c1.getMulticastTimeToLive(), c2.getMulticastTimeToLive())
                    && nullSafeEqual(c1.getTrustedInterfaces(), c2.getTrustedInterfaces());
        }
    }

    private static class IcmpFailureDetectorConfigChecker extends ConfigChecker<IcmpFailureDetectorConfig> {
        @Override
        boolean check(IcmpFailureDetectorConfig t1, IcmpFailureDetectorConfig t2) {
            boolean t1Disabled = t1 == null || !t1.isEnabled();
            boolean t2Disabled = t2 == null || !t2.isEnabled();
            return t1 == t2 || (t1Disabled && t2Disabled) || (t1 != null && t2 != null
                    && t1.isParallelMode() == t2.isParallelMode()
                    && t1.isFailFastOnStartup() == t2.isFailFastOnStartup()
                    && t1.getMaxAttempts() == t2.getMaxAttempts()
                    && t1.getIntervalMilliseconds() == t2.getIntervalMilliseconds()
                    && t1.getTimeoutMilliseconds() == t2.getTimeoutMilliseconds()
                    && t1.getTtl() == t2.getTtl()
            );
        }
    }

    private static class MemberAddressProviderConfigChecker extends ConfigChecker<MemberAddressProviderConfig> {

        @Override
        boolean check(MemberAddressProviderConfig t1, MemberAddressProviderConfig t2) {
            boolean t1Disabled = t1 == null || !t1.isEnabled();
            boolean t2Disabled = t2 == null || !t2.isEnabled();
            return t1 == t2 || (t1Disabled && t2Disabled) || (t1 != null && t2 != null
                && nullSafeEqual(
                    classNameOrImpl(t1.getClassName(), t1.getImplementation()),
                    classNameOrImpl(t2.getClassName(), t2.getImplementation()))
            );
        }
    }

    private static class EndpointConfigChecker extends ConfigChecker<EndpointConfig> {

        private static final OutboundPortDefinitionsChecker OUTBOUND_PORT_DEFINITIONS_CHECKER
                = new OutboundPortDefinitionsChecker();
        private static final InterfacesConfigChecker INTERFACES_CONFIG_CHECKER
                = new InterfacesConfigChecker();
        private static final SSLConfigChecker SSL_CONFIG_CHECKER
                = new SSLConfigChecker();
        private static final SocketInterceptorConfigChecker SOCKET_INTERCEPTOR_CONFIG_CHECKER
                = new SocketInterceptorConfigChecker();
        private static final SymmetricEncryptionConfigChecker SYMMETRIC_ENCRYPTION_CONFIG_CHECKER
                = new SymmetricEncryptionConfigChecker();

        @Override
        boolean check(EndpointConfig c1, EndpointConfig c2) {
            if (c1.getClass() != c2.getClass()) {
                return false;
            }
            boolean compatible = c1 == c2 || !(c1 == null || c2 == null)
                    && OUTBOUND_PORT_DEFINITIONS_CHECKER.check(c1.getOutboundPortDefinitions(),
                    c2.getOutboundPortDefinitions())
                    && nullSafeEqual(c1.getOutboundPorts(), c2.getOutboundPorts())
                    && INTERFACES_CONFIG_CHECKER.check(c1.getInterfaces(), c2.getInterfaces())
                    && SSL_CONFIG_CHECKER.check(c1.getSSLConfig(), c2.getSSLConfig())
                    && SOCKET_INTERCEPTOR_CONFIG_CHECKER.check(c1.getSocketInterceptorConfig(),
                    c2.getSocketInterceptorConfig())
                    && SYMMETRIC_ENCRYPTION_CONFIG_CHECKER.check(c1.getSymmetricEncryptionConfig(),
                    c2.getSymmetricEncryptionConfig())
                    && (c1.isSocketBufferDirect() == c2.isSocketBufferDirect())
                    && (c1.isSocketKeepAlive() == c2.isSocketKeepAlive())
                    && (c1.isSocketBufferDirect() == c2.isSocketBufferDirect())
                    && (c1.getSocketConnectTimeoutSeconds() == c2.getSocketConnectTimeoutSeconds())
                    && (c1.getSocketLingerSeconds() == c2.getSocketLingerSeconds())
                    && (c1.getSocketRcvBufferSizeKb() == c2.getSocketRcvBufferSizeKb())
                    && (c1.getSocketSendBufferSizeKb() == c2.getSocketSendBufferSizeKb());

            if (c1 instanceof ServerSocketEndpointConfig) {
                ServerSocketEndpointConfig s1 = (ServerSocketEndpointConfig) c1;
                ServerSocketEndpointConfig s2 = (ServerSocketEndpointConfig) c2;
                return compatible && nullSafeEqual(s1.getPort(), s2.getPort())
                        && nullSafeEqual(s1.getPortCount(), s2.getPortCount())
                        && (s1.isPortAutoIncrement() == s2.isPortAutoIncrement())
                        && nullSafeEqual(s1.isReuseAddress(), s2.isReuseAddress())
                        && nullSafeEqual(s1.getPublicAddress(), s2.getPublicAddress());
            } else {
                return compatible;
            }
        }
    }

    private static class OutboundPortDefinitionsChecker extends ConfigChecker<Collection<String>> {

        @Override
        boolean check(Collection<String> portDefinitions1, Collection<String> portDefinitions2) {
            String[] defaultValues = {"0", "*"};
            boolean defaultDefinition1 = CollectionUtil.isEmpty(portDefinitions1)
                    || (portDefinitions1.size() == 1 && contains(defaultValues, portDefinitions1.iterator().next()));
            boolean defaultDefinition2 = CollectionUtil.isEmpty(portDefinitions2)
                    || (portDefinitions2.size() == 1 && contains(defaultValues, portDefinitions2.iterator().next()));
            return (defaultDefinition1 && defaultDefinition2) || nullSafeEqual(portDefinitions1, portDefinitions2);
        }
    }

    private static class InterfacesConfigChecker extends ConfigChecker<InterfacesConfig> {
        @Override
        boolean check(InterfacesConfig c1, InterfacesConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(new ArrayList<>(c1.getInterfaces()), new ArrayList<>(c2.getInterfaces())));
        }
    }

    private static class SSLConfigChecker extends ConfigChecker<SSLConfig> {
        @Override
        boolean check(SSLConfig c1, SSLConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(
                        classNameOrImpl(c1.getFactoryClassName(), c1.getFactoryImplementation()),
                        classNameOrImpl(c2.getFactoryClassName(), c2.getFactoryImplementation()))
                    && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }
    }

    public static class AuditlogConfigChecker extends ConfigChecker<AuditlogConfig> {
        @Override
        boolean check(AuditlogConfig c1, AuditlogConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getFactoryClassName(), c2.getFactoryClassName())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }
    }

    private static class SocketInterceptorConfigChecker extends ConfigChecker<SocketInterceptorConfig> {
        @Override
        boolean check(SocketInterceptorConfig c1, SocketInterceptorConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                && nullSafeEqual(
                    classNameOrImpl(c1.getClassName(), c1.getImplementation()),
                    classNameOrImpl(c2.getClassName(), c2.getImplementation()))
                && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }
    }

    private static class SymmetricEncryptionConfigChecker extends ConfigChecker<SymmetricEncryptionConfig> {
        @Override
        boolean check(SymmetricEncryptionConfig c1, SymmetricEncryptionConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getSalt(), c2.getSalt())
                    && nullSafeEqual(c1.getPassword(), c2.getPassword()))
                    && nullSafeEqual(c1.getIterationCount(), c2.getIterationCount())
                    && nullSafeEqual(c1.getAlgorithm(), c2.getAlgorithm())
                    && nullSafeEqual(c1.getKey(), c2.getKey());
        }
    }

    public static class DiscoveryConfigChecker extends ConfigChecker<DiscoveryConfig> {
        @Override
        public boolean check(DiscoveryConfig c1, DiscoveryConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nodeFilterClassNameEqual(c1, c2)
                    && nullSafeEqual(c1.getDiscoveryServiceProvider(), c2.getDiscoveryServiceProvider())
                    && isCollectionCompatible(c1.getDiscoveryStrategyConfigs(), c2.getDiscoveryStrategyConfigs(),
                    new DiscoveryStrategyConfigChecker()));
        }

        private boolean nodeFilterClassNameEqual(DiscoveryConfig c1, DiscoveryConfig c2) {
            return classNameOrImpl(c1.getNodeFilterClass(), c1.getNodeFilter())
                .equals(classNameOrImpl(c2.getNodeFilterClass(), c2.getNodeFilter()));
        }
    }

    private static String classNameOrImpl(String className, Object impl) {
        return impl != null ? impl.getClass().getName() : className;
    }

    private static String classNameOrClass(String className, Class<?> clazz) {
        return clazz != null ? clazz.getName() : className;
    }

    public static class AliasedDiscoveryConfigsChecker extends ConfigChecker<List<AliasedDiscoveryConfig<?>>> {

        @Override
        boolean check(List<AliasedDiscoveryConfig<?>> t1, List<AliasedDiscoveryConfig<?>> t2) {
            Map<String, AliasedDiscoveryConfig> m1 = mapByTag(t1);
            Map<String, AliasedDiscoveryConfig> m2 = mapByTag(t2);

            if (m1.size() != m2.size()) {
                return false;
            }

            for (String tag : m1.keySet()) {
                AliasedDiscoveryConfig c1 = m1.get(tag);
                AliasedDiscoveryConfig c2 = m2.get(tag);
                if (!check(c1, c2)) {
                    return false;
                }
            }

            return true;
        }

        private static Map<String, AliasedDiscoveryConfig> mapByTag(List<AliasedDiscoveryConfig<?>> configs) {
            Map<String, AliasedDiscoveryConfig> result = new HashMap<>();
            for (AliasedDiscoveryConfig c : configs) {
                if (c.isEnabled()) {
                    result.put(c.getTag(), c);
                }
            }
            return result;
        }

        public static boolean check(AliasedDiscoveryConfig c1, AliasedDiscoveryConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getTag(), c2.getTag())
                    && nullSafeEqual(c1.isUsePublicIp(), c2.isUsePublicIp())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }
    }

    public static class WanReplicationConfigChecker extends ConfigChecker<WanReplicationConfig> {
        private static final WanConsumerConfigChecker WAN_CONSUMER_CONFIG_CHECKER = new WanConsumerConfigChecker();

        @Override
        public boolean check(WanReplicationConfig c1, WanReplicationConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && WAN_CONSUMER_CONFIG_CHECKER.check(c1.getConsumerConfig(), c2.getConsumerConfig())
                    && isCollectionCompatible(c1.getBatchPublisherConfigs(), c2.getBatchPublisherConfigs(),
                    new WanBatchPublisherConfigChecker())
                    && isCollectionCompatible(c1.getCustomPublisherConfigs(), c2.getCustomPublisherConfigs(),
                    new WanCustomPublisherConfigChecker());
        }
    }

    public static class WanConsumerConfigChecker extends ConfigChecker<WanConsumerConfig> {
        @Override
        public boolean check(WanConsumerConfig c1, WanConsumerConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation())
                    && nullSafeEqual(c1.isPersistWanReplicatedData(), c2.isPersistWanReplicatedData())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }
    }

    public static class WanCustomPublisherConfigChecker extends ConfigChecker<WanCustomPublisherConfig> {
        @Override
        public boolean check(WanCustomPublisherConfig c1, WanCustomPublisherConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getPublisherId(), c2.getPublisherId())
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }
    }

    public static class WanBatchPublisherConfigChecker extends ConfigChecker<WanBatchPublisherConfig> {
        @Override
        public boolean check(WanBatchPublisherConfig c1, WanBatchPublisherConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getClusterName(), c2.getClusterName())
                    && nullSafeEqual(c1.getPublisherId(), c2.getPublisherId())
                    && c1.isSnapshotEnabled() == c2.isSnapshotEnabled()
                    && c1.getInitialPublisherState() == c2.getInitialPublisherState()
                    && c1.getQueueCapacity() == c2.getQueueCapacity()
                    && c1.getBatchSize() == c2.getBatchSize()
                    && c1.getBatchMaxDelayMillis() == c2.getBatchMaxDelayMillis()
                    && c1.getResponseTimeoutMillis() == c2.getResponseTimeoutMillis()
                    && c1.getQueueFullBehavior() == c2.getQueueFullBehavior()
                    && c1.getAcknowledgeType() == c2.getAcknowledgeType()
                    && c1.getDiscoveryPeriodSeconds() == c2.getDiscoveryPeriodSeconds()
                    && c1.getMaxTargetEndpoints() == c2.getMaxTargetEndpoints()
                    && c1.getMaxConcurrentInvocations() == c2.getMaxConcurrentInvocations()
                    && c1.isUseEndpointPrivateAddress() == c2.isUseEndpointPrivateAddress()
                    && c1.getIdleMinParkNs() == c2.getIdleMinParkNs()
                    && c1.getIdleMaxParkNs() == c2.getIdleMaxParkNs()
                    && nullSafeEqual(c1.getTargetEndpoints(), c2.getTargetEndpoints())
                    && new AliasedDiscoveryConfigsChecker().check(aliasedDiscoveryConfigsFrom(c1), aliasedDiscoveryConfigsFrom(c2))
                    && new DiscoveryConfigChecker().check(c1.getDiscoveryConfig(), c2.getDiscoveryConfig())
                    && new WanSyncConfigChecker().check(c1.getSyncConfig(), c2.getSyncConfig())
                    && nullSafeEqual(c1.getEndpoint(), c2.getEndpoint())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }
    }

    private static class PartitionGroupConfigChecker extends ConfigChecker<PartitionGroupConfig> {
        @Override
        boolean check(PartitionGroupConfig c1, PartitionGroupConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getGroupType(), c2.getGroupType())
                    && isCollectionCompatible(c1.getMemberGroupConfigs(), c2.getMemberGroupConfigs(),
                    new MemberGroupConfigChecker()));
        }
    }

    private static class SerializationConfigChecker extends ConfigChecker<SerializationConfig> {
        @Override
        boolean check(SerializationConfig c1, SerializationConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getPortableVersion(), c2.getPortableVersion())
                    && nullSafeEqual(c1.getDataSerializableFactoryClasses(), c2.getDataSerializableFactoryClasses())
                    && nullSafeEqual(c1.getPortableFactoryClasses(), c2.getPortableFactoryClasses())
                    && isCompatible(c1.getGlobalSerializerConfig(), c2.getGlobalSerializerConfig())
                    && isCollectionCompatible(c1.getSerializerConfigs(), c2.getSerializerConfigs(), new SerializerConfigChecker())
                    && nullSafeEqual(c1.isCheckClassDefErrors(), c2.isCheckClassDefErrors())
                    && nullSafeEqual(c1.isUseNativeByteOrder(), c2.isUseNativeByteOrder())
                    && nullSafeEqual(c1.getByteOrder(), c2.getByteOrder())
                    && nullSafeEqual(c1.isEnableCompression(), c2.isEnableCompression())
                    && nullSafeEqual(c1.isEnableSharedObject(), c2.isEnableSharedObject())
                    && nullSafeEqual(c1.isAllowUnsafe(), c2.isAllowUnsafe())
                    && nullSafeEqual(c1.isAllowOverrideDefaultSerializers(), c2.isAllowOverrideDefaultSerializers())
                    && nullSafeEqual(c1.getJavaSerializationFilterConfig(), c2.getJavaSerializationFilterConfig())
                    && nullSafeEqual(c1.getCompactSerializationConfig(), c2.getCompactSerializationConfig());
        }

        private static boolean isCompatible(GlobalSerializerConfig c1, GlobalSerializerConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.isOverrideJavaSerialization(), c2.isOverrideJavaSerialization());
        }
    }

    private static class ServicesConfigChecker extends ConfigChecker<ServicesConfig> {
        @Override
        boolean check(ServicesConfig c1, ServicesConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.isEnableDefaults(), c2.isEnableDefaults())
                    && isCompatible(c1.getServiceConfigs(), c2.getServiceConfigs());
        }

        private static boolean isCompatible(Collection<ServiceConfig> c1, Collection<ServiceConfig> c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null || c1.size() != c2.size()) {
                return false;
            }

            HashMap<String, ServiceConfig> config1 = new HashMap<>();
            HashMap<String, ServiceConfig> config2 = new HashMap<>();

            for (ServiceConfig serviceConfig : c1) {
                config1.put(serviceConfig.getName(), serviceConfig);
            }
            for (ServiceConfig serviceConfig : c2) {
                config2.put(serviceConfig.getName(), serviceConfig);
            }

            if (!config1.keySet().equals(config2.keySet())) {
                return false;
            }

            for (ServiceConfig serviceConfig : c1) {
                if (!isCompatible(serviceConfig, config2.get(serviceConfig.getName()))) {
                    return false;
                }
            }
            return true;
        }

        private static boolean isCompatible(ServiceConfig c1, ServiceConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties())
                    && nullSafeEqual(c1.getConfigObject(), c2.getConfigObject()));
        }
    }

    private static class SecurityConfigChecker extends ConfigChecker<SecurityConfig> {

        @Override
        boolean check(SecurityConfig c1, SecurityConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.isEnabled(), c2.isEnabled())
                    && (c1.getOnJoinPermissionOperation() == c2.getOnJoinPermissionOperation())
                    && nullSafeEqual(c1.getClientBlockUnmappedActions(), c2.getClientBlockUnmappedActions())
                    && nullSafeEqual(c1.getClientRealm(), c2.getClientRealm())
                    && nullSafeEqual(c1.getMemberRealm(), c2.getMemberRealm())
                    && isCompatible(c1.getRealmConfigs(), c2.getRealmConfigs())
                    && isCompatible(c1.getSecurityInterceptorConfigs(), c2.getSecurityInterceptorConfigs())
                    && isCompatible(c1.getClientPolicyConfig(), c2.getClientPolicyConfig())
                    && isCompatible(c1.getClientPermissionConfigs(), c2.getClientPermissionConfigs())
                    ;
        }

        private static boolean isCompatible(Map<String, RealmConfig> c1, Map<String, RealmConfig> c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null || c1.size() != c2.size()) {
                return false;
            }
            for (String realmName : c1.keySet()) {
                if (!isCompatible(c1.get(realmName), c2.get(realmName))) {
                    return false;
                }
            }
            return true;
        }

        private static boolean isCompatible(RealmConfig c1, RealmConfig c2) {
            if (c1 == null || c2 == null) {
                return false;
            }
            return isCompatible(c1.getCredentialsFactoryConfig(), c2.getCredentialsFactoryConfig())
                    && isCompatible(c1.getJaasAuthenticationConfig(), c2.getJaasAuthenticationConfig())
                    && isCompatible(c1.getTlsAuthenticationConfig(), c2.getTlsAuthenticationConfig())
                    && isCompatible(c1.getLdapAuthenticationConfig(), c2.getLdapAuthenticationConfig())
                    && isCompatible(c1.getKerberosAuthenticationConfig(), c2.getKerberosAuthenticationConfig())
                    && isCompatible(c1.getUsernamePasswordIdentityConfig(), c2.getUsernamePasswordIdentityConfig())
                    && isCompatible(c1.getTokenIdentityConfig(), c2.getTokenIdentityConfig())
                    && isCompatible(c1.getKerberosIdentityConfig(), c2.getKerberosIdentityConfig())
                    ;
        }

        private static boolean isCompatible(UsernamePasswordIdentityConfig c1, UsernamePasswordIdentityConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null && nullSafeEqual(c1.getUsername(), c2.getUsername())
                    && nullSafeEqual(c1.getPassword(), c2.getPassword()));
        }

        private static boolean isCompatible(TokenIdentityConfig c1, TokenIdentityConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null && c1.equals(c2));
        }

        private static boolean isCompatible(TlsAuthenticationConfig c1, TlsAuthenticationConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null && nullSafeEqual(c1.getRoleAttribute(), c2.getRoleAttribute()));
        }

        private static boolean isCompatible(LdapAuthenticationConfig c1, LdapAuthenticationConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null && c1.equals(c2));
        }

        private static boolean isCompatible(KerberosAuthenticationConfig c1, KerberosAuthenticationConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null && c1.equals(c2));
        }

        private static boolean isCompatible(KerberosIdentityConfig c1, KerberosIdentityConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null && c1.equals(c2));
        }

        private static boolean isCompatible(JaasAuthenticationConfig c1, JaasAuthenticationConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null
                    && isCompatibleLoginModule(c1.getLoginModuleConfigs(), c2.getLoginModuleConfigs()));
        }

        private static boolean isCompatible(CredentialsFactoryConfig c1, CredentialsFactoryConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getProperties(), c2.getProperties())
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation());
        }

        private static boolean isCompatibleLoginModule(List<LoginModuleConfig> c1, List<LoginModuleConfig> c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null || c1.size() != c2.size()) {
                return false;
            }

            Map<String, LoginModuleConfig> config1 = new HashMap<>();
            Map<String, LoginModuleConfig> config2 = new HashMap<>();

            for (LoginModuleConfig loginModuleConfig : c1) {
                config1.put(loginModuleConfig.getClassName(), loginModuleConfig);
            }
            for (LoginModuleConfig loginModuleConfig : c2) {
                config2.put(loginModuleConfig.getClassName(), loginModuleConfig);
            }

            if (!config1.keySet().equals(config2.keySet())) {
                return false;
            }

            for (LoginModuleConfig a : c1) {
                LoginModuleConfig b = config2.get(a.getClassName());

                if (!(a == b || (nullSafeEqual(a.getProperties(), b.getProperties())
                        && nullSafeEqual(a.getUsage(), b.getUsage())))) {
                    return false;
                }
            }

            return true;
        }

        private static boolean isCompatible(List<SecurityInterceptorConfig> c1, List<SecurityInterceptorConfig> c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null || c1.size() != c2.size()) {
                return false;
            }

            Map<String, SecurityInterceptorConfig> config1 = new HashMap<>();
            Map<String, SecurityInterceptorConfig> config2 = new HashMap<>();

            for (SecurityInterceptorConfig securityInterceptorConfig : c1) {
                config1.put(securityInterceptorConfig.getClassName(), securityInterceptorConfig);
            }
            for (SecurityInterceptorConfig securityInterceptorConfig : c2) {
                config2.put(securityInterceptorConfig.getClassName(), securityInterceptorConfig);
            }

            if (!config1.keySet().equals(config2.keySet())) {
                return false;
            }

            for (SecurityInterceptorConfig a : c1) {
                SecurityInterceptorConfig b = config2.get(a.getClassName());

                if (!(a == b || nullSafeEqual(a.getImplementation(), b.getImplementation()))) {
                    return false;
                }
            }

            return true;
        }

        private static boolean isCompatible(PermissionPolicyConfig c1, PermissionPolicyConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getProperties(), c2.getProperties())
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation());
        }

        private static boolean isCompatible(Set<PermissionConfig> c1, Set<PermissionConfig> c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null || c1.size() != c2.size()) {
                return false;
            }

            List<PermissionConfig> configs1 = asList(c1.toArray());
            List<PermissionConfig> configs2 = asList(c2.toArray());

            for (PermissionConfig a : configs1) {
                if (!configs2.contains(a)) {
                    return false;
                }
            }

            return true;
        }
    }

    private static class ManagementCenterConfigChecker extends ConfigChecker<ManagementCenterConfig> {
        @Override
        boolean check(ManagementCenterConfig c1, ManagementCenterConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null
                    && (c1.isScriptingEnabled() == c2.isScriptingEnabled())
                    && (c1.isConsoleEnabled() == c2.isConsoleEnabled())
                    && (c1.isDataAccessEnabled() == c2.isDataAccessEnabled()))
                    && nullSafeEqual(c1.getTrustedInterfaces(), c2.getTrustedInterfaces());
        }
    }

    private static class HotRestartConfigChecker extends ConfigChecker<HotRestartPersistenceConfig> {
        @Override
        boolean check(HotRestartPersistenceConfig c1, HotRestartPersistenceConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getBaseDir(), c2.getBaseDir())
                    && nullSafeEqual(c1.getBackupDir(), c2.getBackupDir())
                    && nullSafeEqual(c1.getParallelism(), c2.getParallelism())
                    && nullSafeEqual(c1.getValidationTimeoutSeconds(), c2.getValidationTimeoutSeconds())
                    && nullSafeEqual(c1.getDataLoadTimeoutSeconds(), c2.getDataLoadTimeoutSeconds())
                    && nullSafeEqual(c1.getClusterDataRecoveryPolicy(), c2.getClusterDataRecoveryPolicy())
                    && nullSafeEqual(c1.getEncryptionAtRestConfig(), c2.getEncryptionAtRestConfig()));
        }
    }

    private static class PersistenceConfigChecker extends ConfigChecker<PersistenceConfig> {
        @Override
        boolean check(PersistenceConfig c1, PersistenceConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getBaseDir(), c2.getBaseDir())
                    && nullSafeEqual(c1.getBackupDir(), c2.getBackupDir())
                    && nullSafeEqual(c1.getParallelism(), c2.getParallelism())
                    && nullSafeEqual(c1.getValidationTimeoutSeconds(), c2.getValidationTimeoutSeconds())
                    && nullSafeEqual(c1.getDataLoadTimeoutSeconds(), c2.getDataLoadTimeoutSeconds())
                    && nullSafeEqual(c1.getRebalanceDelaySeconds(), c2.getRebalanceDelaySeconds())
                    && nullSafeEqual(c1.getClusterDataRecoveryPolicy(), c2.getClusterDataRecoveryPolicy())
                    && nullSafeEqual(c1.getEncryptionAtRestConfig(), c2.getEncryptionAtRestConfig()));
        }
    }

    private static class LocalDeviceConfigChecker extends ConfigChecker<LocalDeviceConfig> {

        @Override
        boolean check(LocalDeviceConfig t1, LocalDeviceConfig t2) {
            return Objects.equals(t1, t2);
        }
    }

    private static class CRDTReplicationConfigChecker extends ConfigChecker<CRDTReplicationConfig> {
        @Override
        boolean check(CRDTReplicationConfig c1, CRDTReplicationConfig c2) {
            return c1 == c2 || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getMaxConcurrentReplicationTargets(), c2.getMaxConcurrentReplicationTargets())
                    && nullSafeEqual(c1.getReplicationPeriodMillis(), c2.getReplicationPeriodMillis()));
        }
    }

    static class SplitBrainProtectionConfigChecker extends ConfigChecker<SplitBrainProtectionConfig> {
        @Override
        boolean check(SplitBrainProtectionConfig c1, SplitBrainProtectionConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }

            return ((c1.isEnabled() == c2.isEnabled())
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getProtectOn(), c2.getProtectOn())
                    && (c1.getMinimumClusterSize() == c2.getMinimumClusterSize())
                    && nullSafeEqual(c1.getFunctionClassName(), c2.getFunctionClassName())
                    && nullSafeEqual(c1.getFunctionImplementation(), c2.getFunctionImplementation())
                    && nullSafeEqual(c1.getListenerConfigs(), c2.getListenerConfigs()));
        }
    }

    static class RestApiConfigChecker extends ConfigChecker<RestApiConfig> {
        @Override
        boolean check(RestApiConfig c1, RestApiConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }
            return (c1.isEnabled() == c2.isEnabled())
                    && nullSafeEqual(c1.getEnabledGroups(), c2.getEnabledGroups());
        }
    }

    static class MemcacheProtocolConfigChecker extends ConfigChecker<MemcacheProtocolConfig> {
        @Override
        boolean check(MemcacheProtocolConfig c1, MemcacheProtocolConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }
            return (c1.isEnabled() == c2.isEnabled());
        }
    }

    private static boolean contains(Object[] values, Object toFind) {
        if (values == null || values.length == 0) {
            return false;
        }
        for (int i = 0; i < values.length; i++) {
            if (toFind == values[i] || (toFind != null && toFind.equals(values[i]))) {
                return true;
            }
        }
        return false;
    }
}
