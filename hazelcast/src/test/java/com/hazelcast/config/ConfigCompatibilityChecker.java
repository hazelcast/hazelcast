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

import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.util.CollectionUtil;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.config.AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom;
import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static java.text.MessageFormat.format;
import static java.util.Collections.singletonMap;
import static org.codehaus.groovy.runtime.InvokerHelper.asList;

class ConfigCompatibilityChecker {

    /**
     * Checks if two {@link Config} instances are compatible.
     * <p>
     * This mostly means that the config values will have the same impact on the behaviour of the system,
     * but are not necessarily the same (e.g. a {@code null} value is sometimes the same as an empty
     * collection or a disabled config).
     * <p>
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
        if (!nullSafeEqual(c1.getGroupConfig().getName(), c2.getGroupConfig().getName())) {
            return false;
        }
        if (!nullSafeEqual(c1.getGroupConfig().getPassword(), c2.getGroupConfig().getPassword())) {
            throw new HazelcastException("Incompatible group password");
        }

        checkWanConfigs(c1.getWanReplicationConfigs(), c2.getWanReplicationConfigs());
        checkCompatibleConfigs("partition group", c1.getPartitionGroupConfig(), c2.getPartitionGroupConfig(),
                new PartitionGroupConfigChecker());
        checkCompatibleConfigs("serialization", c1.getSerializationConfig(), c2.getSerializationConfig(),
                new SerializationConfigChecker());
        checkCompatibleConfigs("services", c1.getServicesConfig(), c2.getServicesConfig(), new ServicesConfigChecker());
        checkCompatibleConfigs("management center", c1.getManagementCenterConfig(), c2.getManagementCenterConfig(),
                new ManagementCenterConfigChecker());
        checkCompatibleConfigs("hot restart", c1.getHotRestartPersistenceConfig(), c2.getHotRestartPersistenceConfig(),
                new HotRestartConfigChecker());
        checkCompatibleConfigs("CRDT replication", c1.getCRDTReplicationConfig(), c2.getCRDTReplicationConfig(),
                new CRDTReplicationConfigChecker());
        checkCompatibleConfigs("network", c1.getNetworkConfig(), c2.getNetworkConfig(), new NetworkConfigChecker());
        checkCompatibleConfigs("map", c1, c2, c1.getMapConfigs(), c2.getMapConfigs(), new MapConfigChecker());
        checkCompatibleConfigs("ringbuffer", c1, c2, c1.getRingbufferConfigs(), c2.getRingbufferConfigs(),
                new RingbufferConfigChecker());
        checkCompatibleConfigs("atomic-long", c1, c2, c1.getAtomicLongConfigs(), c2.getAtomicLongConfigs(),
                new AtomicLongConfigChecker());
        checkCompatibleConfigs("atomic-reference", c1, c2, c1.getAtomicReferenceConfigs(), c2.getAtomicReferenceConfigs(),
                new AtomicReferenceConfigChecker());
        checkCompatibleConfigs("queue", c1, c2, c1.getQueueConfigs(), c2.getQueueConfigs(), new QueueConfigChecker());
        checkCompatibleConfigs("semaphore", c1, c2, getSemaphoreConfigsByName(c1), getSemaphoreConfigsByName(c2),
                new SemaphoreConfigChecker());
        checkCompatibleConfigs("lock", c1, c2, c1.getLockConfigs(), c2.getLockConfigs(), new LockConfigChecker());
        checkCompatibleConfigs("topic", c1, c2, c1.getTopicConfigs(), c2.getTopicConfigs(), new TopicConfigChecker());
        checkCompatibleConfigs("reliable topic", c1, c2, c1.getReliableTopicConfigs(), c2.getReliableTopicConfigs(),
                new ReliableTopicConfigChecker());
        checkCompatibleConfigs("cache", c1, c2, c1.getCacheConfigs(), c2.getCacheConfigs(), new CacheSimpleConfigChecker());
        checkCompatibleConfigs("executor", c1, c2, c1.getExecutorConfigs(), c2.getExecutorConfigs(), new ExecutorConfigChecker());
        checkCompatibleConfigs("durable executor", c1, c2, c1.getDurableExecutorConfigs(), c2.getDurableExecutorConfigs(),
                new DurableExecutorConfigChecker());
        checkCompatibleConfigs("scheduled executor", c1, c2, c1.getScheduledExecutorConfigs(), c2.getScheduledExecutorConfigs(),
                new ScheduledExecutorConfigChecker());
        checkCompatibleConfigs("map event journal", c1, c2, c1.getMapEventJournalConfigs(), c2.getMapEventJournalConfigs(),
                new MapEventJournalConfigChecker());
        checkCompatibleConfigs("cache event journal", c1, c2, c1.getCacheEventJournalConfigs(), c2.getCacheEventJournalConfigs(),
                new CacheEventJournalConfigChecker());
        checkCompatibleConfigs("map merkle tree", c1, c2, c1.getMapMerkleTreeConfigs(), c2.getMapMerkleTreeConfigs(),
                new MapMerkleTreeConfigChecker());
        checkCompatibleConfigs("multimap", c1, c2, c1.getMultiMapConfigs(), c2.getMultiMapConfigs(), new MultimapConfigChecker());
        checkCompatibleConfigs("replicated map", c1, c2, c1.getReplicatedMapConfigs(), c2.getReplicatedMapConfigs(),
                new ReplicatedMapConfigChecker());
        checkCompatibleConfigs("list", c1, c2, c1.getListConfigs(), c2.getListConfigs(), new ListConfigChecker());
        checkCompatibleConfigs("set", c1, c2, c1.getSetConfigs(), c2.getSetConfigs(), new SetConfigChecker());
        checkCompatibleConfigs("job tracker", c1, c2, c1.getJobTrackerConfigs(), c2.getJobTrackerConfigs(),
                new JobTrackerConfigChecker());
        checkCompatibleConfigs("flake id generator", c1, c2, c1.getFlakeIdGeneratorConfigs(), c2.getFlakeIdGeneratorConfigs(),
                new FlakeIdGeneratorConfigChecker());
        checkCompatibleConfigs("count down latch", c1, c2, c1.getCountDownLatchConfigs(), c2.getCountDownLatchConfigs(),
                new CountDownLatchConfigChecker());
        checkCompatibleConfigs("cardinality estimator", c1, c2, c1.getCardinalityEstimatorConfigs(),
                c2.getCardinalityEstimatorConfigs(), new CardinalityEstimatorConfigChecker());
        checkCompatibleConfigs("pn counter", c1, c2, c1.getPNCounterConfigs(), c2.getPNCounterConfigs(),
                new PNCounterConfigChecker());
        checkCompatibleConfigs("quorum", c1, c2, c1.getQuorumConfigs(), c2.getQuorumConfigs(), new QuorumConfigChecker());
        checkCompatibleConfigs("security", c1, c2, singletonMap("", c1.getSecurityConfig()),
                singletonMap("", c2.getSecurityConfig()), new SecurityConfigChecker());

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

    private static Map<String, SemaphoreConfig> getSemaphoreConfigsByName(Config c) {
        Collection<SemaphoreConfig> semaphoreConfigs = c.getSemaphoreConfigs();
        HashMap<String, SemaphoreConfig> configsByName = new HashMap<String, SemaphoreConfig>(semaphoreConfigs.size());
        for (SemaphoreConfig config : semaphoreConfigs) {
            configsByName.put(config.getName(), config);
        }
        return configsByName;
    }

    private static <T> void checkCompatibleConfigs(String type, T c1, T c2, ConfigChecker<T> checker) {
        if (!checker.check(c1, c2)) {
            throw new HazelcastException(format("Incompatible " + type + " config :\n{0}\n vs \n{1}", c1, c2));
        }
    }

    private static <T> void checkCompatibleConfigs(String type, Config c1, Config c2, Map<String, T> configs1,
                                                   Map<String, T> configs2, ConfigChecker<T> checker) {
        Set<String> configNames = new HashSet<String>(configs1.keySet());
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
        return (a == b) || (a != null && a.equals(b));
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

    private static boolean isCompatible(CollectionConfig c1, CollectionConfig c2) {
        return c1 == c2 || !(c1 == null || c2 == null)
                && nullSafeEqual(c1.getName(), c2.getName())
                && nullSafeEqual(c1.getItemListenerConfigs(), c2.getItemListenerConfigs())
                && nullSafeEqual(c1.getBackupCount(), c2.getBackupCount())
                && nullSafeEqual(c1.getAsyncBackupCount(), c2.getAsyncBackupCount())
                && nullSafeEqual(c1.getMaxSize(), c2.getMaxSize())
                && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
                && isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig());
    }

    private static boolean isCompatible(HotRestartConfig c1, HotRestartConfig c2) {
        boolean c1Disabled = c1 == null || !c1.isEnabled();
        boolean c2Disabled = c2 == null || !c2.isEnabled();
        return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null && nullSafeEqual(c1.isFsync(), c2.isFsync()));
    }

    private static boolean isCompatible(MergePolicyConfig c1, MergePolicyConfig c2) {
        return c1 == c2 || !(c1 == null || c2 == null)
                && c1.getBatchSize() == c2.getBatchSize()
                && nullSafeEqual(c1.getPolicy(), c2.getPolicy());
    }

    private abstract static class ConfigChecker<T> {

        abstract boolean check(T t1, T t2);

        T getDefault(Config c) {
            return null;
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
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
                    && isCompatible(c1.getRingbufferStoreConfig(), c2.getRingbufferStoreConfig())
                    && ConfigCompatibilityChecker.isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig());
        }

        private static boolean isCompatible(RingbufferStoreConfig c1, RingbufferStoreConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getFactoryClassName(), c2.getFactoryClassName())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }

        @Override
        RingbufferConfig getDefault(Config c) {
            return c.getRingbufferConfig("default");
        }
    }

    public static class MapMerkleTreeConfigChecker extends ConfigChecker<MerkleTreeConfig> {
        @Override
        boolean check(MerkleTreeConfig c1, MerkleTreeConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getMapName(), c2.getMapName())
                    && nullSafeEqual(c1.getDepth(), c2.getDepth()));
        }

        @Override
        MerkleTreeConfig getDefault(Config c) {
            return c.getMapMerkleTreeConfig("default");
        }
    }

    public static class EventJournalConfigChecker extends ConfigChecker<EventJournalConfig> {
        @Override
        boolean check(EventJournalConfig c1, EventJournalConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getMapName(), c2.getMapName())
                    && nullSafeEqual(c1.getCacheName(), c2.getCacheName())
                    && nullSafeEqual(c1.getCapacity(), c2.getCapacity())
                    && nullSafeEqual(c1.getTimeToLiveSeconds(), c2.getTimeToLiveSeconds()));
        }
    }

    public static class MapEventJournalConfigChecker extends EventJournalConfigChecker {
        @Override
        EventJournalConfig getDefault(Config c) {
            return c.getMapEventJournalConfig("default");
        }
    }

    public static class CacheEventJournalConfigChecker extends EventJournalConfigChecker {
        @Override
        EventJournalConfig getDefault(Config c) {
            return c.getCacheEventJournalConfig("default");
        }
    }

    private static class AtomicLongConfigChecker extends ConfigChecker<AtomicLongConfig> {
        @Override
        boolean check(AtomicLongConfig c1, AtomicLongConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
                    && ConfigCompatibilityChecker.isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig());
        }

        @Override
        AtomicLongConfig getDefault(Config c) {
            return c.getAtomicLongConfig("default");
        }
    }

    private static class AtomicReferenceConfigChecker extends ConfigChecker<AtomicReferenceConfig> {
        @Override
        boolean check(AtomicReferenceConfig c1, AtomicReferenceConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
                    && ConfigCompatibilityChecker.isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig());
        }

        @Override
        AtomicReferenceConfig getDefault(Config c) {
            return c.getAtomicReferenceConfig("default");
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
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName());
        }

        private static boolean isCompatible(QueueStoreConfig c1, QueueStoreConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getFactoryClassName(), c2.getFactoryClassName())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }

        @Override
        QueueConfig getDefault(Config c) {
            return c.getQueueConfig("default");
        }
    }

    private static class SemaphoreConfigChecker extends ConfigChecker<SemaphoreConfig> {
        @Override
        boolean check(SemaphoreConfig c1, SemaphoreConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getBackupCount(), c2.getBackupCount())
                    && nullSafeEqual(c1.getAsyncBackupCount(), c2.getAsyncBackupCount())
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
                    && nullSafeEqual(c1.getInitialPermits(), c2.getInitialPermits());
        }

        @Override
        SemaphoreConfig getDefault(Config c) {
            return c.getSemaphoreConfig("default");
        }
    }

    private static class CountDownLatchConfigChecker extends ConfigChecker<CountDownLatchConfig> {
        @Override
        boolean check(CountDownLatchConfig c1, CountDownLatchConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName());
        }

        @Override
        CountDownLatchConfig getDefault(Config c) {
            return c.getCountDownLatchConfig("default");
        }
    }

    private static class LockConfigChecker extends ConfigChecker<LockConfig> {
        @Override
        boolean check(LockConfig c1, LockConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName());
        }

        @Override
        LockConfig getDefault(Config c) {
            return c.getLockConfig("default");
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
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
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
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
                    && nullSafeEqual(c1.getCapacity(), c2.getCapacity());
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
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
                    && isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig())
                    && nullSafeEqual(c1.getPoolSize(), c2.getPoolSize());
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
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
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
                    && nullSafeEqual(c1.getConcurrencyLevel(), c2.getConcurrencyLevel())
                    && nullSafeEqual(c1.isAsyncFillup(), c2.isAsyncFillup())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
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
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation());
        }
    }

    private static class JobTrackerConfigChecker extends ConfigChecker<JobTrackerConfig> {
        @Override
        boolean check(JobTrackerConfig c1, JobTrackerConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }
            int max1 = c1.getMaxThreadSize();
            int max2 = c2.getMaxThreadSize();
            int availableProcessors = RuntimeAvailableProcessors.get();
            return nullSafeEqual(c1.getName(), c2.getName())
                    && (nullSafeEqual(max1, max2) || (Math.min(max1, max2) == 0 && Math.max(max1, max2) == availableProcessors))
                    && nullSafeEqual(c1.getRetryCount(), c2.getRetryCount())
                    && nullSafeEqual(c1.getChunkSize(), c2.getChunkSize())
                    && nullSafeEqual(c1.getQueueSize(), c2.getQueueSize())
                    && nullSafeEqual(c1.isCommunicateStats(), c2.isCommunicateStats())
                    && nullSafeEqual(c1.getTopologyChangedStrategy(), c2.getTopologyChangedStrategy());
        }

        @Override
        JobTrackerConfig getDefault(Config c) {
            return c.getJobTrackerConfig("default");
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
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName());
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
                    && c1.getIdOffset() == c2.getIdOffset()
                    && c1.getNodeIdOffset() == c2.getNodeIdOffset()
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
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName());
        }

        @Override
        PNCounterConfig getDefault(Config c) {
            return c.getPNCounterConfig("default");
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
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
                    && nullSafeEqual(c1.getPartitionLostListenerConfigs(), c2.getPartitionLostListenerConfigs())
                    && nullSafeEqual(c1.getMergePolicy(), c2.getMergePolicy())
                    && ConfigCompatibilityChecker.isCompatible(c1.getHotRestartConfig(), c2.getHotRestartConfig());
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
                    && nullSafeEqual(c1.getMaximumSizePolicy(), c2.getMaximumSizePolicy())
                    && nullSafeEqual(c1.getEvictionPolicy(), c2.getEvictionPolicy())
                    && nullSafeEqual(c1.getComparatorClassName(), c2.getComparatorClassName());
        }

        private static boolean isCompatible(WanReplicationRef c1, WanReplicationRef c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getMergePolicy(), c2.getMergePolicy())
                    && nullSafeEqual(c1.getFilters(), c2.getFilters())
                    && nullSafeEqual(c1.isRepublishingEnabled(), c2.isRepublishingEnabled());
        }

        @Override
        CacheSimpleConfig getDefault(Config c) {
            return c.getCacheConfig("default");
        }
    }

    private static class MapConfigChecker extends ConfigChecker<MapConfig> {
        @Override
        @SuppressWarnings("deprecation")
        boolean check(MapConfig c1, MapConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }
            int maxSize1 = c1.getMaxSizeConfig().getSize();
            int maxSize2 = c2.getMaxSizeConfig().getSize();

            return nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getInMemoryFormat(), c2.getInMemoryFormat())
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                    && nullSafeEqual(c1.isOptimizeQueries(), c2.isOptimizeQueries())
                    && nullSafeEqual(c1.getCacheDeserializedValues(), c2.getCacheDeserializedValues())
                    && nullSafeEqual(c1.getBackupCount(), c2.getBackupCount())
                    && nullSafeEqual(c1.getAsyncBackupCount(), c2.getAsyncBackupCount())
                    && nullSafeEqual(c1.getTimeToLiveSeconds(), c2.getTimeToLiveSeconds())
                    && nullSafeEqual(c1.getMaxIdleSeconds(), c2.getMaxIdleSeconds())
                    && nullSafeEqual(c1.getEvictionPolicy(), c2.getEvictionPolicy())
                    && (nullSafeEqual(maxSize1, maxSize2)
                    || (Math.min(maxSize1, maxSize2) == 0 && Math.max(maxSize1, maxSize2) == Integer.MAX_VALUE))
                    && nullSafeEqual(c1.getEvictionPercentage(), c2.getEvictionPercentage())
                    && nullSafeEqual(c1.getMinEvictionCheckMillis(), c2.getMinEvictionCheckMillis())
                    && ConfigCompatibilityChecker.isCompatible(c1.getMergePolicyConfig(), c2.getMergePolicyConfig())
                    && nullSafeEqual(c1.isReadBackupData(), c2.isReadBackupData())
                    && ConfigCompatibilityChecker.isCompatible(c1.getHotRestartConfig(), c2.getHotRestartConfig())
                    && isCompatible(c1.getMapStoreConfig(), c2.getMapStoreConfig())
                    && isCompatible(c1.getNearCacheConfig(), c2.getNearCacheConfig())
                    && isCompatible(c1.getWanReplicationRef(), c2.getWanReplicationRef())
                    && isCollectionCompatible(c1.getMapIndexConfigs(), c2.getMapIndexConfigs(), new MapIndexConfigChecker())
                    && isCollectionCompatible(c1.getMapAttributeConfigs(), c2.getMapAttributeConfigs(),
                    new MapAttributeConfigChecker())
                    && isCollectionCompatible(c1.getEntryListenerConfigs(), c2.getEntryListenerConfigs(),
                    new EntryListenerConfigChecker())
                    && nullSafeEqual(c1.getPartitionLostListenerConfigs(), c2.getPartitionLostListenerConfigs())
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName())
                    && nullSafeEqual(c1.getPartitioningStrategyConfig(), c2.getPartitioningStrategyConfig());
        }

        private static boolean isCompatible(WanReplicationRef c1, WanReplicationRef c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getMergePolicy(), c2.getMergePolicy())
                    && nullSafeEqual(c1.getFilters(), c2.getFilters())
                    && nullSafeEqual(c1.isRepublishingEnabled(), c2.isRepublishingEnabled());
        }

        @SuppressWarnings("deprecation")
        private static boolean isCompatible(NearCacheConfig c1, NearCacheConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getTimeToLiveSeconds(), c2.getTimeToLiveSeconds())
                    && nullSafeEqual(c1.getMaxSize(), c2.getMaxSize())
                    && nullSafeEqual(c1.getEvictionPolicy(), c2.getEvictionPolicy())
                    && isCompatible(c1.getEvictionConfig(), c2.getEvictionConfig());
        }

        @SuppressWarnings("deprecation")
        private static boolean isCompatible(EvictionConfig c1, EvictionConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getSize(), c2.getSize())
                    && nullSafeEqual(c1.getMaximumSizePolicy(), c2.getMaximumSizePolicy())
                    && nullSafeEqual(c1.getEvictionPolicy(), c2.getEvictionPolicy())
                    && nullSafeEqual(c1.getEvictionPolicyType(), c2.getEvictionPolicyType())
                    && nullSafeEqual(c1.getEvictionStrategyType(), c2.getEvictionStrategyType())
                    && nullSafeEqual(c1.getComparatorClassName(), c2.getComparatorClassName());
        }

        private static boolean isCompatible(MapStoreConfig c1, MapStoreConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getFactoryClassName(), c2.getFactoryClassName())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }

        @Override
        MapConfig getDefault(Config c) {
            return c.getMapConfig("default");
        }
    }

    private static class MapIndexConfigChecker extends ConfigChecker<MapIndexConfig> {
        @Override
        boolean check(MapIndexConfig c1, MapIndexConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getAttribute(), c2.getAttribute())
                    && nullSafeEqual(c1.isOrdered(), c2.isOrdered());
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
                    && nullSafeEqual(c1.getClassName(), c2.getClassName());
        }
    }

    private static class MapAttributeConfigChecker extends ConfigChecker<MapAttributeConfig> {
        @Override
        boolean check(MapAttributeConfig c1, MapAttributeConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getExtractor(), c2.getExtractor());
        }
    }

    private static class DiscoveryStrategyConfigChecker extends ConfigChecker<DiscoveryStrategyConfig> {
        @Override
        boolean check(DiscoveryStrategyConfig c1, DiscoveryStrategyConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }
    }

    private static class MemberGroupConfigChecker extends ConfigChecker<MemberGroupConfig> {
        @Override
        boolean check(MemberGroupConfig c1, MemberGroupConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(new ArrayList<String>(c1.getInterfaces()), new ArrayList<String>(c2.getInterfaces()));
        }
    }

    private static class SerializerConfigChecker extends ConfigChecker<SerializerConfig> {
        @Override
        boolean check(SerializerConfig c1, SerializerConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getTypeClass(), c2.getTypeClass())
                    && nullSafeEqual(c1.getTypeClassName(), c2.getTypeClassName());
        }
    }

    private static class WanSyncConfigChecker extends ConfigChecker<WanSyncConfig> {
        @Override
        boolean check(WanSyncConfig c1, WanSyncConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getConsistencyCheckStrategy(), c2.getConsistencyCheckStrategy());
        }
    }

    private static class NetworkConfigChecker extends ConfigChecker<NetworkConfig> {
        @Override
        boolean check(NetworkConfig c1, NetworkConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getPort(), c2.getPort())
                    && nullSafeEqual(c1.getPortCount(), c2.getPortCount())
                    && nullSafeEqual(c1.isPortAutoIncrement(), c2.isPortAutoIncrement())
                    && nullSafeEqual(c1.isReuseAddress(), c2.isReuseAddress())
                    && nullSafeEqual(c1.getPublicAddress(), c2.getPublicAddress())
                    && isCompatible(c1.getOutboundPortDefinitions(), c2.getOutboundPortDefinitions())
                    && nullSafeEqual(c1.getOutboundPorts(), c2.getOutboundPorts())
                    && isCompatible(c1.getInterfaces(), c2.getInterfaces())
                    && isCompatible(c1.getJoin(), c2.getJoin())
                    && isCompatible(c1.getSymmetricEncryptionConfig(), c2.getSymmetricEncryptionConfig())
                    && isCompatible(c1.getSocketInterceptorConfig(), c2.getSocketInterceptorConfig())
                    && isCompatible(c1.getSSLConfig(), c2.getSSLConfig());
        }

        private static boolean isCompatible(Collection<String> portDefinitions1, Collection<String> portDefinitions2) {
            String[] defaultValues = {"0", "*"};
            boolean defaultDefinition1 = CollectionUtil.isEmpty(portDefinitions1)
                    || (portDefinitions1.size() == 1 && ArrayUtils.contains(defaultValues, portDefinitions1.iterator().next()));
            boolean defaultDefinition2 = CollectionUtil.isEmpty(portDefinitions2)
                    || (portDefinitions2.size() == 1 && ArrayUtils.contains(defaultValues, portDefinitions2.iterator().next()));
            return (defaultDefinition1 && defaultDefinition2) || nullSafeEqual(portDefinitions1, portDefinitions2);
        }

        private static boolean isCompatible(JoinConfig c1, JoinConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && isCompatible(c1.getMulticastConfig(), c2.getMulticastConfig())
                    && isCompatible(c1.getTcpIpConfig(), c2.getTcpIpConfig())
                    && new AliasedDiscoveryConfigsChecker().check(AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(c1), AliasedDiscoveryConfigUtils
                    .aliasedDiscoveryConfigsFrom(c2))
                    && new DiscoveryConfigChecker().check(c1.getDiscoveryConfig(), c2.getDiscoveryConfig());
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

        private static boolean isCompatible(InterfacesConfig c1, InterfacesConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(new ArrayList<String>(c1.getInterfaces()), new ArrayList<String>(c2.getInterfaces())));
        }

        private static boolean isCompatible(SymmetricEncryptionConfig c1, SymmetricEncryptionConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getSalt(), c2.getSalt())
                    && nullSafeEqual(c1.getPassword(), c2.getPassword()))
                    && nullSafeEqual(c1.getIterationCount(), c2.getIterationCount())
                    && nullSafeEqual(c1.getAlgorithm(), c2.getAlgorithm())
                    && nullSafeEqual(c1.getKey(), c2.getKey());
        }

        private static boolean isCompatible(SocketInterceptorConfig c1, SocketInterceptorConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation()))
                    && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }

        private static boolean isCompatible(SSLConfig c1, SSLConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getFactoryClassName(), c2.getFactoryClassName())
                    && nullSafeEqual(c1.getFactoryImplementation(), c2.getFactoryImplementation()))
                    && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }
    }

    private static class DiscoveryConfigChecker extends ConfigChecker<DiscoveryConfig> {
        @Override
        boolean check(DiscoveryConfig c1, DiscoveryConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getNodeFilterClass(), c2.getNodeFilterClass())
                    && nullSafeEqual(c1.getDiscoveryServiceProvider(), c2.getDiscoveryServiceProvider())
                    && isCollectionCompatible(c1.getDiscoveryStrategyConfigs(), c2.getDiscoveryStrategyConfigs(),
                    new DiscoveryStrategyConfigChecker()));
        }
    }

    private static class AliasedDiscoveryConfigsChecker extends ConfigChecker<List<AliasedDiscoveryConfig<?>>> {

        @Override
        boolean check(List<AliasedDiscoveryConfig<?>> t1, List<AliasedDiscoveryConfig<?>> t2) {
            Map<Class, AliasedDiscoveryConfig> m1 = mapByClass(t1);
            Map<Class, AliasedDiscoveryConfig> m2 = mapByClass(t2);

            if (m1.size() != m2.size()) {
                return false;
            }

            for (Class clazz : m1.keySet()) {
                AliasedDiscoveryConfig c1 = m1.get(clazz);
                AliasedDiscoveryConfig c2 = m2.get(clazz);
                if (!check(c1, c2)) {
                    return false;
                }
            }

            return true;
        }

        private static Map<Class, AliasedDiscoveryConfig> mapByClass(List<AliasedDiscoveryConfig<?>> configs) {
            Map<Class, AliasedDiscoveryConfig> result = new HashMap<Class, AliasedDiscoveryConfig>();
            for (AliasedDiscoveryConfig c : configs) {
                if (c.isEnabled()) {
                    result.put(c.getClass(), c);
                }
            }
            return result;
        }

        private static boolean check(AliasedDiscoveryConfig c1, AliasedDiscoveryConfig c2) {
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    // TODO
//                    && nullSafeEqual(c1.getEnvironment(), c2.getEnvironment())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }
    }

    private static class WanReplicationConfigChecker extends ConfigChecker<WanReplicationConfig> {
        @Override
        boolean check(WanReplicationConfig c1, WanReplicationConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && isCompatible(c1.getWanConsumerConfig(), c2.getWanConsumerConfig())
                    && isCollectionCompatible(c1.getWanPublisherConfigs(), c2.getWanPublisherConfigs(),
                    new WanPublisherConfigChecker());
        }

        private boolean isCompatible(WanConsumerConfig c1, WanConsumerConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation())
                    && nullSafeEqual(c1.isPersistWanReplicatedData(), c2.isPersistWanReplicatedData())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }
    }

    private static class WanPublisherConfigChecker extends ConfigChecker<WanPublisherConfig> {
        @Override
        boolean check(WanPublisherConfig c1, WanPublisherConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getGroupName(), c2.getGroupName())
                    && nullSafeEqual(c1.getQueueCapacity(), c2.getQueueCapacity())
                    && nullSafeEqual(c1.getQueueFullBehavior(), c2.getQueueFullBehavior())
                    && nullSafeEqual(c1.getInitialPublisherState(), c2.getInitialPublisherState())
                    && new AliasedDiscoveryConfigsChecker().check(aliasedDiscoveryConfigsFrom(c1), aliasedDiscoveryConfigsFrom(c2))
                    && new DiscoveryConfigChecker().check(c1.getDiscoveryConfig(), c2.getDiscoveryConfig())
                    && new WanSyncConfigChecker().check(c1.getWanSyncConfig(), c2.getWanSyncConfig())
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
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
                    && nullSafeEqual(c1.getJavaSerializationFilterConfig(), c2.getJavaSerializationFilterConfig());
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

            HashMap<String, ServiceConfig> config1 = new HashMap<String, ServiceConfig>();
            HashMap<String, ServiceConfig> config2 = new HashMap<String, ServiceConfig>();

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
                    && nullSafeEqual(c1.getClientBlockUnmappedActions(), c2.getClientBlockUnmappedActions())
                    && isCompatible(c1.getMemberCredentialsConfig(), c2.getMemberCredentialsConfig())
                    && isCompatible(c1.getSecurityInterceptorConfigs(), c2.getSecurityInterceptorConfigs())
                    && isCompatible(c1.getClientPolicyConfig(), c2.getClientPolicyConfig())
                    && isCompatible(c1.getClientPermissionConfigs(), c2.getClientPermissionConfigs())
                    && isCompatibleLoginModule(c1.getMemberLoginModuleConfigs(), c2.getMemberLoginModuleConfigs())
                    && isCompatibleLoginModule(c1.getClientLoginModuleConfigs(), c2.getClientLoginModuleConfigs());
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

            Map<String, LoginModuleConfig> config1 = new HashMap<String, LoginModuleConfig>();
            Map<String, LoginModuleConfig> config2 = new HashMap<String, LoginModuleConfig>();

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

            Map<String, SecurityInterceptorConfig> config1 = new HashMap<String, SecurityInterceptorConfig>();
            Map<String, SecurityInterceptorConfig> config2 = new HashMap<String, SecurityInterceptorConfig>();

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
            boolean c1Disabled = c1 == null || !c1.isEnabled();
            boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) || (c1 != null && c2 != null
                    && nullSafeEqual(c1.getUrl(), c2.getUrl())
                    && nullSafeEqual(c1.getUpdateInterval(), c2.getUpdateInterval()));
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
                    && nullSafeEqual(c1.getClusterDataRecoveryPolicy(), c2.getClusterDataRecoveryPolicy()));
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

    static class QuorumConfigChecker extends ConfigChecker<QuorumConfig> {
        @Override
        boolean check(QuorumConfig c1, QuorumConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }

            return ((c1.isEnabled() == c2.isEnabled())
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getType(), c2.getType())
                    && (c1.getSize() == c2.getSize())
                    && nullSafeEqual(c1.getQuorumFunctionClassName(), c2.getQuorumFunctionClassName())
                    && nullSafeEqual(c1.getQuorumFunctionImplementation(), c2.getQuorumFunctionImplementation())
                    && nullSafeEqual(c1.getListenerConfigs(), c2.getListenerConfigs()));
        }
    }
}
