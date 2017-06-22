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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.config.ConfigUtils;
import com.hazelcast.util.CollectionUtil;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.text.MessageFormat.format;

class ConfigCompatibilityChecker {

    /**
     * Checks if two {@link Config}'s are compatible. This mostly means that the config values will have the same
     * impact on the behaviour of the system but are not necessarily the same (e.g. null value is sometimes the same
     * as an empty collection or a disabled config).
     * NOTE: This method checks MOST but NOT ALL configuration. As such it is best used in test scenarios to cover
     * as much config checks as possible automatically.
     *
     * @param c1 the {@link Config} to check
     * @param c2 the {@link Config} to check
     * @return {@code true} if the configs are compatible
     * @throws HazelcastException       if configs are incompatible
     * @throws IllegalArgumentException if one of the configs is {@code null}
     */
    static boolean isCompatible(final Config c1, final Config c2) {
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
        checkCompatibleConfigs("partition group", c1.getPartitionGroupConfig(), c2.getPartitionGroupConfig(), new PartitionGroupConfigChecker());
        checkCompatibleConfigs("serialization", c1.getSerializationConfig(), c2.getSerializationConfig(), new SerializationConfigChecker());
        checkCompatibleConfigs("services", c1.getServicesConfig(), c2.getServicesConfig(), new ServicesConfigChecker());
        checkCompatibleConfigs("management center", c1.getManagementCenterConfig(), c2.getManagementCenterConfig(), new ManagementCenterConfigChecker());
        checkCompatibleConfigs("hot restart", c1.getHotRestartPersistenceConfig(), c2.getHotRestartPersistenceConfig(), new HotRestartConfigChecker());
        checkCompatibleConfigs("network", c1.getNetworkConfig(), c2.getNetworkConfig(), new NetworkConfigChecker());
        checkCompatibleConfigs("map", c1, c2, c1.getMapConfigs(), c2.getMapConfigs(), new MapConfigChecker());
        checkCompatibleConfigs("ringbuffer", c1, c2, c1.getRingbufferConfigs(), c2.getRingbufferConfigs(), new RingbufferConfigChecker());
        checkCompatibleConfigs("queue", c1, c2, c1.getQueueConfigs(), c2.getQueueConfigs(), new QueueConfigChecker());
        checkCompatibleConfigs("semaphore", c1, c2, getSemaphoreConfigsByName(c1), getSemaphoreConfigsByName(c2), new SemaphoreConfigChecker());
        checkCompatibleConfigs("lock", c1, c2, c1.getLockConfigs(), c2.getLockConfigs(), new LockConfigChecker());
        checkCompatibleConfigs("topic", c1, c2, c1.getTopicConfigs(), c2.getTopicConfigs(), new TopicConfigChecker());
        checkCompatibleConfigs("reliable topic", c1, c2, c1.getReliableTopicConfigs(), c2.getReliableTopicConfigs(), new ReliableTopicConfigChecker());
        checkCompatibleConfigs("cache", c1, c2, c1.getCacheConfigs(), c2.getCacheConfigs(), new CacheSimpleConfigChecker());
        checkCompatibleConfigs("executor", c1, c2, c1.getExecutorConfigs(), c2.getExecutorConfigs(), new ExecutorConfigChecker());
        checkCompatibleConfigs("durable executor", c1, c2, c1.getDurableExecutorConfigs(), c2.getDurableExecutorConfigs(), new DurableExecutorConfigChecker());
        checkCompatibleConfigs("scheduled executor", c1, c2, c1.getScheduledExecutorConfigs(), c2.getScheduledExecutorConfigs(), new ScheduledExecutorConfigChecker());
        checkCompatibleConfigs("map event journal", c1, c2, c1.getMapEventJournalConfigs(), c2.getMapEventJournalConfigs(), new MapEventJournalConfigChecker());
        checkCompatibleConfigs("cache event journal", c1, c2, c1.getCacheEventJournalConfigs(), c2.getCacheEventJournalConfigs(), new CacheEventJournalConfigChecker());
        checkCompatibleConfigs("multimap", c1, c2, c1.getMultiMapConfigs(), c2.getMultiMapConfigs(), new MultimapConfigChecker());
        checkCompatibleConfigs("list", c1, c2, c1.getListConfigs(), c2.getListConfigs(), new ListConfigChecker());
        checkCompatibleConfigs("set", c1, c2, c1.getSetConfigs(), c2.getSetConfigs(), new SetConfigChecker());
        checkCompatibleConfigs("job tracker", c1, c2, c1.getJobTrackerConfigs(), c2.getJobTrackerConfigs(), new JobTrackerConfigChecker());

        return true;
    }

    public static void checkWanConfigs(Map<String, WanReplicationConfig> c1, Map<String, WanReplicationConfig> c2) {
        if ((c1 != c2 && (c1 == null || c2 == null)) || c1.size() != c2.size()) {
            throw new HazelcastException(format("Incompatible wan replication config :\n{0}\n vs \n{1}", c1, c2));
        }
        final WanReplicationConfigChecker checker = new WanReplicationConfigChecker();
        for (Entry<String, WanReplicationConfig> entry : c1.entrySet()) {
            checkCompatibleConfigs("wan replication", entry.getValue(), c2.get(entry.getKey()), checker);
        }
    }

    private static Map<String, SemaphoreConfig> getSemaphoreConfigsByName(Config c) {
        final Collection<SemaphoreConfig> semaphoreConfigs = c.getSemaphoreConfigs();
        final HashMap<String, SemaphoreConfig> configsByName = new HashMap<String, SemaphoreConfig>(semaphoreConfigs.size());
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

    private static <T> void checkCompatibleConfigs(
            String type, Config c1, Config c2,
            Map<String, T> configs1, Map<String, T> configs2, ConfigChecker<T> checker) {

        final Set<String> configNames = new HashSet<String>(configs1.keySet());
        configNames.addAll(configs2.keySet());

        for (final String name : configNames) {
            final T config1 = ConfigUtils.lookupByPattern(c1.getConfigPatternMatcher(), configs1, name);
            final T config2 = ConfigUtils.lookupByPattern(c2.getConfigPatternMatcher(), configs2, name);
            if (config1 != null && config2 != null && !checker.check(config1, config2)) {
                throw new HazelcastException(format("Incompatible " + type + " config :\n{0}\n vs \n{1}",
                        config1, config2));
            }
        }
        final T config1 = checker.getDefault(c1);
        final T config2 = checker.getDefault(c2);
        if (!checker.check(config1, config2)) {
            throw new HazelcastException(format("Incompatible default " + type + " config :\n{0}\n vs \n{1}",
                    config1, config2));
        }
    }

    private abstract static class ConfigChecker<T> {
        abstract boolean check(T t1, T t2);

        T getDefault(Config c) {
            return null;
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

        final Iterator<T> i1 = c1.iterator();
        final Iterator<T> i2 = c2.iterator();
        while (i1.hasNext() && i2.hasNext()) {
            final T config1 = i1.next();
            final T config2 = i2.next();
            if (!checker.check(config1, config2)) {
                return false;
            }
        }
        return !(i1.hasNext() || i2.hasNext());
    }

    private static boolean isCompatible(HotRestartConfig c1, HotRestartConfig c2) {
        final boolean c1Disabled = c1 == null || !c1.isEnabled();
        final boolean c2Disabled = c2 == null || !c2.isEnabled();
        return c1 == c2 || (c1Disabled && c2Disabled) ||
                (c1 != null && c2 != null && nullSafeEqual(c1.isFsync(), c2.isFsync()));
    }

    // CONFIG CHECKERS
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
                    && isCompatible(c1.getRingbufferStoreConfig(), c2.getRingbufferStoreConfig());
        }

        private static boolean isCompatible(RingbufferStoreConfig c1, RingbufferStoreConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getClassName(), c2.getClassName())
                            && nullSafeEqual(c1.getFactoryClassName(), c2.getFactoryClassName())
                            && nullSafeEqual(c1.getProperties(), c2.getProperties()));
        }

        @Override
        RingbufferConfig getDefault(Config c) {
            return c.getRingbufferConfig("default");
        }
    }

    public static class EventJournalConfigChecker extends ConfigChecker<EventJournalConfig> {
        @Override
        boolean check(EventJournalConfig c1, EventJournalConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
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
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled())
                    && nullSafeEqual(c1.getQuorumName(), c2.getQuorumName());
        }

        private static boolean isCompatible(QueueStoreConfig c1, QueueStoreConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
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
                    && nullSafeEqual(c1.getInitialPermits(), c2.getInitialPermits());
        }

        @Override
        SemaphoreConfig getDefault(Config c) {
            return c.getSemaphoreConfig("default");
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

    private static boolean isCompatible(CollectionConfig c1, CollectionConfig c2) {
        return c1 == c2 || !(c1 == null || c2 == null)
                && nullSafeEqual(c1.getName(), c2.getName())
                && nullSafeEqual(c1.getItemListenerConfigs(), c2.getItemListenerConfigs())
                && nullSafeEqual(c1.getBackupCount(), c2.getBackupCount())
                && nullSafeEqual(c1.getAsyncBackupCount(), c2.getAsyncBackupCount())
                && nullSafeEqual(c1.getMaxSize(), c2.getMaxSize())
                && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled());
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
            final int cap1 = c1.getQueueCapacity();
            final int cap2 = c2.getQueueCapacity();
            return nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getPoolSize(), c2.getPoolSize())
                    && (nullSafeEqual(cap1, cap2) || (Math.min(cap1, cap2) == 0 && Math.max(cap1, cap2) == Integer.MAX_VALUE))
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
                    && nullSafeEqual(c1.isStatisticsEnabled(), c2.isStatisticsEnabled());
        }

        @Override
        MultiMapConfig getDefault(Config c) {
            return c.getMultiMapConfig("default");
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
            final int max1 = c1.getMaxThreadSize();
            final int max2 = c2.getMaxThreadSize();
            return nullSafeEqual(c1.getName(), c2.getName())
                    && (nullSafeEqual(max1, max2) || (Math.min(max1, max2) == 0 && Math.max(max1, max2) == Runtime.getRuntime().availableProcessors()))
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
                    && isCollectionCompatible(c1.getCacheEntryListeners(), c2.getCacheEntryListeners(), new CacheSimpleEntryListenerConfigChecker())
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
        boolean check(MapConfig c1, MapConfig c2) {
            if (c1 == c2) {
                return true;
            }
            if (c1 == null || c2 == null) {
                return false;
            }
            final int maxSize1 = c1.getMaxSizeConfig().getSize();
            final int maxSize2 = c2.getMaxSizeConfig().getSize();

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
                    && (nullSafeEqual(maxSize1, maxSize2) || (Math.min(maxSize1, maxSize2) == 0 && Math.max(maxSize1, maxSize2) == Integer.MAX_VALUE))
                    && nullSafeEqual(c1.getEvictionPercentage(), c2.getEvictionPercentage())
                    && nullSafeEqual(c1.getMinEvictionCheckMillis(), c2.getMinEvictionCheckMillis())
                    && nullSafeEqual(c1.getMergePolicy(), c2.getMergePolicy())
                    && nullSafeEqual(c1.isReadBackupData(), c2.isReadBackupData())
                    && ConfigCompatibilityChecker.isCompatible(c1.getHotRestartConfig(), c2.getHotRestartConfig())
                    && isCompatible(c1.getMapStoreConfig(), c2.getMapStoreConfig())
                    && isCompatible(c1.getNearCacheConfig(), c2.getNearCacheConfig())
                    && isCompatible(c1.getWanReplicationRef(), c2.getWanReplicationRef())
                    && isCollectionCompatible(c1.getMapIndexConfigs(), c2.getMapIndexConfigs(), new MapIndexConfigChecker())
                    && isCollectionCompatible(c1.getMapAttributeConfigs(), c2.getMapAttributeConfigs(), new MapAttributeConfigChecker())
                    && isCollectionCompatible(c1.getEntryListenerConfigs(), c2.getEntryListenerConfigs(), new EntryListenerConfigChecker())
                    && nullSafeEqual(c1.getPartitionLostListenerConfigs(), c2.getPartitionLostListenerConfigs())
                    && nullSafeEqual(c1.getPartitioningStrategyConfig(), c2.getPartitioningStrategyConfig());
        }

        private static boolean isCompatible(WanReplicationRef c1, WanReplicationRef c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getMergePolicy(), c2.getMergePolicy())
                    && nullSafeEqual(c1.getFilters(), c2.getFilters())
                    && nullSafeEqual(c1.isRepublishingEnabled(), c2.isRepublishingEnabled());
        }

        private static boolean isCompatible(NearCacheConfig c1, NearCacheConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && nullSafeEqual(c1.getTimeToLiveSeconds(), c2.getTimeToLiveSeconds())
                    && nullSafeEqual(c1.getMaxSize(), c2.getMaxSize())
                    && nullSafeEqual(c1.getEvictionPolicy(), c2.getEvictionPolicy())
                    && isCompatible(c1.getEvictionConfig(), c2.getEvictionConfig());
        }

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
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
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
                    && nullSafeEqual(new ArrayList<String>(c1.getInterfaces()),
                    new ArrayList<String>(c2.getInterfaces()));
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
            final String[] defaultValues = {"0", "*"};
            final boolean defaultDefinition1 = CollectionUtil.isEmpty(portDefinitions1) ||
                    (portDefinitions1.size() == 1 && ArrayUtils.contains(defaultValues, portDefinitions1.iterator().next()));
            final boolean defaultDefinition2 = CollectionUtil.isEmpty(portDefinitions2) ||
                    (portDefinitions2.size() == 1 && ArrayUtils.contains(defaultValues, portDefinitions2.iterator().next()));
            return (defaultDefinition1 && defaultDefinition2) || nullSafeEqual(portDefinitions1, portDefinitions2);
        }

        private static boolean isCompatible(JoinConfig c1, JoinConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && isCompatible(c1.getMulticastConfig(), c2.getMulticastConfig())
                    && isCompatible(c1.getTcpIpConfig(), c2.getTcpIpConfig())
                    && new AwsConfigChecker().check(c1.getAwsConfig(), c2.getAwsConfig())
                    && new DiscoveryConfigChecker().check(c1.getDiscoveryConfig(), c2.getDiscoveryConfig());
        }


        private static boolean isCompatible(TcpIpConfig c1, TcpIpConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getConnectionTimeoutSeconds(), c2.getConnectionTimeoutSeconds())
                            && nullSafeEqual(c1.getMembers(), c2.getMembers()))
                            && nullSafeEqual(c1.getRequiredMember(), c2.getRequiredMember());
        }

        private static boolean isCompatible(MulticastConfig c1, MulticastConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getMulticastGroup(), c2.getMulticastGroup())
                            && nullSafeEqual(c1.getMulticastPort(), c2.getMulticastPort()))
                            && nullSafeEqual(c1.getMulticastTimeoutSeconds(), c2.getMulticastTimeoutSeconds())
                            && nullSafeEqual(c1.getMulticastTimeToLive(), c2.getMulticastTimeToLive())
                            && nullSafeEqual(c1.getTrustedInterfaces(), c2.getTrustedInterfaces());
        }

        private static boolean isCompatible(InterfacesConfig c1, InterfacesConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(new ArrayList<String>(c1.getInterfaces()), new ArrayList<String>(c2.getInterfaces())));
        }

        private static boolean isCompatible(SymmetricEncryptionConfig c1, SymmetricEncryptionConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getSalt(), c2.getSalt())
                            && nullSafeEqual(c1.getPassword(), c2.getPassword()))
                            && nullSafeEqual(c1.getIterationCount(), c2.getIterationCount())
                            && nullSafeEqual(c1.getAlgorithm(), c2.getAlgorithm())
                            && nullSafeEqual(c1.getKey(), c2.getKey());
        }

        private static boolean isCompatible(SocketInterceptorConfig c1, SocketInterceptorConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getClassName(), c2.getClassName())
                            && nullSafeEqual(c1.getImplementation(), c2.getImplementation()))
                            && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }

        private static boolean isCompatible(SSLConfig c1, SSLConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getFactoryClassName(), c2.getFactoryClassName())
                            && nullSafeEqual(c1.getFactoryImplementation(), c2.getFactoryImplementation()))
                            && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }
    }


    private static class DiscoveryConfigChecker extends ConfigChecker<DiscoveryConfig> {
        @Override
        boolean check(DiscoveryConfig c1, DiscoveryConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getNodeFilterClass(), c2.getNodeFilterClass())
                            && nullSafeEqual(c1.getDiscoveryServiceProvider(), c2.getDiscoveryServiceProvider())
                            && isCollectionCompatible(c1.getDiscoveryStrategyConfigs(), c2.getDiscoveryStrategyConfigs(), new DiscoveryStrategyConfigChecker()));
        }
    }

    private static class AwsConfigChecker extends ConfigChecker<AwsConfig> {
        @Override
        boolean check(AwsConfig c1, AwsConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getAccessKey(), c2.getAccessKey())
                            && nullSafeEqual(c1.getSecretKey(), c2.getSecretKey())
                            && nullSafeEqual(c1.getRegion(), c2.getRegion())
                            && nullSafeEqual(c1.getSecurityGroupName(), c2.getSecurityGroupName())
                            && nullSafeEqual(c1.getTagKey(), c2.getTagKey())
                            && nullSafeEqual(c1.getTagValue(), c2.getTagValue())
                            && nullSafeEqual(c1.getHostHeader(), c2.getHostHeader())
                            && nullSafeEqual(c1.getIamRole(), c2.getIamRole())
                            && nullSafeEqual(c1.getConnectionTimeoutSeconds(), c2.getConnectionTimeoutSeconds()));
        }
    }

    private static class WanReplicationConfigChecker extends ConfigChecker<WanReplicationConfig> {
        @Override
        boolean check(WanReplicationConfig c1, WanReplicationConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getName(), c2.getName())
                    && isCompatible(c1.getWanConsumerConfig(), c2.getWanConsumerConfig())
                    && isCollectionCompatible(c1.getWanPublisherConfigs(), c2.getWanPublisherConfigs(), new WanPublisherConfigChecker());
        }

        private boolean isCompatible(WanConsumerConfig c1, WanConsumerConfig c2) {
            return c1 == c2 || !(c1 == null || c2 == null)
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation())
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
                    && new AwsConfigChecker().check(c1.getAwsConfig(), c2.getAwsConfig())
                    && new DiscoveryConfigChecker().check(c1.getDiscoveryConfig(), c2.getDiscoveryConfig())
                    && nullSafeEqual(c1.getClassName(), c2.getClassName())
                    && nullSafeEqual(c1.getImplementation(), c2.getImplementation())
                    && nullSafeEqual(c1.getProperties(), c2.getProperties());
        }
    }

    private static class PartitionGroupConfigChecker extends ConfigChecker<PartitionGroupConfig> {
        @Override
        boolean check(PartitionGroupConfig c1, PartitionGroupConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getGroupType(), c2.getGroupType())
                            && isCollectionCompatible(c1.getMemberGroupConfigs(), c2.getMemberGroupConfigs(), new MemberGroupConfigChecker()));
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
                    && nullSafeEqual(c1.isAllowUnsafe(), c2.isAllowUnsafe());
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

            final HashMap<String, ServiceConfig> config1 = new HashMap<String, ServiceConfig>();
            final HashMap<String, ServiceConfig> config2 = new HashMap<String, ServiceConfig>();

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
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getName(), c2.getName())
                            && nullSafeEqual(c1.getClassName(), c2.getClassName())
                            && nullSafeEqual(c1.getImplementation(), c2.getImplementation())
                            && nullSafeEqual(c1.getProperties(), c2.getProperties())
                            && nullSafeEqual(c1.getConfigObject(), c2.getConfigObject()));
        }
    }

    private static class ManagementCenterConfigChecker extends ConfigChecker<ManagementCenterConfig> {
        @Override
        boolean check(ManagementCenterConfig c1, ManagementCenterConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getUrl(), c2.getUrl())
                            && nullSafeEqual(c1.getUpdateInterval(), c2.getUpdateInterval()));
        }
    }

    private static class HotRestartConfigChecker extends ConfigChecker<HotRestartPersistenceConfig> {
        @Override
        boolean check(HotRestartPersistenceConfig c1, HotRestartPersistenceConfig c2) {
            final boolean c1Disabled = c1 == null || !c1.isEnabled();
            final boolean c2Disabled = c2 == null || !c2.isEnabled();
            return c1 == c2 || (c1Disabled && c2Disabled) ||
                    (c1 != null && c2 != null
                            && nullSafeEqual(c1.getBaseDir(), c2.getBaseDir())
                            && nullSafeEqual(c1.getBackupDir(), c2.getBackupDir())
                            && nullSafeEqual(c1.getParallelism(), c2.getParallelism())
                            && nullSafeEqual(c1.getValidationTimeoutSeconds(), c2.getValidationTimeoutSeconds())
                            && nullSafeEqual(c1.getDataLoadTimeoutSeconds(), c2.getDataLoadTimeoutSeconds())
                            && nullSafeEqual(c1.getClusterDataRecoveryPolicy(), c2.getClusterDataRecoveryPolicy()));
        }
    }
}
