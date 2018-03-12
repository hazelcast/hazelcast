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

package com.hazelcast.internal.config;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.AbstractBasicConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.HyperLogLogMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.version.Version;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_EVICTION_PERCENTAGE;
import static com.hazelcast.config.MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Validates a Hazelcast configuration in a specific context like OS vs. EE or client vs. member nodes.
 */
public final class ConfigValidator {

    private static final ILogger LOGGER = Logger.getLogger(ConfigValidator.class);

    private static final EnumSet<EvictionConfig.MaxSizePolicy> SUPPORTED_ON_HEAP_NEAR_CACHE_MAXSIZE_POLICIES
            = EnumSet.of(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);

    private static final EnumSet<EvictionPolicy> SUPPORTED_EVICTION_POLICIES = EnumSet.of(LRU, LFU);

    private static final List<String> STATISTIC_MERGE_POLICIES = new ArrayList<String>(asList(
            HigherHitsMergePolicy.class.getName(),
            HigherHitsMergePolicy.class.getSimpleName(),
            LatestAccessMergePolicy.class.getName(),
            LatestAccessMergePolicy.class.getSimpleName(),
            LatestUpdateMergePolicy.class.getName(),
            LatestUpdateMergePolicy.class.getSimpleName()
    ));

    private static final List<String> CARDINALITY_MERGE_POLICY = new ArrayList<String>(asList(
            HyperLogLogMergePolicy.class.getName(),
            HyperLogLogMergePolicy.class.getSimpleName()
    ));

    private ConfigValidator() {
    }

    /**
     * Checks preconditions to create a map proxy.
     *
     * @param mapConfig the {@link MapConfig}
     */
    public static void checkMapConfig(MapConfig mapConfig) {
        checkMergePolicy(mapConfig.isStatisticsEnabled(), mapConfig.getMergePolicyConfig().getPolicy());
        checkNotNative(mapConfig.getInMemoryFormat());

        logIgnoredConfig(mapConfig);
    }

    /**
     * Checks preconditions to create a map proxy with Near Cache.
     *
     * @param mapName         name of the map that Near Cache will be created for
     * @param nearCacheConfig the {@link NearCacheConfig} to be checked
     * @param isClient        {@code true} if the config is for a Hazelcast client, {@code false} otherwise
     */
    public static void checkNearCacheConfig(String mapName, NearCacheConfig nearCacheConfig,
                                            NativeMemoryConfig nativeMemoryConfig, boolean isClient) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();

        checkNotNative(inMemoryFormat);
        checkLocalUpdatePolicy(mapName, nearCacheConfig.getLocalUpdatePolicy());
        checkEvictionConfig(nearCacheConfig.getEvictionConfig(), true);
        checkOnHeapNearCacheMaxSizePolicy(nearCacheConfig);
        checkNearCacheNativeMemoryConfig(nearCacheConfig.getInMemoryFormat(), nativeMemoryConfig, getBuildInfo().isEnterprise());

        if (isClient && nearCacheConfig.isCacheLocalEntries()) {
            throw new IllegalArgumentException("The Near Cache option `cache-local-entries` is not supported in "
                    + "client configurations.");
        }
        checkPreloaderConfig(nearCacheConfig, isClient);
    }

    /**
     * Checks precondition to use {@link InMemoryFormat#NATIVE}
     *
     * @param nativeMemoryConfig native memory configuration
     */
    // not private for testing
    static void checkNearCacheNativeMemoryConfig(InMemoryFormat inMemoryFormat, NativeMemoryConfig nativeMemoryConfig,
                                                 boolean isEnterprise) {
        if (!isEnterprise) {
            return;
        }

        if (inMemoryFormat != NATIVE) {
            return;
        }

        if (nativeMemoryConfig != null && nativeMemoryConfig.isEnabled()) {
            return;
        }

        throw new IllegalArgumentException("Enable native memory config to use NATIVE in-memory-format for Near Cache");
    }

    private static void checkOnHeapNearCacheMaxSizePolicy(NearCacheConfig nearCacheConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat == NATIVE) {
            return;
        }

        EvictionConfig.MaxSizePolicy maxSizePolicy = nearCacheConfig.getEvictionConfig().getMaximumSizePolicy();
        if (!SUPPORTED_ON_HEAP_NEAR_CACHE_MAXSIZE_POLICIES.contains(maxSizePolicy)) {
            throw new IllegalArgumentException(format("Near Cache maximum size policy %s cannot be used with %s storage."
                            + " Supported maximum size policies are: %s",
                    maxSizePolicy, inMemoryFormat, SUPPORTED_ON_HEAP_NEAR_CACHE_MAXSIZE_POLICIES));
        }
    }

    /**
     * Checks IMaps' supported Near Cache local update policy configuration.
     *
     * @param mapName           name of the map that Near Cache will be created for
     * @param localUpdatePolicy local update policy
     */
    public static void checkLocalUpdatePolicy(String mapName, NearCacheConfig.LocalUpdatePolicy localUpdatePolicy) {
        if (localUpdatePolicy != INVALIDATE) {
            throw new IllegalArgumentException(format("Wrong `local-update-policy` option is selected for `%s` map Near Cache."
                    + " Only `%s` option is supported but found `%s`", mapName, INVALIDATE, localUpdatePolicy));
        }
    }

    /**
     * Checks if a {@link EvictionConfig} is valid in its context.
     *
     * @param evictionConfig the {@link EvictionConfig}
     * @param isNearCache    {@code true} if the config is for a Near Cache, {@code false} otherwise
     */
    @SuppressWarnings("ConstantConditions")
    public static void checkEvictionConfig(EvictionConfig evictionConfig, boolean isNearCache) {
        if (evictionConfig == null) {
            throw new IllegalArgumentException("Eviction config cannot be null!");
        }
        EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();
        String comparatorClassName = evictionConfig.getComparatorClassName();
        EvictionPolicyComparator comparator = evictionConfig.getComparator();

        checkEvictionConfig(evictionPolicy, comparatorClassName, comparator, isNearCache);
    }

    /**
     * Checks if parameters for an {@link EvictionConfig} are valid in their context.
     *
     * @param evictionPolicy      the {@link EvictionPolicy} for the {@link EvictionConfig}
     * @param comparatorClassName the comparator class name for the {@link EvictionConfig}
     * @param comparator          the comparator implementation for the {@link EvictionConfig}
     * @param isNearCache         {@code true} if the config is for a Near Cache, {@code false} otherwise
     */
    public static void checkEvictionConfig(EvictionPolicy evictionPolicy,
                                           String comparatorClassName,
                                           Object comparator,
                                           boolean isNearCache) {
        if (comparatorClassName != null && comparator != null) {
            throw new IllegalArgumentException("Only one of the `comparator class name` and `comparator`"
                    + " can be configured in the eviction configuration!");
        }

        if (!isNearCache && !SUPPORTED_EVICTION_POLICIES.contains(evictionPolicy)) {
            if (isNullOrEmpty(comparatorClassName) && comparator == null) {

                String msg = format("Eviction policy `%s` is not supported. Either you can provide a custom one or "
                        + "can use one of the supported: %s.", evictionPolicy, SUPPORTED_EVICTION_POLICIES);

                throw new IllegalArgumentException(msg);
            }
        } else {
            if (evictionPolicy != EvictionConfig.DEFAULT_EVICTION_POLICY) {
                if (!isNullOrEmpty(comparatorClassName)) {
                    throw new IllegalArgumentException(
                            "Only one of the `eviction policy` and `comparator class name` can be configured!");
                }
                if (comparator != null) {
                    throw new IllegalArgumentException(
                            "Only one of the `eviction policy` and `comparator` can be configured!");
                }
            }
        }
    }

    /**
     * Validates the given {@link CacheSimpleConfig}.
     *
     * @param cacheSimpleConfig the {@link CacheSimpleConfig} to check
     */
    public static void checkCacheConfig(CacheSimpleConfig cacheSimpleConfig) {
        checkCacheConfig(cacheSimpleConfig.getInMemoryFormat(), cacheSimpleConfig.getEvictionConfig(),
                cacheSimpleConfig.isStatisticsEnabled(), cacheSimpleConfig.getMergePolicy());
    }

    /**
     * Validates the given parameters in the context of a {@link ICache} config.
     *
     * @param inMemoryFormat      the in-memory format the {@code Cache} is configured with
     * @param evictionConfig      eviction configuration of {@code Cache}
     * @param isStatisticsEnabled {@code true} if statistics are enabled, {@code false} otherwise
     * @param mergePolicy         the configured merge policy
     */
    public static void checkCacheConfig(InMemoryFormat inMemoryFormat,
                                        EvictionConfig evictionConfig,
                                        boolean isStatisticsEnabled,
                                        String mergePolicy) {
        checkMergePolicy(isStatisticsEnabled, mergePolicy);
        checkNotNative(inMemoryFormat);

        if (inMemoryFormat == NATIVE) {
            EvictionConfig.MaxSizePolicy maxSizePolicy = evictionConfig.getMaximumSizePolicy();
            if (maxSizePolicy == EvictionConfig.MaxSizePolicy.ENTRY_COUNT) {
                throw new IllegalArgumentException("Invalid max-size policy "
                        + '(' + maxSizePolicy + ") for NATIVE in-memory format! Only "
                        + EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE + ", "
                        + EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + ", "
                        + EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE + ", "
                        + EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE
                        + " are supported.");
            }
        }
    }

    /**
     * Returns {@code true} if supplied {@code inMemoryFormat} can be merged
     * with the supplied {@code mergePolicy}, otherwise returns {@code false}.
     * <p>
     * When there is a wrong policy detected, it does one of two things: if
     * fail-fast option is {@code true} and cluster version is 3.10 or later,
     * it throws {@link IllegalArgumentException} otherwise prints a warning
     * log.
     */
    public static boolean checkMergePolicySupportsInMemoryFormat(String name,
                                                                 String mergePolicy,
                                                                 InMemoryFormat inMemoryFormat,
                                                                 Version clusterVersion,
                                                                 boolean failFast,
                                                                 ILogger logger) {

        if (canMergeWithPolicy(mergePolicy, inMemoryFormat, clusterVersion)) {
            return true;
        }

        if (clusterVersion.isGreaterOrEqual(V3_10) && failFast) {
            throw new IllegalArgumentException(createSplitRecoveryWarningMsg(name, mergePolicy));
        } else {
            logger.warning(createSplitRecoveryWarningMsg(name, mergePolicy));
        }

        return false;
    }

    private static boolean canMergeWithPolicy(String mergePolicyClassName,
                                              InMemoryFormat inMemoryFormat,
                                              Version clusterVersion) {
        if (inMemoryFormat != NATIVE) {
            return true;
        }

        try {
            return clusterVersion.isGreaterOrEqual(V3_10)
                    && SplitBrainMergePolicy.class.isAssignableFrom(Class.forName(mergePolicyClassName));
        } catch (ClassNotFoundException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static String createSplitRecoveryWarningMsg(String name, String mergePolicy) {
        String messageTemplate = "Split brain recovery is not supported for '%s',"
                + " because it's using merge policy `%s` to merge `%s` data."
                + " To fix this, use an implementation of `%s` with a cluster version `%s` or later";

        return format(messageTemplate, name, mergePolicy, NATIVE,
                SplitBrainMergePolicy.class.getName(), V3_10);
    }

    /**
     * Validates the given {@link ReplicatedMapConfig}.
     *
     * @param replicatedMapConfig the {@link ReplicatedMapConfig} to check
     */
    public static void checkReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        checkMergePolicy(replicatedMapConfig.isStatisticsEnabled(), replicatedMapConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link MultiMapConfig}.
     *
     * @param multiMapConfig the {@link MultiMapConfig} to check
     */
    public static void checkMultiMapConfig(MultiMapConfig multiMapConfig) {
        checkMergePolicy(multiMapConfig.isStatisticsEnabled(), multiMapConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link QueueConfig}.
     *
     * @param queueConfig the {@link QueueConfig} to check
     */
    public static void checkQueueConfig(QueueConfig queueConfig) {
        // the queue statistics do not provide the necessary data for the merge policies
        checkMergePolicy(false, queueConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link CollectionConfig}.
     *
     * @param collectionConfig the {@link CollectionConfig} to check
     */
    public static void checkCollectionConfig(CollectionConfig collectionConfig) {
        // the collection statistics do not provide the necessary data for the merge policies
        checkMergePolicy(false, collectionConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link RingbufferConfig}.
     *
     * @param ringbufferConfig the {@link RingbufferConfig} to check
     */
    public static void checkRingbufferConfig(RingbufferConfig ringbufferConfig) {
        checkMergePolicy(false, ringbufferConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link AbstractBasicConfig}.
     *
     * @param basicConfig the {@link AbstractBasicConfig} to check
     */
    @SuppressWarnings("checkstyle:illegaltype")
    public static void checkBasicConfig(AbstractBasicConfig basicConfig) {
        checkMergePolicy(false, basicConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link ScheduledExecutorConfig}.
     *
     * @param scheduledExecutorConfig the {@link ScheduledExecutorConfig} to check
     */
    public static void checkScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig) {
        checkMergePolicy(false, scheduledExecutorConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Throws {@link IllegalArgumentException} if the supplied merge policy needs statistics to be enabled.
     *
     * @param isStatisticsEnabled {@code true} if statistics are enabled, {@code false} otherwise
     * @param mergePolicy         the configured merge policy
     */
    public static void checkMergePolicy(boolean isStatisticsEnabled, String mergePolicy) {
        if (!isStatisticsEnabled && STATISTIC_MERGE_POLICIES.contains(mergePolicy)) {
            throw new IllegalArgumentException("You need to enable statistics to use merge policy " + mergePolicy);
        }
        if (CARDINALITY_MERGE_POLICY.contains(mergePolicy)) {
            throw new IllegalArgumentException("The " + mergePolicy + " is only available for the CardinalityEstimator");
        }
    }

    /**
     * Throws {@link IllegalArgumentException} if the supplied {@link InMemoryFormat} is {@link InMemoryFormat#NATIVE}.
     *
     * @param inMemoryFormat supplied inMemoryFormat
     */
    private static void checkNotNative(InMemoryFormat inMemoryFormat) {
        if (inMemoryFormat == NATIVE && !getBuildInfo().isEnterprise()) {
            throw new IllegalArgumentException("NATIVE storage format is supported in Hazelcast Enterprise only."
                    + " Make sure you have Hazelcast Enterprise JARs on your classpath!");
        }
    }

    /**
     * Throws {@link IllegalArgumentException} if the supplied {@link NearCacheConfig}
     * has an invalid {@link NearCachePreloaderConfig}.
     *
     * @param nearCacheConfig supplied NearCacheConfig
     * @param isClient        {@code true} if the config is for a Hazelcast client, {@code false} otherwise
     */
    private static void checkPreloaderConfig(NearCacheConfig nearCacheConfig, boolean isClient) {
        if (!isClient && nearCacheConfig.getPreloaderConfig().isEnabled()) {
            throw new IllegalArgumentException("The Near Cache pre-loader is just available on Hazelcast clients!");
        }
    }

    @SuppressWarnings("deprecation")
    private static void logIgnoredConfig(MapConfig mapConfig) {
        if (mapConfig.getMinEvictionCheckMillis() != DEFAULT_MIN_EVICTION_CHECK_MILLIS
                || mapConfig.getEvictionPercentage() != DEFAULT_EVICTION_PERCENTAGE) {
            LOGGER.warning("As of Hazelcast version 3.7 `minEvictionCheckMillis` and `evictionPercentage`"
                    + " are deprecated due to a change of the eviction mechanism."
                    + " The new eviction mechanism uses a probabilistic algorithm based on sampling."
                    + " Please see documentation for further details.");
        }
    }
}
