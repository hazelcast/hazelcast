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
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.config.AbstractBasicConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.merge.SplitBrainMergeTypeProvider;

import java.util.EnumSet;

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_EVICTION_PERCENTAGE;
import static com.hazelcast.config.MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.internal.config.MergePolicyValidator.checkCacheMergePolicy;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMapMergePolicy;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMergePolicy;
import static com.hazelcast.internal.config.MergePolicyValidator.checkReplicatedMapMergePolicy;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static java.lang.String.format;

/**
 * Validates a Hazelcast configuration in a specific context like OS vs. EE or client vs. member nodes.
 */
public final class ConfigValidator {

    private static final ILogger LOGGER = Logger.getLogger(ConfigValidator.class);

    private static final EnumSet<MaxSizePolicy> SUPPORTED_ON_HEAP_NEAR_CACHE_MAXSIZE_POLICIES
            = EnumSet.of(MaxSizePolicy.ENTRY_COUNT);

    private static final EnumSet<EvictionPolicy> SUPPORTED_EVICTION_POLICIES = EnumSet.of(LRU, LFU);

    private ConfigValidator() {
    }

    /**
     * Validates the given {@link MapConfig}.
     *
     * @param mapConfig           the {@link MapConfig}
     * @param mergePolicyProvider the {@link MergePolicyProvider} to resolve merge policy classes
     */
    public static void checkMapConfig(MapConfig mapConfig, MergePolicyProvider mergePolicyProvider) {
        checkNotNativeWhenOpenSource(mapConfig.getInMemoryFormat());
        checkMapMergePolicy(mapConfig, mergePolicyProvider);
        logIgnoredConfig(mapConfig);
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

    /**
     * Checks preconditions to create a map proxy with Near Cache.
     *
     * @param mapName            name of the map that Near Cache will be created for
     * @param nearCacheConfig    the {@link NearCacheConfig} to be checked
     * @param nativeMemoryConfig the {@link NativeMemoryConfig} of the Hazelcast instance
     * @param isClient           {@code true} if the config is for a Hazelcast client, {@code false} otherwise
     */
    public static void checkNearCacheConfig(String mapName, NearCacheConfig nearCacheConfig,
                                            NativeMemoryConfig nativeMemoryConfig, boolean isClient) {
        checkNotNativeWhenOpenSource(nearCacheConfig.getInMemoryFormat());
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
     * Checks IMap's supported Near Cache local update policy configuration.
     *
     * @param mapName           name of the map that Near Cache will be created for
     * @param localUpdatePolicy local update policy
     */
    private static void checkLocalUpdatePolicy(String mapName, LocalUpdatePolicy localUpdatePolicy) {
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

    private static void checkOnHeapNearCacheMaxSizePolicy(NearCacheConfig nearCacheConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat == NATIVE) {
            return;
        }

        MaxSizePolicy maxSizePolicy = nearCacheConfig.getEvictionConfig().getMaximumSizePolicy();
        if (!SUPPORTED_ON_HEAP_NEAR_CACHE_MAXSIZE_POLICIES.contains(maxSizePolicy)) {
            throw new IllegalArgumentException(format("Near Cache maximum size policy %s cannot be used with %s storage."
                            + " Supported maximum size policies are: %s",
                    maxSizePolicy, inMemoryFormat, SUPPORTED_ON_HEAP_NEAR_CACHE_MAXSIZE_POLICIES));
        }
    }

    /**
     * Checks precondition to use {@link InMemoryFormat#NATIVE}.
     *
     * @param inMemoryFormat     the {@link InMemoryFormat} of the Near Cache
     * @param nativeMemoryConfig the {@link NativeMemoryConfig} of the Hazelcast instance
     * @param isEnterprise       {@code true} if the Hazelcast instance is EE, {@code false} otherwise
     */
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

    /**
     * Checks if parameters for an {@link EvictionConfig} are valid in their context.
     *
     * @param evictionPolicy      the {@link EvictionPolicy} for the {@link EvictionConfig}
     * @param comparatorClassName the comparator class name for the {@link EvictionConfig}
     * @param comparator          the comparator implementation for the {@link EvictionConfig}
     * @param isNearCache         {@code true} if the config is for a Near Cache, {@code false} otherwise
     */
    public static void checkEvictionConfig(EvictionPolicy evictionPolicy, String comparatorClassName, Object comparator,
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
                    throw new IllegalArgumentException("Only one of the `eviction policy` and `comparator` can be configured!");
                }
            }
        }
    }

    /**
     * Validates the given {@link CacheSimpleConfig}.
     *
     * @param cacheSimpleConfig   the {@link CacheSimpleConfig} to check
     * @param mergePolicyProvider the {@link CacheMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCacheConfig(CacheSimpleConfig cacheSimpleConfig, CacheMergePolicyProvider mergePolicyProvider) {
        checkCacheConfig(cacheSimpleConfig.getInMemoryFormat(), cacheSimpleConfig.getEvictionConfig(),
                cacheSimpleConfig.getMergePolicy(), cacheSimpleConfig, mergePolicyProvider);
    }

    /**
     * Validates the given {@link CacheConfig}.
     *
     * @param cacheConfig         the {@link CacheConfig} to check
     * @param mergePolicyProvider the {@link CacheMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCacheConfig(CacheConfig cacheConfig, CacheMergePolicyProvider mergePolicyProvider) {
        checkCacheConfig(cacheConfig.getInMemoryFormat(), cacheConfig.getEvictionConfig(), cacheConfig.getMergePolicy(),
                cacheConfig, mergePolicyProvider);
    }

    /**
     * Validates the given parameters in the context of an {@link ICache} config.
     *
     * @param inMemoryFormat       the {@link InMemoryFormat} of the cache
     * @param evictionConfig       the {@link EvictionConfig} of the cache
     * @param mergePolicyClassname the configured merge policy of the cache
     * @param mergeTypeProvider    the {@link SplitBrainMergeTypeProvider} of the cache
     * @param mergePolicyProvider  the {@link CacheMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCacheConfig(InMemoryFormat inMemoryFormat, EvictionConfig evictionConfig, String mergePolicyClassname,
                                        SplitBrainMergeTypeProvider mergeTypeProvider,
                                        CacheMergePolicyProvider mergePolicyProvider) {
        checkNotNativeWhenOpenSource(inMemoryFormat);
        checkEvictionConfig(inMemoryFormat, evictionConfig);
        checkCacheMergePolicy(mergePolicyClassname, mergeTypeProvider, mergePolicyProvider);
    }

    /**
     * Checks the merge policy configuration in the context of an {@link ICache}.
     *
     * @param inMemoryFormat the {@link InMemoryFormat} of the cache
     * @param evictionConfig the {@link EvictionConfig} of the cache
     */
    static void checkEvictionConfig(InMemoryFormat inMemoryFormat, EvictionConfig evictionConfig) {
        if (inMemoryFormat == NATIVE) {
            MaxSizePolicy maxSizePolicy = evictionConfig.getMaximumSizePolicy();
            if (maxSizePolicy == MaxSizePolicy.ENTRY_COUNT) {
                throw new IllegalArgumentException("Invalid max-size policy "
                        + '(' + maxSizePolicy + ") for NATIVE in-memory format! Only "
                        + MaxSizePolicy.USED_NATIVE_MEMORY_SIZE + ", "
                        + MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + ", "
                        + MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE + ", "
                        + MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE
                        + " are supported.");
            }
        }
    }

    /**
     * Validates the given {@link ReplicatedMapConfig}.
     *
     * @param replicatedMapConfig the {@link ReplicatedMapConfig} to check
     * @param mergePolicyProvider the {@link com.hazelcast.replicatedmap.merge.MergePolicyProvider}
     *                            to resolve merge policy classes
     */
    public static void checkReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig,
                                                com.hazelcast.replicatedmap.merge.MergePolicyProvider mergePolicyProvider) {
        checkReplicatedMapMergePolicy(replicatedMapConfig, mergePolicyProvider);
    }

    /**
     * Validates the given {@link MultiMapConfig}.
     *
     * @param multiMapConfig      the {@link MultiMapConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkMultiMapConfig(MultiMapConfig multiMapConfig,
                                           SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergePolicy(multiMapConfig, mergePolicyProvider, multiMapConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link QueueConfig}.
     *
     * @param queueConfig         the {@link QueueConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkQueueConfig(QueueConfig queueConfig, SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergePolicy(queueConfig, mergePolicyProvider, queueConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link CollectionConfig}.
     *
     * @param collectionConfig    the {@link CollectionConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCollectionConfig(CollectionConfig collectionConfig,
                                             SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergePolicy(collectionConfig, mergePolicyProvider, collectionConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link RingbufferConfig}.
     *
     * @param ringbufferConfig    the {@link RingbufferConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkRingbufferConfig(RingbufferConfig ringbufferConfig,
                                             SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergePolicy(ringbufferConfig, mergePolicyProvider, ringbufferConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link AbstractBasicConfig} implementation.
     *
     * @param basicConfig         the {@link AbstractBasicConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     * @param <C>                 type of the {@link AbstractBasicConfig}
     */
    public static <C extends AbstractBasicConfig> void checkBasicConfig(C basicConfig,
                                                                        SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergePolicy(basicConfig, mergePolicyProvider, basicConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link ScheduledExecutorConfig}.
     *
     * @param scheduledExecutorConfig the {@link ScheduledExecutorConfig} to check
     * @param mergePolicyProvider     the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig,
                                                    SplitBrainMergePolicyProvider mergePolicyProvider) {
        String mergePolicyClassName = scheduledExecutorConfig.getMergePolicyConfig().getPolicy();
        checkMergePolicy(scheduledExecutorConfig, mergePolicyProvider, mergePolicyClassName);
    }

    /**
     * Throws {@link IllegalArgumentException} if the given {@link InMemoryFormat}
     * is {@link InMemoryFormat#NATIVE} and Hazelcast is OS.
     *
     * @param inMemoryFormat supplied inMemoryFormat
     */
    private static void checkNotNativeWhenOpenSource(InMemoryFormat inMemoryFormat) {
        if (inMemoryFormat == NATIVE && !getBuildInfo().isEnterprise()) {
            throw new IllegalArgumentException("NATIVE storage format is supported in Hazelcast Enterprise only."
                    + " Make sure you have Hazelcast Enterprise JARs on your classpath!");
        }
    }

    /**
     * Throws {@link IllegalArgumentException} if the given {@link NearCacheConfig}
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
}
