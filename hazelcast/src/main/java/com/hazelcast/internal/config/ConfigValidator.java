/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.EvictionPolicy.RANDOM;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_EVICTION_PERCENTAGE;
import static com.hazelcast.config.MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;

/**
 * Validates a Hazelcast configuration in a specific context like OS vs. EE or client vs. member nodes.
 */
public final class ConfigValidator {

    private static final ILogger LOGGER = Logger.getLogger(ConfigValidator.class);

    private ConfigValidator() {
    }

    /**
     * Checks preconditions to create a map proxy.
     *
     * @param mapConfig the {@link MapConfig}
     */
    public static void checkMapConfig(MapConfig mapConfig) {
        checkNotNative(mapConfig.getInMemoryFormat());

        logIgnoredConfig(mapConfig);
    }

    /**
     * Checks preconditions to create a map proxy with Near Cache.
     *
     * @param nearCacheConfig the {@link NearCacheConfig}
     * @param isClient        {@code true} if the config is for a Hazelcast client, {@code false} otherwise
     */
    public static void checkNearCacheConfig(NearCacheConfig nearCacheConfig, boolean isClient) {
        checkNotNative(nearCacheConfig.getInMemoryFormat());
        checkEvictionConfig(nearCacheConfig.getEvictionConfig(), true);

        if (isClient && nearCacheConfig.isCacheLocalEntries()) {
            throw new IllegalArgumentException(
                    "The Near Cache option `cache-local-entries` is not supported in client configurations!");
        }
        checkPreloaderConfig(nearCacheConfig, isClient);
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
        if (!isNearCache && (evictionPolicy == null || evictionPolicy == NONE || evictionPolicy == RANDOM)) {
            if (isNullOrEmpty(comparatorClassName) && comparator == null) {
                throw new IllegalArgumentException(
                        "Eviction policy must be set to an eviction policy type rather than `null`, `NONE`, `RANDOM`"
                                + " or custom eviction policy comparator must be specified!");
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
     * Throws {@link IllegalArgumentException} if the supplied {@link InMemoryFormat} is {@link InMemoryFormat#NATIVE}.
     *
     * @param inMemoryFormat supplied inMemoryFormat
     */
    private static void checkNotNative(InMemoryFormat inMemoryFormat) {
        if (inMemoryFormat == NATIVE && !BuildInfoProvider.getBuildInfo().isEnterprise()) {
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
            LOGGER.warning("As of version 3.7 `minEvictionCheckMillis` and `evictionPercentage`"
                    + " are deprecated due to a change of the eviction mechanism."
                    + " The new eviction mechanism uses a probabilistic algorithm based on sampling."
                    + " Please see documentation for further details.");
        }
    }
}
