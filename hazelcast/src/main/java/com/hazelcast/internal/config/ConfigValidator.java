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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_EVICTION_PERCENTAGE;
import static com.hazelcast.config.MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS;

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

        checkNoCacheLocalEntriesOnClients(nearCacheConfig, isClient);
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
     * Throws {@link IllegalArgumentException} if the supplied {@link NearCacheConfig} has
     * {@link NearCacheConfig#isCacheLocalEntries} set.
     *
     * @param nearCacheConfig supplied NearCacheConfig
     * @param isClient        if the supplied NearCacheConfig is from a client
     */
    private static void checkNoCacheLocalEntriesOnClients(NearCacheConfig nearCacheConfig, boolean isClient) {
        if (isClient && nearCacheConfig.isCacheLocalEntries()) {
            throw new IllegalArgumentException(
                    "The Near Cache option `cache-local-entries` is not supported in client configurations!");
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
