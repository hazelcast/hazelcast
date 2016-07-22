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

package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_EVICTION_PERCENTAGE;
import static com.hazelcast.config.MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS;

/**
 * Validates map configuration.
 */
public final class MapConfigValidator {

    private static final ILogger LOGGER = Logger.getLogger(MapConfig.class);

    private MapConfigValidator() {
    }

    /**
     * Throws {@link IllegalArgumentException} if the supplied {@link InMemoryFormat} is {@link InMemoryFormat#NATIVE}
     *
     * @param inMemoryFormat supplied inMemoryFormat.
     */
    public static void checkNotNative(InMemoryFormat inMemoryFormat) {
        if (NATIVE == inMemoryFormat) {
            throw new IllegalArgumentException("NATIVE storage format is supported in Hazelcast Enterprise only. "
                    + "Make sure you have Hazelcast Enterprise JARs on your classpath!");
        }
    }

    /**
     * Checks preconditions to create a map proxy.
     *
     * @param mapConfig the mapConfig
     */
    public static void checkMapConfig(MapConfig mapConfig) {
        checkNotNative(mapConfig.getInMemoryFormat());

        logIgnoredConfig(mapConfig);
    }

    private static void logIgnoredConfig(MapConfig mapConfig) {
        if (DEFAULT_MIN_EVICTION_CHECK_MILLIS != mapConfig.getMinEvictionCheckMillis()
                || DEFAULT_EVICTION_PERCENTAGE != mapConfig.getEvictionPercentage()) {

            LOGGER.warning("As of version 3.7, `minEvictionCheckMillis` and `evictionPercentage` "
                    + "are deprecated due to the eviction mechanism change. New eviction mechanism "
                    + "uses a probabilistic algorithm based on sampling. Please see documentation for further details");
        }
    }
}
