/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.eviction;

import com.hazelcast.cache.impl.eviction.impl.strategy.sampling.SamplingBasedEvictionStrategy;

import java.util.HashMap;
import java.util.Map;

/**
 * Provider to get any kind ({@link EvictionStrategyType}) of {@link EvictionStrategy}.
 */
public final class EvictionStrategyProvider {

    private static final Map<EvictionStrategyType, EvictionStrategy> EVICTION_STRATEGY_MAP =
            new HashMap<EvictionStrategyType, EvictionStrategy>();

    static {
        init();
    }

    private EvictionStrategyProvider() {

    }

    private static void init() {
        EVICTION_STRATEGY_MAP.put(EvictionStrategyType.SAMPLING_BASED_EVICTION, new SamplingBasedEvictionStrategy());
    }

    /**
     * Gets the {@link EvictionStrategy} implementation specified with <code>evictionStrategyType</code>.
     *
     * @param evictionConfig {@link EvictionConfig} for requested {@link EvictionStrategy} implementation
     *
     * @return the requested {@link EvictionStrategy} implementation
     */
    public static EvictionStrategy getEvictionStrategy(EvictionConfig evictionConfig) {
        if (evictionConfig == null) {
            return null;
        }
        final EvictionStrategyType evictionStrategyType = evictionConfig.getEvictionStrategyType();
        if (evictionStrategyType == null) {
            return null;
        }
        return EVICTION_STRATEGY_MAP.get(evictionStrategyType);

        // TODO "evictionStrategyFactory" can be handled here from a single point
        // for user defined custom implementations.
        // So "EvictionStrategy" implementation can be taken from user defined factory.
    }

    /**
     * Gets default {@link EvictionStrategy} implementation.
     *
     * @return the default {@link EvictionStrategy} implementation
     */
    public static EvictionStrategy getDefaultEvictionStrategy() {
        return EVICTION_STRATEGY_MAP.get(EvictionStrategyType.DEFAULT_EVICTION_STRATEGY);
    }

}
