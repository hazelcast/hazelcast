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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;

import static com.hazelcast.internal.config.ConfigValidator.checkCacheEvictionConfig;
import static com.hazelcast.internal.eviction.EvictionChecker.EVICT_ALWAYS;
import static com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator;

/**
 * Contains eviction specific functionality of a {@link QueryCacheRecordStore}.
 */
class EvictionOperator {
    // It could be the current size of the CQC is over a configured limit. This can happen e.g. when multiple threads
    // are inserting entries concurrently. However each eviction cycle can remove at most 1 entry -> we run multiple
    // eviction cycles when the current size is over the configured eviction threshold. This property controls maximum
    // no. of eviction cycles during one insertion into CQC.
    // Too low value might be insufficient to properly evict entries, too high value can cause latency spikes.
    private static final int MAX_EVICTION_ATTEMPTS = 10;

    private final QueryCacheRecordHashMap cache;
    private final EvictionConfig evictionConfig;
    private final EvictionChecker evictionChecker;
    private final EvictionPolicyEvaluator<Object, QueryCacheRecord> evictionPolicyEvaluator;
    private final SamplingEvictionStrategy<Object, QueryCacheRecord, QueryCacheRecordHashMap> evictionStrategy;
    private final EvictionListener<Object, QueryCacheRecord> listener;
    private final ClassLoader classLoader;

    EvictionOperator(QueryCacheRecordHashMap cache,
                     QueryCacheConfig config,
                     EvictionListener<Object, QueryCacheRecord> listener,
                     ClassLoader classLoader) {
        this.cache = cache;
        this.evictionConfig = config.getEvictionConfig();
        this.evictionChecker = createCacheEvictionChecker();
        this.evictionPolicyEvaluator = createEvictionPolicyEvaluator();
        this.evictionStrategy = SamplingEvictionStrategy.INSTANCE;
        this.listener = listener;
        this.classLoader = classLoader;
    }

    boolean isEvictionEnabled() {
        return evictionStrategy != null && evictionPolicyEvaluator != null;
    }

    void evictIfRequired() {
        if (!isEvictionEnabled()) {
            return;
        }

        for (int i = 0; evictionChecker.isEvictionRequired() && i < MAX_EVICTION_ATTEMPTS; i++) {
            // we already established we should evict -> we can pass EVICT_ALWAYS to the eviction strategy
            evictionStrategy.evict(cache, evictionPolicyEvaluator, EVICT_ALWAYS, listener);
        }
    }

    private EvictionChecker createCacheEvictionChecker() {
        return () -> cache.size() >= evictionConfig.getSize();
    }

    private EvictionPolicyEvaluator<Object, QueryCacheRecord> createEvictionPolicyEvaluator() {
        checkCacheEvictionConfig(evictionConfig);
        return getEvictionPolicyEvaluator(evictionConfig, classLoader);
    }
}
