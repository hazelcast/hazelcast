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

import com.hazelcast.cache.impl.eviction.impl.evaluator.LFUEvictionPolicyEvaluator;
import com.hazelcast.cache.impl.eviction.impl.evaluator.LRUEvictionPolicyEvaluator;
import com.hazelcast.config.EvictionPolicy;

import java.util.HashMap;
import java.util.Map;

/**
 * Provider to get any kind ({@link EvictionPolicy}) of {@link EvictionPolicyEvaluator}.
 */
public final class EvictionPolicyEvaluatorProvider {

    private static final Map<EvictionPolicy, EvictionPolicyEvaluator> EVICTION_POLICY_EVALUATOR_MAP =
            new HashMap<EvictionPolicy, EvictionPolicyEvaluator>();

    static {
        init();
    }

    private EvictionPolicyEvaluatorProvider() {

    }

    private static void init() {
        EVICTION_POLICY_EVALUATOR_MAP.put(EvictionPolicy.LRU, new LRUEvictionPolicyEvaluator());
        EVICTION_POLICY_EVALUATOR_MAP.put(EvictionPolicy.LFU, new LFUEvictionPolicyEvaluator());
    }

    /**
     * Gets the {@link EvictionPolicyEvaluator} implementation specified with <code>evictionPolicy</code>.
     *
     * @param evictionPolicy {@link EvictionPolicy} of requested {@link EvictionPolicyEvaluator} implementation
     *
     * @return the requested {@link EvictionPolicyEvaluator} implementation
     */
    public static EvictionPolicyEvaluator getEvictionPolicyEvaluator(EvictionPolicy evictionPolicy) {
        if (evictionPolicy == null || evictionPolicy == EvictionPolicy.NONE) {
            return null;
        }
        final EvictionPolicyEvaluator evictionPolicyEvaluator =
                EVICTION_POLICY_EVALUATOR_MAP.get(evictionPolicy);
        if (evictionPolicyEvaluator != null) {
            return evictionPolicyEvaluator;
        } else {
            throw new IllegalArgumentException("Unsupported eviction policy: " + evictionPolicy);
        }
    }

}
