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

package com.hazelcast.cache.impl.eviction.impl.evaluator;

import com.hazelcast.cache.impl.eviction.Evictable;

/**
 * Interface for evaluation implementations of {@link com.hazelcast.config.EvictionPolicy#LRU} policy.
 */
public class LRUEvictionPolicyEvaluator<A, E extends Evictable>
        extends AbstractEvictionPolicyEvaluator<A, E> {

    @Override
    protected Evictable selectEvictableAsPolicy(Evictable current, Evictable candidate) {
        if (candidate.getAccessTime() < current.getAccessTime()) {
            return candidate;
        } else {
            return current;
        }
    }

}
