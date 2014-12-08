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
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.cache.impl.eviction.EvictionPolicyEvaluator;
import com.hazelcast.cache.impl.record.Expirable;
import com.hazelcast.util.Clock;

import java.util.Collections;

/**
 * Base class for evaluation implementations of {@link com.hazelcast.config.EvictionPolicy}.
 */
public abstract class AbstractEvictionPolicyEvaluator<A, E extends Evictable>
        implements EvictionPolicyEvaluator<A, E> {

    /**
     * Compares two evictable candidates and selects one as {@link com.hazelcast.config.EvictionPolicy} rule.
     *
     * @param current   Currently selected evictable candidate
     * @param candidate Evictable candidate to compare with current one
     *
     * @return the selected evictable candidate
     */
    protected abstract Evictable selectEvictableAsPolicy(Evictable current, Evictable candidate);

    /**
     * The evaluate method implements the {@link com.hazelcast.config.EvictionPolicy} rule
     * on the given input set of candidates.
     *
     * @param evictionCandidates Multiple {@link EvictionCandidate} to be evicted
     *
     * @return multiple {@link EvictionCandidate} these are available to be evicted
     */
    @Override
    public <C extends EvictionCandidate<A, E>> Iterable<C> evaluate(Iterable<C> evictionCandidates) {
        C evictionCandidate = null;
        long now = Clock.currentTimeMillis();
        for (C candidate : evictionCandidates) {
            if (evictionCandidate == null) {
                evictionCandidate = candidate;
            } else {
                Evictable evictable = candidate.getEvictable();

                if (evictable == null) {
                    continue;
                }

                // If evictable is also an expirable
                if (evictable instanceof Expirable) {
                    Expirable expirable = (Expirable) evictable;
                    // If there is an expired candidate, let's evict that one immediately
                    if (expirable.isExpiredAt(now)) {
                        return candidate instanceof Iterable
                                    ? (Iterable<C>) candidate
                                    : Collections.singleton(candidate);
                    }
                }

                Evictable selected = selectEvictableAsPolicy(evictionCandidate.getEvictable(), evictable);
                if (selected == evictable) {
                    evictionCandidate = candidate;
                }
            }
        }
        if (evictionCandidate == null) {
            return null;
        } else {
            return evictionCandidate instanceof Iterable
                    ? (Iterable<C>) evictionCandidate
                    : Collections.singleton(evictionCandidate);
        }
    }

}
