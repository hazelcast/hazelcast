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

package com.hazelcast.internal.eviction.impl.evaluator;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.internal.eviction.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.Expirable;
import com.hazelcast.util.Clock;

import java.util.Collections;

/**
 * Default {@link EvictionPolicyEvaluator} implementation.
 *
 * @param <A> Type of the accessor (id) of the {@link com.hazelcast.internal.eviction.EvictionCandidate}
 * @param <E> Type of the {@link com.hazelcast.internal.eviction.Evictable} value of
 *            {@link com.hazelcast.internal.eviction.EvictionCandidate}
 */
public class DefaultEvictionPolicyEvaluator<A, E extends Evictable>
        implements EvictionPolicyEvaluator<A, E> {

    private final EvictionPolicyComparator evictionPolicyComparator;

    public DefaultEvictionPolicyEvaluator(EvictionPolicyComparator evictionPolicyComparator) {
        this.evictionPolicyComparator = evictionPolicyComparator;
    }

    @Override
    public EvictionPolicyComparator getEvictionPolicyComparator() {
        return evictionPolicyComparator;
    }

    /**
     * The evaluate method implements the {@link com.hazelcast.config.EvictionPolicy} rule
     * on the given input set of candidates.
     *
     * @param evictionCandidates Multiple {@link com.hazelcast.internal.eviction.EvictionCandidate} to be evicted
     *
     * @return multiple {@link com.hazelcast.internal.eviction.EvictionCandidate} these are available to be evicted
     */
    @Override
    public <C extends EvictionCandidate<A, E>> Iterable<C> evaluate(Iterable<C> evictionCandidates) {
        C selectedEvictionCandidate = null;
        long now = Clock.currentTimeMillis();
        for (C currentEvictionCandidate : evictionCandidates) {
            if (selectedEvictionCandidate == null) {
                selectedEvictionCandidate = currentEvictionCandidate;
            } else {
                E evictable = currentEvictionCandidate.getEvictable();

                if (evictable == null) {
                    continue;
                }

                if (isExpired(now, evictable)) {
                    return currentEvictionCandidate instanceof Iterable
                            ? (Iterable<C>) currentEvictionCandidate
                            : Collections.singleton(currentEvictionCandidate);
                }

                int comparisonResult = evictionPolicyComparator.compare(selectedEvictionCandidate, currentEvictionCandidate);
                if (comparisonResult == EvictionPolicyComparator.SECOND_ENTRY_HAS_HIGHER_PRIORITY_TO_BE_EVICTED) {
                    selectedEvictionCandidate = currentEvictionCandidate;
                }
            }
        }
        if (selectedEvictionCandidate == null) {
            return null;
        } else {
            return selectedEvictionCandidate instanceof Iterable
                    ? (Iterable<C>) selectedEvictionCandidate
                    : Collections.singleton(selectedEvictionCandidate);
        }
    }

    private boolean isExpired(long now, Evictable evictable) {
        // If evictable is also an expirable
        if (evictable instanceof Expirable) {
            Expirable expirable = (Expirable) evictable;
            // If there is an expired candidate, let's evict that one immediately
            return expirable.isExpiredAt(now);
        }
        return false;
    }

}
