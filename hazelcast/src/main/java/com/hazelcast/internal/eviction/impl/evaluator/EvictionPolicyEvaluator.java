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

package com.hazelcast.internal.eviction.impl.evaluator;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.internal.eviction.Expirable;
import com.hazelcast.util.Clock;

/**
 * Default {@link EvictionPolicyEvaluator} implementation.
 *
 * @param <A> Type of the accessor (id) of the {@link com.hazelcast.internal.eviction.EvictionCandidate}
 * @param <E> Type of the {@link com.hazelcast.internal.eviction.Evictable} value of
 *            {@link com.hazelcast.internal.eviction.EvictionCandidate}
 */
public class EvictionPolicyEvaluator<A, E extends Evictable> {

    private final EvictionPolicyComparator evictionPolicyComparator;

    public EvictionPolicyEvaluator(EvictionPolicyComparator evictionPolicyComparator) {
        this.evictionPolicyComparator = evictionPolicyComparator;
    }

    public EvictionPolicyComparator getEvictionPolicyComparator() {
        return evictionPolicyComparator;
    }

    /**
     * Selects the best candidate to be evicted.
     * The definition of the best depends on configured eviction policy. (LRU, LFU, custom, etc)
     *
     * It returns <code>null</code> when there the input is empty.
     *
     * @param evictionCandidates Multiple {@link com.hazelcast.internal.eviction.EvictionCandidate} to be evicted
     * @return a selected candidate to be evicted or null.
     */
    public <C extends EvictionCandidate<A, E>> C evaluate(Iterable<C> evictionCandidates) {
        C selectedEvictionCandidate = null;
        long now = Clock.currentTimeMillis();
        for (C currentEvictionCandidate : evictionCandidates) {
            if (selectedEvictionCandidate == null) {
                selectedEvictionCandidate = currentEvictionCandidate;
            } else {
                E evictable = currentEvictionCandidate.getEvictable();
                if (isExpired(now, evictable)) {
                    return currentEvictionCandidate;
                }

                int comparisonResult = evictionPolicyComparator.compare(selectedEvictionCandidate, currentEvictionCandidate);
                if (comparisonResult == EvictionPolicyComparator.SECOND_ENTRY_HAS_HIGHER_PRIORITY_TO_BE_EVICTED) {
                    selectedEvictionCandidate = currentEvictionCandidate;
                }
            }
        }
        return selectedEvictionCandidate;
    }

    private boolean isExpired(long now, Evictable evictable) {
        boolean expired = false;
        // check if evictable is also an expirable
        if (evictable instanceof Expirable) {
            Expirable expirable = (Expirable) evictable;
            // if there is an expired candidate, let's evict that one immediately
            expired = expirable.isExpiredAt(now);
        }
        return expired;
    }
}
