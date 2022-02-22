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

package com.hazelcast.internal.eviction.impl.evaluator;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.Expirable;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;

/**
 * Default {@link EvictionPolicyEvaluator} implementation.
 *
 * @param <A> Type of the accessor (id) of the {@link
 *            com.hazelcast.internal.eviction.EvictionCandidate}
 * @param <E> Type of the {@link com.hazelcast.internal.eviction.Evictable} value of
 *            {@link com.hazelcast.internal.eviction.EvictionCandidate}
 */
public class EvictionPolicyEvaluator<A, E extends Evictable> {

    private final EvictionPolicyComparator comparator;

    public EvictionPolicyEvaluator(EvictionPolicyComparator comparator) {
        this.comparator = comparator;
    }

    public EvictionPolicyComparator getEvictionPolicyComparator() {
        return comparator;
    }

    /**
     * Selects the best candidate to be evicted.
     * The definition of the best depends on configured
     * eviction policy. (LRU, LFU, custom, etc)
     *
     * It returns <code>null</code> when there the input is empty.
     *
     * @param candidates Multiple {@link
     *                   com.hazelcast.internal.eviction.EvictionCandidate} to be evicted
     * @return a selected candidate to be evicted or null.
     */
    @SuppressWarnings("checkstyle:rvcheckcomparetoforspecificreturnvalue")
    public <C extends EvictionCandidate<A, E>> C evaluate(Iterable<C> candidates) {
        long now = Clock.currentTimeMillis();

        C selected = null;
        for (C current : candidates) {
            // initialize selected by setting it to current candidate.
            if (selected == null) {
                selected = current;
                continue;
            }

            // then check if current candidate is expired.
            if (isExpired(current.getEvictable(), now)) {
                return current;
            }

            // check if current candidate is more eligible than selected.
            if (comparator.compare(current, selected) < 0) {
                selected = current;
            }
        }
        return selected;
    }

    private static boolean isExpired(Evictable evictable, long now) {
        if (!(evictable instanceof Expirable)) {
            return false;
        }

        return ((Expirable) evictable).isExpiredAt(now);
    }
}
