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

package com.hazelcast.cache.impl.eviction.policies;

import com.hazelcast.cache.impl.eviction.AbstractEvictionPolicyStrategy;
import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.util.Clock;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

/**
 * An abstract base class {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy} returning
 * a single entry from {@link #evaluate(Iterable)} method by comparing the entries based on the given
 * {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}, such as LRU / LFU.
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 */
public abstract class AbstractSingleEvictionPolicyStrategy<A, E extends Evictable>
        extends AbstractEvictionPolicyStrategy<A, E> {

    /**
     * Compares two evictable candidates and returns the better matching one based on the
     * policy implementation.
     *
     * @param current   Currently selected evictable candidate
     * @param candidate Evictable candidate to compare with current one
     * @return The selected evictable candidate
     */
    protected abstract Evictable compareEvictable(Evictable current, Evictable candidate);

    /**
     * The evaluate method implements the {@link com.hazelcast.config.EvictionPolicy} rule
     * on the given input set of candidates.
     *
     * @param evictionCandidates Multiple {@link EvictionCandidate}s to select the best matching
     *                           candidate from that will be evicted
     * @return Multiple {@link EvictionCandidate}s to be evicted
     */
    @Override
    public Iterable<EvictionCandidate<A, E>> evaluate(Iterable<EvictionCandidate<A, E>> evictionCandidates) {
        EvictionCandidate<A, E> evictionCandidate = null;
        long now = Clock.currentTimeMillis();

        Iterator<EvictionCandidate<A, E>> iterator = evictionCandidates.iterator();

        boolean hasAtLeastOneElement = false;
        while (iterator.hasNext()) {
            hasAtLeastOneElement = true;

            EvictionCandidate<A, E> candidate = iterator.next();
            if (evictionCandidate == null) {
                evictionCandidate = candidate;
            } else {
                Evictable evictable = candidate.getEvictable();

                if (evictable == null) {
                    continue;
                }

                // Test if evictable is already expired (if expirable)
                if (evictable.isExpired(now)) {
                    // If there is an expired candidate, let's evict that one immediately
                    return Collections.singleton(candidate);
                }

                Evictable selected = compareEvictable(evictionCandidate.getEvictable(), evictable);
                if (selected == evictable) {
                    evictionCandidate = candidate;
                }
            }
        }

        if (!hasAtLeastOneElement) {
            throw new IllegalStateException("At least one EvictionCandidate must be provided for eviction selection");
        }

        if (evictionCandidate == null) {
            return null;
        }

        return Collections.singleton(evictionCandidate);
    }

    @Override
    public void onCreation(Evictable evictable) {
    }

    @Override
    public void onLoad(Evictable evictable) {
    }

    @Override
    public void onRead(Evictable evictable) {
    }

    @Override
    public void onUpdate(Evictable evictable) {
    }

    @Override
    public void onRemove(Evictable evictable) {
    }

    @Override
    public void configure(Properties properties) {
    }
}
