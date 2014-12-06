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

/**
 * Interface for evaluation implementations of {@link com.hazelcast.config.EvictionPolicy}.
 */
public interface EvictionPolicyEvaluator<A, E extends Evictable> {

    /**
     * The evaluate method implements the actual policy rules and is called on every eviction to select one or
     * more candidates to be evicted from the given input set of candidates.
     * The selection algorithm should execute in a constant time to deliver a predictable timing results of
     * the eviction system.
     *
     * @param evictionCandidates Multiple {@link EvictionCandidate} to be evicted
     *
     * @return multiple {@link EvictionCandidate} these are available to be evicted
     */
    <C extends EvictionCandidate<A, E>> Iterable<C> evaluate(Iterable<C> evictionCandidates);

}
