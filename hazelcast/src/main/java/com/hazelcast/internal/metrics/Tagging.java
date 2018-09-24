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

package com.hazelcast.internal.metrics;

import com.hazelcast.internal.metrics.ProbingCycle.Tags;

/**
 * Can be implemented by instances passed to {@link ProbingCycle#probe(Object)}
 * (and its sibling methods) to provided the {@link Tags} context by the object
 * itself instead of before calling {@code probe}.
 */
public interface Tagging {

    /**
     * The implementing object (with probed fields or methods) is asked to provide
     * the {@link Tags} context for the object.
     *
     * The performed tagging is relative to potential parent context {@link Tags}
     * that were added before probing the implementing instance.
     *
     * This is an alternative to building the context in the
     * {@link ProbeSource#probeNow(ProbingCycle)} implementation.
     *
     * @param context to use to build the objects context using the {@link Tags}
     *        methods.
     */
    void tagIn(Tags context);
}
