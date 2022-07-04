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

package com.hazelcast.jet.core.function;

import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Represents the function you pass to windowing processors
 * such as {@link Processors#aggregateToSlidingWindowP aggregateToSlidingWindowP()}
 * and {@link Processors#aggregateToSessionWindowP aggregateToSessionWindowP()} as
 * {@code mapToOutputFn}.
 * It creates the item to emit based on the results of a single aggregate
 * operation performed for a particular window and a particular grouping
 * key.
 *
 * @param <K> type of the key
 * @param <R> the type of aggregation result this function receives
 * @param <OUT> the type of the output item this function returns
 *
 * @since Jet 3.0
 */
@FunctionalInterface
public interface KeyedWindowResultFunction<K, R, OUT> extends Serializable {

    /**
     * Applies the function to the given arguments
     *
     * @param winStart the inclusive lower timestamp of the window
     * @param winEnd the exclusive upper timestamp of the window
     * @param key the grouping key
     * @param windowResult the result of the aggregate operation
     * @param isEarly whether the result is an early result as specified by
     *                {@link WindowDefinition#setEarlyResultsPeriod(long)}
     * @return the function result
     */
    @Nullable
    OUT apply(long winStart, long winEnd, @Nonnull K key, @Nonnull R windowResult, boolean isEarly);

}
