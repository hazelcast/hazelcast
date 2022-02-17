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

package com.hazelcast.aggregation;

import java.io.Serializable;

/**
 * Defines a contract for all aggregators. Exposes API for parallel two-phase aggregations:
 * - accumulation of input entries by multiple instance of aggregators
 * - combining all aggregators into one to calculate the final result
 * <p>
 * Aggregator does not have to be thread-safe.
 * accumulate() and combine() calls may be interwoven.
 * <p>
 * The very instance passed to an aggregate() method will not be used at all. It is just a prototype object
 * that will be cloned using serialization, since each partition gets its own instance of an aggregator.
 * In this way the aggregator is not used by multiple-threads. Each thread gets its own aggregator instance.
 *
 * @param <I> input type
 * @param <R> result type
 * @since 3.8
 */
public interface Aggregator<I, R> extends Serializable {

    /**
     * Accumulates the given entries.
     *
     * @param input input to accumulate.
     */
    void accumulate(I input);

    /**
     * Called after the last call to combine on a specific instance. Enables disposing of the intermediary state.
     * This should be a very fast operation that just disposes unnecessary state (if applicable).
     * <p>
     * IMPORTANT: It may not be called if the instance aggregator does not take part in the accumulation phase.
     * It's caused by the fact that the aggregation may be run in a parallel way and each thread gets a clone of the
     * aggregator.
     */
    default void onAccumulationFinished() {
    }

    /**
     * Incorporates the intermediary result of the given aggregator to this instance of the aggregator.
     * The given aggregator has to be of the same class as the one that the method is being called on.
     * Enables merging the intermediary state from a given aggregator.
     * It is used when the aggregation is split into a couple of aggregators.
     *
     * @param aggregator aggregator providing intermediary results to be combined into the results of this aggregator.
     *                   The given aggregator has to be of the same class as the one that the method is being called on.
     */
    void combine(Aggregator aggregator);

    /**
     * Called after the last call to combine on a specific instance. Enables disposing of the intermediary state.
     * This should be a very fast operation that just disposes unnecessary state (if applicable).
     * <p>
     * IMPORTANT: It may not be called if the instance aggregator does not take part in the combination phase.
     * It's caused by the fact that the aggregation may be run in a parallel way and each thread gets a clone of the
     * aggregator.
     */
    default void onCombinationFinished() {
    }

    /**
     * Returns the result of the aggregation. The result may be calculated in this call or cached by the aggregator.
     *
     * @return returns the result of the aggregation.
     */
    R aggregate();

}
