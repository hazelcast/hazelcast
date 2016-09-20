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

package com.hazelcast.cardinality;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ICompletableFuture;

/**
 * CardinalityEstimator is a redundant and highly available distributed data-structure used
 * for probabilistic cardinality estimation purposes, on unique items, in significantly sized data cultures.
 *
 * CardinalityEstimator is internally based on HyperLogLog data-structure,
 * and uses P^2 byte registers for storage and computation. (Default P = 14)
 */
public interface CardinalityEstimator extends DistributedObject {

    /**
     * Aggregates the given hash which can result in a new estimation being available or not.
     * The latter case would signify that the internal registers of the data-structure were not affected.
     * The most obvious reason for this scenario would be that another hash (seen before) caused a register
     * to hold a bigger value that this one could, therefore it got skipped.
     *
     * @param obj the serializable object to aggregate in the estimate.
     * @return boolean flag True, when a new estimate can be computed.
     * @since 3.8
     */
    boolean aggregate(Object obj);


    /**
     * Estimates the cardinality of the aggregation so far.
     * If it was previously estimated and never invalidated, then the cached version is used.
     *
     * @return Returns the previous cached estimation or the newly computed one.
     * @since 3.8
     */
    long estimate();

    /**
     * Aggregates the given hash which can result in a new estimation being available or not.
     *
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     * The operations result can be obtained in a blocking way, or a
     * callback can be provided for execution upon completion, as demonstrated in the following examples:
     * <p>
     * <pre>
     *     ICompletableFuture&lt;Boolean&gt; future = estimator.aggregateAsync();
     *     // do something else, then read the result
     *     Boolean result = future.get(); // this method will block until the result is available
     * </pre>
     * </p>
     * <p>
     * <pre>
     *     ICompletableFuture&lt;Boolean&gt; future = estimator.aggregateAsync();
     *     future.andThen(new ExecutionCallback&lt;Boolean&gt;() {
     *          void onResponse(Boolean response) {
     *              // do something with the result
     *          }
     *
     *          void onFailure(Throwable t) {
     *              // handle failure
     *          }
     *     });
     * </pre>
     * </p>
     * @param obj the serializable object to aggregate in the estimate.
     * @return {@link ICompletableFuture} bearing the response, True, when a new estimate can be computed.
     * @since 3.8
     */
    ICompletableFuture<Boolean> aggregateAsync(Object obj);

    /**
     * Estimates the cardinality of the aggregation so far.
     * If it was previously estimated and never invalidated, then the cached version is used.
     *
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     * The operations result can be obtained in a blocking way, or a
     * callback can be provided for execution upon completion, as demonstrated in the following examples:
     * <p>
     * <pre>
     *     ICompletableFuture&lt;Long&gt; future = estimator.estimateAsync();
     *     // do something else, then read the result
     *     Long result = future.get(); // this method will block until the result is available
     * </pre>
     * </p>
     * <p>
     * <pre>
     *     ICompletableFuture&lt;Long&gt; future = estimator.estimateAsync();
     *     future.andThen(new ExecutionCallback&lt;Long&gt;() {
     *          void onResponse(Long response) {
     *              // do something with the result
     *          }
     *
     *          void onFailure(Throwable t) {
     *              // handle failure
     *          }
     *     });
     * </pre>
     * </p>
     * @return {@link ICompletableFuture} bearing the response, the new estimate.
     * @since 3.8
     */
    ICompletableFuture<Long> estimateAsync();

}
