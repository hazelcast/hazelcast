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

package com.hazelcast.cardinality;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.spi.annotation.Beta;

/**
 * CardinalityEstimator is a redundant and highly available distributed data-structure used
 * for probabilistic cardinality estimation purposes, on unique items, in significantly sized data cultures.
 * <p>
 * CardinalityEstimator is internally based on a HyperLogLog++ data-structure,
 * and uses P^2 byte registers for storage and computation. (Default P = 14)
 * <p>
 * Supports Quorum {@link com.hazelcast.config.QuorumConfig} since 3.10 in cluster versions 3.10 and higher.
 */
@Beta
public interface CardinalityEstimator extends DistributedObject {

    /**
     * Add a new object in the estimation set. This is the method you want to
     * use to feed objects into the estimator.
     * <p>
     * Objects are considered identical if they are serialized into the same binary blob.
     * In other words: It does <strong>not</strong> use Java equality.
     *
     * @param obj object to add in the estimation set.
     * @throws NullPointerException if obj is null
     * @since 3.8
     */
    void add(Object obj);

    /**
     * Estimates the cardinality of the aggregation so far.
     * If it was previously estimated and never invalidated, then a cached version is used.
     *
     * @return a cached estimation or a newly computed one.
     * @since 3.8
     */
    long estimate();

    /**
     * Add a new object in the estimation set. This is the method you want to
     * use to feed objects into the estimator.
     * <p>
     * Objects are considered identical if they are serialized into the same binary blob.
     * In other words: It does <strong>not</strong> use Java equality.
     * <p>
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     * The operations result can be obtained in a blocking way, or a
     * callback can be provided for execution upon completion, as demonstrated in the following examples:
     * <pre>
     *     ICompletableFuture&lt;Void&gt; future = estimator.addAsync();
     *     // do something else, then read the result
     *     Boolean result = future.get(); // this method will block until the result is available
     * </pre>
     * <pre>
     *     ICompletableFuture&lt;Void&gt; future = estimator.addAsync();
     *     future.andThen(new ExecutionCallback&lt;Void&gt;() {
     *          void onResponse(Void response) {
     *              // do something
     *          }
     *
     *          void onFailure(Throwable t) {
     *              // handle failure
     *          }
     *     });
     * </pre>
     *
     * @param obj object to add in the estimation set.
     * @return an {@link ICompletableFuture} API consumers can use to track execution of this request.
     * @throws NullPointerException if obj is null
     * @since 3.8
     */
    ICompletableFuture<Void> addAsync(Object obj);

    /**
     * Estimates the cardinality of the aggregation so far.
     * If it was previously estimated and never invalidated, then a cached version is used.
     * <p>
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     * The operations result can be obtained in a blocking way, or a
     * callback can be provided for execution upon completion, as demonstrated in the following examples:
     * <pre>
     *     ICompletableFuture&lt;Long&gt; future = estimator.estimateAsync();
     *     // do something else, then read the result
     *     Long result = future.get(); // this method will block until the result is available
     * </pre>
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
     *
     * @return {@link ICompletableFuture} bearing the response, the estimate.
     * @since 3.8
     */
    ICompletableFuture<Long> estimateAsync();
}
