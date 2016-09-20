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
import com.hazelcast.spi.annotation.Beta;

/**
 * CardinalityEstimator is a redundant and highly available distributed data-structure used
 * for probabilistic cardinality estimation purposes, on unique items, in significantly sized data cultures.
 *
 * CardinalityEstimator is internally based on a HyperLogLog++ data-structure,
 * and uses P^2 byte registers for storage and computation. (Default P = 14)
 */
@Beta
public interface CardinalityEstimator extends DistributedObject {

    /**
     * Consider the 64bit hash value, produced by the serialized form of the given object,
     * in the cardinality estimation. The implementation is free to choose whether this hash
     * will be used towards the estimation, therefore, this operation may or may not affect
     * the current estimation.
     *
     * Input object, unless a boxed primitive, must have a
     * {@link com.hazelcast.nio.serialization.StreamSerializer} implementation, registered in the Hazelcast
     * config to allow serialization. The hashing algorithm used to calculate the 64bit hash,
     * is {@link com.hazelcast.util.HashUtil#MurmurHash3_x64_64(byte[], int, int)}.
     *
     * @param obj the serializable object to aggregate in the estimate.
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
     * Consider the 64bit hash value, produced by the serialized form of the given object,
     * in the cardinality estimation. The implementation is free to choose whether this hash
     * will be used towards the estimation, therefore, this operation may or may not affect
     * the current estimation.
     *
     * Input object, unless a boxed primitive, must have a
     * {@link com.hazelcast.nio.serialization.StreamSerializer} implementation, registered in the Hazelcast
     * config to allow serialization. The hashing algorithm used to calculate the 64bit hash,
     * is {@link com.hazelcast.util.HashUtil#MurmurHash3_x64_64(byte[], int, int)}.
     *
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     * The operations result can be obtained in a blocking way, or a
     * callback can be provided for execution upon completion, as demonstrated in the following examples:
     * <p>
     * <pre>
     *     ICompletableFuture&lt;Void&gt; future = estimator.addAsync();
     *     // do something else, then read the result
     *     Boolean result = future.get(); // this method will block until the result is available
     * </pre>
     * </p>
     * <p>
     * <pre>
     *     ICompletableFuture&lt;Void&gt; future = estimator.addAsync();
     *     future.andThen(new ExecutionCallback&lt;Void&gt;() {
     *          void onResponse(Void response) {
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
     * @return an {@link ICompletableFuture} API consumers can use to track execution of this request.
     * @throws NullPointerException if obj is null
     * @since 3.8
     */
    ICompletableFuture<Void> addAsync(Object obj);

    /**
     * Estimates the cardinality of the aggregation so far.
     * If it was previously estimated and never invalidated, then a cached version is used.
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
     * @return {@link ICompletableFuture} bearing the response, the estimate.
     * @since 3.8
     */
    ICompletableFuture<Long> estimateAsync();

}
