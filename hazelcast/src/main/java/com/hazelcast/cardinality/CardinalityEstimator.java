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

package com.hazelcast.cardinality;

import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.DistributedObject;

import java.util.concurrent.CompletionStage;

import javax.annotation.Nonnull;

/**
 * CardinalityEstimator is a redundant and highly available distributed
 * data-structure used for probabilistic cardinality estimation purposes,
 * on unique items, in significantly sized data cultures.
 * <p>
 * CardinalityEstimator is internally based on a HyperLogLog++ data-structure,
 * and uses P^2 byte registers for storage and computation. (Default P = 14)
 * <p>
 * Supports split brain protection {@link SplitBrainProtectionConfig} since 3.10 in cluster versions 3.10 and higher.
 * <p>
 * <b>Asynchronous methods</b>
 * <p>
 * Asynchronous methods return a {@link CompletionStage} that can be used to
 * chain further computation stages. Alternatively, a {@link java.util.concurrent.CompletableFuture}
 * can be obtained via {@link CompletionStage#toCompletableFuture()} to wait
 * for the operation to complete in a blocking way.
 * <p>
 * Actions supplied for dependent completions of default non-async methods and async methods
 * without an explicit {@link java.util.concurrent.Executor} argument are performed
 * by the {@link java.util.concurrent.ForkJoinPool#commonPool()} (unless it does not
 * support a parallelism level of at least 2, in which case a new {@code Thread} is
 * created per task).
 */
public interface CardinalityEstimator extends DistributedObject {

    /**
     * Add a new object in the estimation set. This is the method you want to
     * use to feed objects into the estimator.
     * <p>
     * Objects are considered identical if they are serialized into the same
     * binary blob.
     * In other words: It does <strong>not</strong> use Java equality.
     *
     * @param obj object to add in the estimation set.
     * @throws NullPointerException if obj is null
     * @since 3.8
     */
    void add(@Nonnull Object obj);

    /**
     * Estimates the cardinality of the aggregation so far.
     * If it was previously estimated and never invalidated, then a cached
     * version is used.
     *
     * @return a cached estimation or a newly computed one.
     * @since 3.8
     */
    long estimate();

    /**
     * Add a new object in the estimation set. This is the method you want to
     * use to feed objects into the estimator.
     * <p>
     * Objects are considered identical if they are serialized into the same
     * binary blob.
     * In other words: It does <strong>not</strong> use Java equality.
     * <p>
     * This method will dispatch a request and return immediately a {@link CompletionStage}.
     * The operations result can be obtained in a blocking way, or a
     * callback can be provided for execution upon completion, as demonstrated in the following examples:
     * <pre>
     *     CompletionStage&lt;Void&gt; stage = estimator.addAsync();
     *     // do something else, then read the result
     *     Boolean result = stage.toCompletableFuture().get(); // this method will block until the result is available
     * </pre>
     * <pre>
     *     CompletionStage&lt;Void&gt; stage = estimator.addAsync();
     *     stage.whenCompleteAsync((response, throwable) -&gt; {
     *          if (throwable == null) {
     *              // do something
     *          } else {
     *              // handle failure
     *          }
     *     });
     * </pre>
     *
     * @param obj object to add in the estimation set.
     * @return a {@link CompletionStage} API consumers can use to chain further computation stages
     * @throws NullPointerException if obj is null
     * @since 3.8
     */
    CompletionStage<Void> addAsync(@Nonnull Object obj);

    /**
     * Estimates the cardinality of the aggregation so far.
     * If it was previously estimated and never invalidated, then a cached version
     * is used.
     * <p>
     * This method will dispatch a request and return immediately a {@link CompletionStage}.
     * The operations result can be obtained in a blocking way, or a
     * callback can be provided for execution upon completion, as demonstrated in the following examples:
     * <pre>
     *     CompletionStage&lt;Long&gt; stage = estimator.estimateAsync();
     *     // do something else, then read the result
     *     Long result = stage.toCompletableFuture().get(); // this method will block until the result is available
     * </pre>
     * <pre>
     *     CompletionStage&lt;Long&gt; stage = estimator.estimateAsync();
     *     stage.whenCompleteAsync((response, throwable) -&gt; {
     *          if (throwable == null) {
     *              // do something with the result
     *          } else {
     *              // handle failure
     *          }
     *     });
     * </pre>
     *
     * @return {@link CompletionStage} bearing the response, the estimate.
     * @since 3.8
     */
    CompletionStage<Long> estimateAsync();
}
