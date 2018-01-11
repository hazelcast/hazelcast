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

package com.hazelcast.core;

/**
 * IAtomicLong is a redundant and highly available distributed alternative to the
 * {@link java.util.concurrent.atomic.AtomicLong java.util.concurrent.atomic.AtomicLong}.
 *
 * Asynchronous variants of all methods have been introduced in version 3.7.
 * Async methods return immediately an {@link ICompletableFuture} from which the operation's result
 * can be obtained either in a blocking manner or by registering a callback to be executed
 * upon completion. For example:
 *
 * <p>
 * <pre>
 *     ICompletableFuture&lt;Long&gt; future = atomicLong.addAndGetAsync(13);
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
 *
 * During a network partition event it is possible for the {@link IAtomicLong} to exist in each of the partitioned
 * clusters or to not exist at all. Under these circumstances the values held in the {@link IAtomicLong} may diverge.
 * Once the network partition heals, Hazelcast will use the value held in the largest cluster, in this case updates
 * made to the {@link IAtomicLong} in the smaller clusters will be lost.  Where the merging clusters are the same sizes
 * a winner of the merge will be randomly chosen.
 *
 * Supports Quorum {@link com.hazelcast.config.QuorumConfig} since 3.10 in cluster versions 3.10 and higher.
 *
 * @see IAtomicReference
 */
public interface IAtomicLong extends DistributedObject {

    /**
     * Returns the name of this IAtomicLong instance.
     *
     * @return the name of this IAtomicLong instance
     */
    String getName();

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add to the current value
     * @return the updated value, the given value added to the current value
     */
    long addAndGet(long delta);

    /**
     * Atomically sets the value to the given updated value
     * only if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return true if successful; or false if the actual value
     *         was not equal to the expected value.
     */
    boolean compareAndSet(long expect, long update);

    /**
     * Atomically decrements the current value by one.
     *
     * @return the updated value, the current value decremented by one
     */
    long decrementAndGet();

    /**
     * Gets the current value.
     *
     * @return the current value
     */
    long get();

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add to the current value
     * @return the old value before the add
     */
    long getAndAdd(long delta);

    /**
     * Atomically sets the given value and returns the old value.
     *
     * @param newValue the new value
     * @return the old value
     */
    long getAndSet(long newValue);

    /**
     * Atomically increments the current value by one.
     *
     * @return the updated value, the current value incremented by one
     */
    long incrementAndGet();

    /**
     * Atomically increments the current value by one.
     *
     * @return the old value
     */
    long getAndIncrement();

    /**
     * Atomically sets the given value.
     *
     * @param newValue the new value
     */
    void set(long newValue);

    /**
     * Alters the currently stored value by applying a function on it.
     *
     * @param function the function applied to the currently stored value
     * @throws IllegalArgumentException if function is null.
     * @since 3.2
     */
    void alter(IFunction<Long, Long> function);

    /**
     * Alters the currently stored value by applying a function on it and gets the result.
     *
     * @param function the function applied to the currently stored value
     * @return the new value.
     * @throws IllegalArgumentException if function is null.
     * @since 3.2
     */
    long alterAndGet(IFunction<Long, Long> function);

    /**
     * Alters the currently stored value by applying a function on it on and gets the old value.
     *
     * @param function the function applied to the currently stored value
     * @return  the old value
     * @throws IllegalArgumentException if function is null.
     * @since 3.2
     */
    long getAndAlter(IFunction<Long, Long> function);

    /**
     * Applies a function on the value, the actual stored value will not change.
     *
     * @param function the function applied to the value, the value is not changed
     * @return  the result of the function application
     * @throws IllegalArgumentException if function is null.
     * @since 3.2
     */
    <R> R apply(IFunction<Long, R> function);

    /**
     * Atomically adds the given value to the current value.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     * The operations result can be obtained in a blocking way, or a
     * callback can be provided for execution upon completion, as demonstrated in the following examples:
     * <p>
     * <pre>
     *     ICompletableFuture&lt;Long&gt; future = atomicLong.addAndGetAsync(13);
     *     // do something else, then read the result
     *     Long result = future.get(); // this method will block until the result is available
     * </pre>
     * </p>
     * <p>
     * <pre>
     *     ICompletableFuture&lt;Long&gt; future = atomicLong.addAndGetAsync(13);
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
     *
     * @param delta the value to add
     * @return an {@link ICompletableFuture} bearing the response
     * @since 3.7
     */
    ICompletableFuture<Long> addAndGetAsync(long delta);

    /**
     * Atomically sets the value to the given updated value
     * only if the current value {@code ==} the expected value.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @param expect the expected value
     * @param update the new value
     * @return an {@link ICompletableFuture} with value true if successful; or false if the actual value
     *         was not equal to the expected value.
     * @since 3.7
     */
    ICompletableFuture<Boolean> compareAndSetAsync(long expect, long update);

    /**
     * Atomically decrements the current value by one.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @return an {@link ICompletableFuture} with the updated value.
     * @since 3.7
     */
    ICompletableFuture<Long> decrementAndGetAsync();

    /**
     * Gets the current value. This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @return an {@link ICompletableFuture} with the current value
     * @since 3.7
     */
    ICompletableFuture<Long> getAsync();

    /**
     * Atomically adds the given value to the current value.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @param delta the value to add
     * @return an {@link ICompletableFuture} with the old value before the addition
     * @since 3.7
     */
    ICompletableFuture<Long> getAndAddAsync(long delta);

    /**
     * Atomically sets the given value and returns the old value.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @param newValue the new value
     * @return an {@link ICompletableFuture} with the old value
     * @since 3.7
     */
    ICompletableFuture<Long> getAndSetAsync(long newValue);

    /**
     * Atomically increments the current value by one.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @return an {@link ICompletableFuture} with the updated value
     * @since 3.7
     */
    ICompletableFuture<Long> incrementAndGetAsync();

    /**
     * Atomically increments the current value by one.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @return an {@link ICompletableFuture} with the old value
     * @since 3.7
     */
    ICompletableFuture<Long> getAndIncrementAsync();

    /**
     * Atomically sets the given value.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @param newValue the new value
     * @return an {@link ICompletableFuture} API consumers can use to track execution of this request
     * @since 3.7
     */
    ICompletableFuture<Void> setAsync(long newValue);

    /**
     * Alters the currently stored value by applying a function on it.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @param function the function
     * @throws IllegalArgumentException if function is null.
     * @return an {@link ICompletableFuture} API consumers can use to track execution of this request
     * @since 3.7
     */
    ICompletableFuture<Void> alterAsync(IFunction<Long, Long> function);

    /**
     * Alters the currently stored value by applying a function on it and gets the result.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @param function the function
     * @return an {@link ICompletableFuture} with the new value.
     * @throws IllegalArgumentException if function is null.
     * @since 3.7
     */
    ICompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function);

    /**
     * Alters the currently stored value by applying a function on it on and gets the old value.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     *
     * @param function the function
     * @return an {@link ICompletableFuture} with the old value
     * @throws IllegalArgumentException if function is null.
     * @since 3.7
     */
    ICompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function);

    /**
     * Applies a function on the value, the actual stored value will not change.
     * This method will dispatch a request and return immediately an {@link ICompletableFuture}.
     * Example:
     * <p>
     * <pre>
     *     class IsOneFunction implements IFunction&lt;Long, Boolean&gt; {
     *       &#64;Override
     *       public Boolean apply(Long input) {
     *         return input.equals(1L);
     *       }
     *     }
     *
     *     ICompletableFuture<Boolean> future = atomicLong.applyAsync(new IsOneFunction());
     *     future.andThen(new ExecutionCallback&lt;Boolean&gt;() {
     *        void onResponse(Boolean response) {
     *            // do something with the response
     *        }
     *
     *        void onFailure(Throwable t) {
     *            // handle failure
     *        }
     *     });
     * </pre>
     * </p>
     *
     * @param function the function
     * @return an {@link ICompletableFuture} with the result of the function application
     * @throws IllegalArgumentException if function is null.
     * @since 3.7
     */
    <R> ICompletableFuture<R> applyAsync(IFunction<Long, R> function);
}
