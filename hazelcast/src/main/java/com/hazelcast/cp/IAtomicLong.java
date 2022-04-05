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

package com.hazelcast.cp;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IFunction;

import java.util.concurrent.CompletionStage;

/**
 * IAtomicLong is a redundant and highly available distributed alternative to
 * the {@link java.util.concurrent.atomic.AtomicLong}.
 * <p>
 * Asynchronous variants of all methods have been introduced in version 3.7.
 * Async methods immediately return a {@link CompletionStage} which can be used to
 * chain further computation stages or can be converted to a {@code CompletableFuture}
 * from which the operation's result can be obtained in a blocking manner. For example:
 * <pre>
 * CompletionStage&lt;Long&gt; stage = atomicLong.addAndGetAsync(13);
 * stage.whenCompleteAsync((response, t) -&gt; {
 *     if (t == null) {
 *         // do something with the result
 *     } else {
 *         // handle failure
 *     }
 * });
 * </pre>
 * <p>
 * Actions supplied for dependent completions of default non-async methods and async methods
 * without an explicit {@link java.util.concurrent.Executor} argument are performed
 * by the {@link java.util.concurrent.ForkJoinPool#commonPool()} (unless it does not
 * support a parallelism level of at least 2, in which case a new {@code Thread} is
 * created per task).
 * <p>
 * IAtomicLong is accessed via {@link CPSubsystem#getAtomicLong(String)}.
 * It works on top of the Raft consensus algorithm. It offers linearizability during crash
 * failures and network partitions. It is CP with respect to the CAP principle.
 * If a network partition occurs, it remains available on at most one side
 * of the partition.
 * <p>
 * IAtomicLong impl does not offer exactly-once / effectively-once
 * execution semantics. It goes with at-least-once execution semantics
 * by default and can cause an API call to be committed multiple times
 * in case of CP member failures. It can be tuned to offer at-most-once
 * execution semantics. Please see
 * {@link CPSubsystemConfig#setFailOnIndeterminateOperationState(boolean)}
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
     * @return {@code true} if successful; or {@code false} if the actual value
     * was not equal to the expected value.
     */
    boolean compareAndSet(long expect, long update);

    /**
     * Atomically decrements the current value by one.
     *
     * @return the updated value, the current value decremented by one
     */
    long decrementAndGet();

    /**
     * Atomically decrements the current value by one.
     *
     * @return the old value
     */
    long getAndDecrement();

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
     * @throws IllegalArgumentException if function is {@code null}
     * @since 3.2
     */
    void alter(IFunction<Long, Long> function);

    /**
     * Alters the currently stored value by applying a function on it and
     * gets the result.
     *
     * @param function the function applied to the currently stored value
     * @return the new value
     * @throws IllegalArgumentException if function is {@code null}
     * @since 3.2
     */
    long alterAndGet(IFunction<Long, Long> function);

    /**
     * Alters the currently stored value by applying a function on it on and
     * gets the old value.
     *
     * @param function the function applied to the currently stored value
     * @return the old value
     * @throws IllegalArgumentException if function is {@code null}
     * @since 3.2
     */
    long getAndAlter(IFunction<Long, Long> function);

    /**
     * Applies a function on the value, the actual stored value will not change.
     *
     * @param function the function applied to the value, the value is not changed
     * @param <R> the result type of the function
     * @return the result of the function application
     * @throws IllegalArgumentException if function is {@code null}
     * @since 3.2
     */
    <R> R apply(IFunction<Long, R> function);

    /**
     * Atomically adds the given value to the current value.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     * <p>
     * The operations result can be obtained in a blocking way, or a callback
     * can be provided for execution upon completion, as demonstrated in the
     * following examples:
     * <pre>
     * CompletionStage&lt;Long&gt; stage = atomicLong.addAndGetAsync(13);
     * // do something else, then read the result
     *
     * // this method will block until the result is available
     * Long result = stage.toCompletableFuture().get();
     * </pre>
     * <pre>
     * CompletionStage&lt;Long&gt; stage = atomicLong.addAndGetAsync(13);
     * stage.whenCompleteAsync((response, t) -&gt; {
     *     if (t == null) {
     *         // do something with the result
     *     } else {
     *         // handle failure
     *     }
     * });
     * </pre>
     *
     * @param delta the value to add
     * @return a {@link CompletionStage} bearing the response
     * @since 3.7
     */
    CompletionStage<Long> addAndGetAsync(long delta);

    /**
     * Atomically sets the value to the given updated value
     * only if the current value {@code ==} the expected value.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @param expect the expected value
     * @param update the new value
     * @return an {@link CompletionStage} with value {@code true} if successful;
     * or {@code false} if the actual value was not equal to the expected value
     * @since 3.7
     */
    CompletionStage<Boolean> compareAndSetAsync(long expect, long update);

    /**
     * Atomically decrements the current value by one.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @return a {@link CompletionStage} with the updated value
     * @since 3.7
     */
    CompletionStage<Long> decrementAndGetAsync();

    /**
     * Atomically decrements the current value by one.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @return a {@link CompletionStage} with the old value
     * @since 4.1
     */
    CompletionStage<Long> getAndDecrementAsync();

    /**
     * Gets the current value. This method will dispatch a request and return
     * immediately a {@link CompletionStage}.
     *
     * @return a {@link CompletionStage} with the current value
     * @since 3.7
     */
    CompletionStage<Long> getAsync();

    /**
     * Atomically adds the given value to the current value.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @param delta the value to add
     * @return a {@link CompletionStage} with the old value before the addition
     * @since 3.7
     */
    CompletionStage<Long> getAndAddAsync(long delta);

    /**
     * Atomically sets the given value and returns the old value.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @param newValue the new value
     * @return a {@link CompletionStage} with the old value
     * @since 3.7
     */
    CompletionStage<Long> getAndSetAsync(long newValue);

    /**
     * Atomically increments the current value by one.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @return a {@link CompletionStage} with the updated value
     * @since 3.7
     */
    CompletionStage<Long> incrementAndGetAsync();

    /**
     * Atomically increments the current value by one.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @return a {@link CompletionStage} with the old value
     * @since 3.7
     */
    CompletionStage<Long> getAndIncrementAsync();

    /**
     * Atomically sets the given value.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @param newValue the new value
     * @return a {@link CompletionStage}
     * @since 3.7
     */
    CompletionStage<Void> setAsync(long newValue);

    /**
     * Alters the currently stored value by applying a function on it.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @param function the function
     * @return a {@link CompletionStage} with the new value
     * @throws IllegalArgumentException if function is {@code null}
     * @since 3.7
     */
    CompletionStage<Void> alterAsync(IFunction<Long, Long> function);

    /**
     * Alters the currently stored value by applying a function on it and gets
     * the result.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @param function the function
     * @return a {@link CompletionStage} with the new value
     * @throws IllegalArgumentException if function is {@code null}
     * @since 3.7
     */
    CompletionStage<Long> alterAndGetAsync(IFunction<Long, Long> function);

    /**
     * Alters the currently stored value by applying a function on it on and
     * gets the old value.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}.
     *
     * @param function the function
     * @return a {@link CompletionStage} with the old value
     * @throws IllegalArgumentException if function is {@code null}
     * @since 3.7
     */
    CompletionStage<Long> getAndAlterAsync(IFunction<Long, Long> function);

    /**
     * Applies a function on the value, the actual stored value will not
     * change.
     * <p>
     * This method will dispatch a request and return immediately a
     * {@link CompletionStage}. For example:
     * <pre>
     * class IsOneFunction implements IFunction&lt;Long, Boolean&gt; {
     *     &#64;Override
     *     public Boolean apply(Long input) {
     *         return input.equals(1L);
     *     }
     * }
     *
     * CompletionStage&lt;Boolean&gt; stage = atomicLong.applyAsync(new IsOneFunction());
     * stage.whenCompleteAsync((response, t) -&gt; {
     *    if (t == null) {
     *        // do something with the response
     *    } else {
     *       // handle failure
     *    }
     * });
     * </pre>
     *
     * @param function the function
     * @param <R> the result type of the function
     * @return a {@link CompletionStage} with the result of the function application
     * @throws IllegalArgumentException if function is {@code null}
     * @since 3.7
     */
    <R> CompletionStage<R> applyAsync(IFunction<Long, R> function);
}
