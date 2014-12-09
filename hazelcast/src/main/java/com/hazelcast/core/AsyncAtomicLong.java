/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.annotation.Beta;

/**
 * This interface is an {@link IAtomicLong} interface that exposes its operations using an
 * {@link ICompletableFuture} interface so it can be used in the reactive programming model
 * approach.
 *
 * @since 3.2
 */
@Beta
public interface AsyncAtomicLong extends IAtomicLong {

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    ICompletableFuture<Long> asyncAddAndGet(long delta);

    /**
     * Atomically sets the value to the given updated value
     * only if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return true if successful; or false if the actual value
     *         was not equal to the expected value.
     */
    ICompletableFuture<Boolean> asyncCompareAndSet(long expect, long update);

    /**
     * Atomically decrements the current value by one.
     *
     * @return the updated value
     */
    ICompletableFuture<Long> asyncDecrementAndGet();

    /**
     * Gets the current value.
     *
     * @return the current value
     */
    ICompletableFuture<Long> asyncGet();

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the old value before the add
     */
    ICompletableFuture<Long> asyncGetAndAdd(long delta);

    /**
     * Atomically sets the given value and returns the old value.
     *
     * @param newValue the new value
     * @return the old value
     */
    ICompletableFuture<Long> asyncGetAndSet(long newValue);

    /**
     * Atomically increments the current value by one.
     *
     * @return the updated value
     */
    ICompletableFuture<Long> asyncIncrementAndGet();

    /**
     * Atomically increments the current value by one.
     *
     * @return the old value
     */
    ICompletableFuture<Long> asyncGetAndIncrement();

    /**
     * Atomically sets the given value.
     *
     * @param newValue the new value
     */
    ICompletableFuture<Void> asyncSet(long newValue);

    /**
     * Alters the currently stored value by applying a function on it.
     *
     * @param function the function
     * @throws IllegalArgumentException if function is null.
     * @since 3.2
     */
    ICompletableFuture<Void> asyncAlter(IFunction<Long, Long> function);

    /**
     * Alters the currently stored value by applying a function on it and gets the result.
     *
     * @param function the function
     * @return the new value.
     * @throws IllegalArgumentException if function is null.
     * @since 3.2
     */
    ICompletableFuture<Long> asyncAlterAndGet(IFunction<Long, Long> function);

    /**
     * Alters the currently stored value by applying a function on it on and gets the old value.
     *
     * @param function the function
     * @return  the old value
     * @throws IllegalArgumentException if function is null.
     * @since 3.2
     */
    ICompletableFuture<Long> asyncGetAndAlter(IFunction<Long, Long> function);

    /**
     * Applies a function on the value, the actual stored value will not change.
     *
     * @param function the function
     * @return  the result of the function application
     * @throws IllegalArgumentException if function is null.
     * @since 3.2
     */
    <R> ICompletableFuture<R> asyncApply(IFunction<Long, R> function);
}
