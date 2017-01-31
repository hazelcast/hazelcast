/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * A {@link IAtomicReference} that exposes its operations using a {@link ICompletableFuture}
 * so it can be used in the reactive programming model approach.
 *
 * Instead of this interface, use the equivalent async methods in the public {@link IAtomicReference} interface.
 * This interface has been deprecated and will be removed in a future version.
 *
 * @since 3.2
 */
@Beta
@Deprecated
public interface AsyncAtomicReference<E> extends IAtomicReference<E> {

    /**
     * Atomically sets the value to the given updated value
     * only if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return true if successful; or false if the actual value
     *         was not equal to the expected value.
     */
    ICompletableFuture<Boolean> asyncCompareAndSet(E expect, E update);

    /**
     * Gets the current value.
     *
     * @return the current value
     */
    ICompletableFuture<E> asyncGet();

    /**
     * Atomically sets the given value.
     *
     * @param newValue the new value
     */
    ICompletableFuture<Void> asyncSet(E newValue);

    /**
     * Gets the value and sets the new value.
     *
     * @param newValue the new value.
     * @return the old value.
     */
    ICompletableFuture<E> asyncGetAndSet(E newValue);

    /**
     * Sets and gets the value.
     *
     * @param update the new value
     * @return  the new value
     * @deprecated will be removed from Hazelcast 3.4 since it doesn't really serve a purpose.
     */
    ICompletableFuture<E> asyncSetAndGet(E update);

    /**
     * Checks if the stored reference is null.
     *
     * @return true if null, false otherwise.
     */
    ICompletableFuture<Boolean> asyncIsNull();

    /**
     * Clears the current stored reference.
     */
    ICompletableFuture<Void> asyncClear();

    /**
     * Checks if the reference contains the value.
     *
     * @param value the value to check (is allowed to be null).
     * @return true if the value is found, false otherwise.
     */
    ICompletableFuture<Boolean> asyncContains(E value);

    /**
     * Alters the currently stored reference by applying a function on it.
     *
     * @param function the function
     * @throws IllegalArgumentException if function is null.
     */
    ICompletableFuture<Void> asyncAlter(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it and gets the result.
     *
     * @param function the function
     * @return the new value.
     * @throws IllegalArgumentException if function is null.
     */
    ICompletableFuture<E> asyncAlterAndGet(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it on and gets the old value.
     *
     * @param function the function
     * @return  the old value
     * @throws IllegalArgumentException if function is null.
     */
    ICompletableFuture<E> asyncGetAndAlter(IFunction<E, E> function);

    /**
     * Applies a function on the value, the actual stored value will not change.
     *
     * @param function the function
     * @return  the result of the function application
     * @throws IllegalArgumentException if function is null.
     */
    <R> ICompletableFuture<R> asyncApply(IFunction<E, R> function);
}
