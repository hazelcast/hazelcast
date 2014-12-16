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

/**
 * IAtomicLong is a redundant and highly available distributed alternative to the
 * {@link java.util.concurrent.atomic.AtomicLong java.util.concurrent.atomic.AtomicLong}.
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
}
