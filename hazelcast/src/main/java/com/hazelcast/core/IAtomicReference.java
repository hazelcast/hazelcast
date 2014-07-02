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
 * IAtomicReference is a redundant and highly available distributed alternative to the
 * {@link java.util.concurrent.atomic.AtomicReference java.util.concurrent.atomic.AtomicReference}.
 *
 * @see IAtomicLong
 * @since 3.2
 */
public interface IAtomicReference<E> extends DistributedObject {

    /**
     * Atomically sets the value to the given updated value
     * only if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return true if successful; or false if the actual value
     *         was not equal to the expected value.
     */
    boolean compareAndSet(E expect, E update);

    /**
     * Gets the current value.
     *
     * @return the current value
     */
    E get();

    /**
     * Atomically sets the given value.
     *
     * @param newValue the new value
     */
    void set(E newValue);

    /**
     * Gets the value and sets the new value.
     *
     * @param newValue the new value.
     * @return the old value.
     */
    E getAndSet(E newValue);

    /**
     * Sets and gets the value.
     *
     * @param update the new value
     * @return  the new value
     * @deprecated will be removed from Hazelcast 3.4 since it doesn't really serve a purpose.
     */
    E setAndGet(E update);

    /**
     * Checks if the stored reference is null.
     *
     * @return true if null, false otherwise.
     */
    boolean isNull();

    /**
     * Clears the current stored reference.
     */
    void clear();

    /**
     * Checks if the reference contains the value.
     *
     * @param value the value to check (is allowed to be null).
     * @return if the value is found, false otherwise.
     */
    boolean contains(E value);

    /**
     * Alters the currently stored reference by applying a function on it.
     *
     * @param function the function
     * @throws IllegalArgumentException if function is null.
     */
    void alter(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it and gets the result.
     *
     * @param function the function
     * @return the new value.
     * @throws IllegalArgumentException if function is null.
     */
    E alterAndGet(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it on and gets the old value.
     *
     * @param function the function
     * @return  the old value
     * @throws IllegalArgumentException if function is null.
     */
    E getAndAlter(IFunction<E, E> function);

    /**
     * Applies a function on the value, the actual stored value will not change.
     *
     * @param function the function
     * @return  the result of the function application
     * @throws IllegalArgumentException if function is null.
     */
    <R> R apply(IFunction<E, R> function);
}
