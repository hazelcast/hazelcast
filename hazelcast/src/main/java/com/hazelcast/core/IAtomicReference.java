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


/**
 * IAtomicReference is a redundant and highly available distributed alternative to the
 * {@link java.util.concurrent.atomic.AtomicReference java.util.concurrent.atomic.AtomicReference}.
 *
 * Asynchronous variants have been introduced in version 3.7.
 * Async methods return immediately an {@link ICompletableFuture} from which the operation's result
 * can be obtained either in a blocking manner or by registering a callback to be executed
 * upon completion. For example:
 *
 * <p>
 * <pre>
 *     ICompletableFuture&lt;E&gt; future = atomicRef.getAsync();
 *     future.andThen(new ExecutionCallback&lt;E&gt;() {
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
     * Gets the old value and sets the new value.
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
     * @return true if the value is found, false otherwise.
     */
    boolean contains(E value);

    /**
     * Alters the currently stored reference by applying a function on it.
     *
     * @param function the function that alters the currently stored reference
     * @throws IllegalArgumentException if function is null.
     */
    void alter(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it and gets the result.
     *
     * @param function the function that alters the currently stored reference
     * @return the new value, the result of the applied function.
     * @throws IllegalArgumentException if function is null.
     */
    E alterAndGet(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it on and gets the old value.
     *
     * @param function the function that alters the currently stored reference
     * @return  the old value, the value before the function is applied
     * @throws IllegalArgumentException if function is null.
     */
    E getAndAlter(IFunction<E, E> function);

    /**
     * Applies a function on the value, the actual stored value will not change.
     *
     * @param function the function applied on the value, the stored value does not change
     * @return  the result of the function application
     * @throws IllegalArgumentException if function is null.
     */
    <R> R apply(IFunction<E, R> function);

    /**
     * Atomically sets the value to the given updated value
     * only if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return true if successful; or false if the actual value
     *         was not equal to the expected value.
     */
    ICompletableFuture<Boolean> compareAndSetAsync(E expect, E update);

    /**
     * Gets the current value.
     *
     * @return the current value
     */
    ICompletableFuture<E> getAsync();

    /**
     * Atomically sets the given value.
     *
     * @param newValue the new value
     */
    ICompletableFuture<Void> setAsync(E newValue);

    /**
     * Gets the value and sets the new value.
     *
     * @param newValue the new value.
     * @return the old value.
     */
    ICompletableFuture<E> getAndSetAsync(E newValue);

    /**
     * Checks if the stored reference is null.
     *
     * @return true if null, false otherwise.
     */
    ICompletableFuture<Boolean> isNullAsync();

    /**
     * Clears the current stored reference.
     */
    ICompletableFuture<Void> clearAsync();

    /**
     * Checks if the reference contains the value.
     *
     * @param expected the value to check (is allowed to be null).
     * @return true if the value is found, false otherwise.
     */
    ICompletableFuture<Boolean> containsAsync(E expected);

    /**
     * Alters the currently stored reference by applying a function on it.
     *
     * @param function the function
     * @throws IllegalArgumentException if function is null.
     */
    ICompletableFuture<Void> alterAsync(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it and gets the result.
     *
     * @param function the function
     * @return the new value.
     * @throws IllegalArgumentException if function is null.
     */
    ICompletableFuture<E> alterAndGetAsync(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it on and gets the old value.
     *
     * @param function the function
     * @return  the old value
     * @throws IllegalArgumentException if function is null.
     */
    ICompletableFuture<E> getAndAlterAsync(IFunction<E, E> function);

    /**
     * Applies a function on the value, the actual stored value will not change.
     *
     * @param function the function
     * @return  the result of the function application
     * @throws IllegalArgumentException if function is null.
     */
    <R> ICompletableFuture<R> applyAsync(IFunction<E, R> function);
}
