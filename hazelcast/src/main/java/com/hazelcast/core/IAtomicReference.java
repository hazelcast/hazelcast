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
 * IAtomicReference is a redundant and highly available distributed alternative
 * to the {@link java.util.concurrent.atomic.AtomicReference}.
 * <p>
 * Asynchronous variants have been introduced in version 3.7.
 * Async methods return immediately an {@link ICompletableFuture} from which
 * the operation's result can be obtained either in a blocking manner or by
 * registering a callback to be executed upon completion. For example:
 * <pre><code>
 * ICompletableFuture<E> future = atomicRef.getAsync();
 * future.andThen(new ExecutionCallback<E>() {
 *     void onResponse(Long response) {
 *         // do something with the result
 *     }
 *
 *     void onFailure(Throwable t) {
 *         // handle failure
 *     }
 * });
 * </code></pre>
 * During a network partition event it is possible for the
 * {@link IAtomicReference} to exist in each of the partitioned clusters or to
 * not exist at all. Under these circumstances the values held in the
 * {@link IAtomicReference} may diverge. Once the network partition heals,
 * Hazelcast will use the configured split-brain merge policy to resolve
 * conflicting values.
 * <p>
 * Supports Quorum {@link com.hazelcast.config.QuorumConfig} since 3.10 in
 * cluster versions 3.10 and higher.
 *
 * @param <E> the type of object referred to by this reference
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
     * @return {@code true} if successful; or {@code false} if the actual value
     * was not equal to the expected value
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
     * @param newValue the new value
     * @return the old value
     */
    E getAndSet(E newValue);

    /**
     * Sets and gets the value.
     *
     * @param update the new value
     * @return the new value
     * @deprecated will be removed from Hazelcast 3.4 since it doesn't really serve a purpose
     */
    E setAndGet(E update);

    /**
     * Checks if the stored reference is {@code null}.
     *
     * @return {@code true} if {@code null}, {@code false} otherwise
     */
    boolean isNull();

    /**
     * Clears the current stored reference.
     */
    void clear();

    /**
     * Checks if the reference contains the value.
     *
     * @param value the value to check (is allowed to be {@code null})
     * @return {@code true} if the value is found, {@code false} otherwise
     */
    boolean contains(E value);

    /**
     * Alters the currently stored reference by applying a function on it.
     *
     * @param function the function that alters the currently stored reference
     * @throws IllegalArgumentException if function is {@code null}
     */
    void alter(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it and
     * gets the result.
     *
     * @param function the function that alters the currently stored reference
     * @return the new value, the result of the applied function
     * @throws IllegalArgumentException if function is {@code null}
     */
    E alterAndGet(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it on
     * and gets the old value.
     *
     * @param function the function that alters the currently stored reference
     * @return the old value, the value before the function is applied
     * @throws IllegalArgumentException if function is {@code null}
     */
    E getAndAlter(IFunction<E, E> function);

    /**
     * Applies a function on the value, the actual stored value will not
     * change.
     *
     * @param function the function applied on the value, the stored value does not change
     * @return the result of the function application
     * @throws IllegalArgumentException if function is {@code null}
     */
    <R> R apply(IFunction<E, R> function);

    /**
     * Atomically sets the value to the given updated value only if the
     * current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} if successful; or {@code false} if the actual value
     * was not equal to the expected value
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
     * @param newValue the new value
     * @return the old value
     */
    ICompletableFuture<E> getAndSetAsync(E newValue);

    /**
     * Checks if the stored reference is {@code null}.
     *
     * @return {@code true} if {@code null}, {@code false} otherwise
     */
    ICompletableFuture<Boolean> isNullAsync();

    /**
     * Clears the current stored reference.
     */
    ICompletableFuture<Void> clearAsync();

    /**
     * Checks if the reference contains the value.
     *
     * @param expected the value to check (is allowed to be null)
     * @return {@code true} if the value is found, {@code false} otherwise
     */
    ICompletableFuture<Boolean> containsAsync(E expected);

    /**
     * Alters the currently stored reference by applying a function on it.
     *
     * @param function the function
     * @throws IllegalArgumentException if function is {@code null}
     */
    ICompletableFuture<Void> alterAsync(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it and
     * gets the result.
     *
     * @param function the function
     * @return the new value
     * @throws IllegalArgumentException if function is {@code null}
     */
    ICompletableFuture<E> alterAndGetAsync(IFunction<E, E> function);

    /**
     * Alters the currently stored reference by applying a function on it on
     * and gets the old value.
     *
     * @param function the function
     * @return the old value
     * @throws IllegalArgumentException if function is {@code null}
     */
    ICompletableFuture<E> getAndAlterAsync(IFunction<E, E> function);

    /**
     * Applies a function on the value, the actual stored value will not
     * change.
     *
     * @param function the function
     * @return the result of the function application
     * @throws IllegalArgumentException if function is {@code null}
     */
    <R> ICompletableFuture<R> applyAsync(IFunction<E, R> function);
}
