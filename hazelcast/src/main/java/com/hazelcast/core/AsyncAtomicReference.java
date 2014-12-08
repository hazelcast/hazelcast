package com.hazelcast.core;

import com.hazelcast.spi.annotation.Beta;

/**
 * A {@link IAtomicReference} that exposes its operations using a {@link ICompletableFuture}
 * so it can be used in the reactive programming model approach.
 *
 * @since 3.2
 */
@Beta
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
