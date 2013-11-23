package com.hazelcast.core;

public interface AsyncAtomicLong extends IAtomicLong{

    /**
     * Atomically sets the given value and returns the old value.
     *
     * @param newValue the new value
     * @return the old value
     */
    CompletionFuture<Long> asyncGetAndSet(long newValue);

   /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    CompletionFuture<Long> asyncAddAndGet(long delta);

    /**
     * Atomically sets the value to the given updated value
     * only if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return true if successful; or false if the actual value
     *         was not equal to the expected value.
     */
    CompletionFuture<Boolean> asyncCompareAndSet(long expect, long update);

    /**
     * Atomically decrements the current value by one.
     *
     * @return the updated value
     */
    CompletionFuture<Long> asyncDecrementAndGet();

    /**
     * Gets the current value.
     *
     * @return the current value
     */
    CompletionFuture<Long> asyncGet();

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the old value before the add
     */
    CompletionFuture<Long> asyncGetAndAdd(long delta);

    /**
     * Atomically increments the current value by one.
     *
     * @return the updated value
     */
    CompletionFuture<Long> asyncIncrementAndGet();

    /**
     * Atomically increments the current value by one.
     *
     * @return the old value
     */
    CompletionFuture<Long> asyncGetAndIncrement();

    /**
     * Atomically sets the given value.
     *
     * @param newValue the new value
     */
    CompletionFuture<Void> asyncSet(long newValue);
}
