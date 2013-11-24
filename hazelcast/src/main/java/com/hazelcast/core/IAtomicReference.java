package com.hazelcast.core;

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

    E getAndSet(E newValue);

    E setAndGet(E update);

    boolean isNull();

    void clear();

    boolean contains(E value);

    void alter(Function<E, E> function);

    E alterAndGet(Function<E, E> function);

    E getAndAlter(Function<E, E> function);

    <R> R apply(Function<E, R> function);
}
