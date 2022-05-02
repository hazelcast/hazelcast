package com.hazelcast.tpc.engine;

import java.util.function.Supplier;

import static com.hazelcast.internal.util.Preconditions.*;

public final class SlabAllocator<E> {

    private final Supplier<E> supplier;
    private E[] array;
    private int index = -1;

    public SlabAllocator(int capacity, Supplier<E> supplier) {
        this.array = (E[]) new Object[capacity];
        this.supplier = checkNotNull(supplier);
    }

    public E allocate() {
        if (index == -1) {
            return supplier.get();
        }

        E object = array[index];
        array[index] = null;
        index--;
        return object;
    }

    public void free(E e) {
        checkNotNull(e);

        if (index <= array.length - 1) {
            index++;
            array[index] = e;
        }
    }
}
