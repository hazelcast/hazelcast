package com.hazelcast.spi.impl.reactor;

public final class CircularQueue<E> {

    private long head;
    private long tail = -1;
    private final E[] array;
    private final int mask;
    private final int capacity;

    public CircularQueue(int capacity) {
        this.array = (E[]) new Object[capacity];
        this.capacity = capacity;
        this.mask = capacity - 1;
    }

    public boolean enqueue(E item) {
        if (tail - head + 1 == capacity) {
            return false;
        }

        long t = tail + 1;
        int index = (int) (t & mask);
        array[index] = item;
        this.tail = t;
        return true;
    }

    public E dequeue() {
        if (tail < head) {
            return null;
        }

        long h = head;
        int index = (int) (h & mask);
        E item = array[index];
        array[index] = null;
        this.head = h + 1;
        return item;
    }

    public boolean isEmpty() {
        return tail < head;
    }
}
