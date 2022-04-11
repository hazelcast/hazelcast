package com.hazelcast.internal.util.collection;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

public class FixedCapacityArrayList<E> extends AbstractList<E> implements List<E> {
    private static final int DEFAULT_CAPACITY = 10;

    private E[] elementData;
    private int size;

    public FixedCapacityArrayList() {
        this(DEFAULT_CAPACITY);
    }

    public FixedCapacityArrayList(int initialCapacity) {
        this.elementData = (E[]) new Object[initialCapacity];
    }

    @Override
    public boolean add(E e) {
        modCount++;
        if (size == elementData.length) {
            grow();
        }
        elementData[size++] = e;
        return true;
    }

    @Override
    public E get(int index) {
        return elementData[index];
    }

    @Override
    public int size() {
        return size;
    }

    public E[] getArray() {
        return elementData;
    }

    private void grow() {
        int newCapacity = elementData.length << 1;
        elementData = Arrays.copyOf(elementData, newCapacity);
    }
}
