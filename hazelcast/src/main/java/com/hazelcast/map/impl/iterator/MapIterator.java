package com.hazelcast.map.impl.iterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MapIterator<R> implements Iterator<R> {

    private final int size;
    private Iterator<R> it;
    private int idx = 0;
    private final List<Iterator<R>> partitionIterators;

    public MapIterator(List<Iterator<R>> partitionIterators) {
        this.partitionIterators = partitionIterators;
        this.size = partitionIterators.size();
        it = partitionIterators.get(idx);
    }

    @Override
    public R next() {
        if (it.hasNext()) {
            return it.next();
        }
        throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() {
        while (!it.hasNext()) {
            if (idx == size - 1) {
                return false;
            }
            it = partitionIterators.get(++idx);
        }
        return true;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing when iterating map is not supported");
    }
}
