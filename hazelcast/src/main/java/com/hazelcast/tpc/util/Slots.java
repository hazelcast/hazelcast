package com.hazelcast.tpc.util;

public final class Slots<E> {

    private int[] tags;
    private E[] items;
    private int pos;

    public Slots(int size) {
        tags = new int[size];
        items = (E[]) new Object[size];

        for (int k = 0; k < size; k++) {
            tags[k] = k;
        }
        pos = size;
    }

    public int insert(E item) {
        if(pos == 0){
            throw new RuntimeException();
        }

        pos--;
        int tag = tags[pos];
        items[tag] = item;
        return tag;
    }

    public E remove(int tag) {
        E item = items[tag];
        items[tag] = null;
        tags[pos] = tag;
        pos++;
        return item;
    }
}
