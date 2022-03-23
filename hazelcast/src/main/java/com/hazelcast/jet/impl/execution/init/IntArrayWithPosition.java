package com.hazelcast.jet.impl.execution.init;

import java.util.Arrays;

class IntArrayWithPosition {
    private final int[] array;
    private int pos;

    IntArrayWithPosition(int size) {
        array = new int[size];
    }

    void add(int i) {
        array[pos++] = i;
    }

    int[] getFilledElements() {
        if (pos == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, 0, pos);
    }
}