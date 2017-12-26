package com.hazelcast.dictionary.examples;

import java.io.Serializable;
import java.util.Arrays;

public class PIntArray implements Serializable {
    public int[] field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PIntArray pIntArray = (PIntArray) o;

        return Arrays.equals(field, pIntArray.field);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(field);
    }

    @Override
    public String toString() {
        return Arrays.toString(field);
    }
}
