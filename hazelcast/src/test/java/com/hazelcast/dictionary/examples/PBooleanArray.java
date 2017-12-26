package com.hazelcast.dictionary.examples;

import java.io.Serializable;
import java.util.Arrays;

public class PBooleanArray implements Serializable {
    public boolean[] field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PBooleanArray that = (PBooleanArray) o;

        return Arrays.equals(field, that.field);
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
