package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class PShort implements Serializable {
    public short field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PShort pShort = (PShort) o;

        return field == pShort.field;
    }

    @Override
    public int hashCode() {
        return (int) field;
    }

    @Override
    public String toString() {
        return "" + field;
    }
}
