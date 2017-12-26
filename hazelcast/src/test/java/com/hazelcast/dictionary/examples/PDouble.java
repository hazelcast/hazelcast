package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class PDouble implements Serializable {
    public double field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PDouble pDouble = (PDouble) o;

        return Double.compare(pDouble.field, field) == 0;
    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits(field);
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public String toString() {
        return "" + field;
    }
}
