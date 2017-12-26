package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class PFloat implements Serializable {
    public float field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PFloat pFloat = (PFloat) o;

        return Float.compare(pFloat.field, field) == 0;
    }

    @Override
    public int hashCode() {
        return (field != +0.0f ? Float.floatToIntBits(field) : 0);
    }

    @Override
    public String toString() {
        return "" + field;
    }
}
