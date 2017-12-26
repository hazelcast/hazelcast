package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class PInt implements Serializable {
    public int field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PInt pInt = (PInt) o;

        return field == pInt.field;
    }

    @Override
    public int hashCode() {
        return field;
    }

    @Override
    public String toString() {
        return "" + field;
    }
}
