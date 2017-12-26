package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class PBoolean implements Serializable {
    public boolean field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PBoolean pBoolean = (PBoolean) o;

        return field == pBoolean.field;
    }

    @Override
    public int hashCode() {
        return (field ? 1 : 0);
    }

    @Override
    public String toString() {
        return "" + field;
    }
}
