package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class PChar implements Serializable {
    public char field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PChar pChar = (PChar) o;

        return field == pChar.field;
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
