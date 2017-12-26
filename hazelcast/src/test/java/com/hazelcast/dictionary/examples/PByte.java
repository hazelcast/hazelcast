package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class PByte implements Serializable {
    public byte field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PByte pByte = (PByte) o;

        return field == pByte.field;
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
