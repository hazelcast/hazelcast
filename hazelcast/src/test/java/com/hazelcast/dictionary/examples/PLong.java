package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class PLong implements Serializable {
    public long field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PLong pLong = (PLong) o;

        return field == pLong.field;
    }

    @Override
    public int hashCode() {
        return (int) (field ^ (field >>> 32));
    }

    @Override
    public String toString() {
        return "" + field;
    }
}
