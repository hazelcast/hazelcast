package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class IntegerReference implements Serializable {

    public Integer field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntegerReference that = (IntegerReference) o;

        return field != null ? field.equals(that.field) : that.field == null;
    }

    @Override
    public int hashCode() {
        return field != null ? field.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "" + field;
    }
}
