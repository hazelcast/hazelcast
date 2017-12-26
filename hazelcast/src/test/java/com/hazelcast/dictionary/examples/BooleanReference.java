package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class BooleanReference implements Serializable {
    public Boolean field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BooleanReference that = (BooleanReference) o;
        if(field==null){
            return that.field==null;
        }

        return field.equals(that.field);
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
