package com.hazelcast.query.extractor;

public abstract class ValueFilter<T> {

    public boolean apply(T value) {
        return true;
    }

}
