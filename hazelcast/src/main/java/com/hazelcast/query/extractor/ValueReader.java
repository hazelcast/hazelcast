package com.hazelcast.query.extractor;

public abstract class ValueReader<T> {

    public abstract void read(String path, ValueCollector<T> collector);

    public abstract void read(String path, ValueCollector<T> collector, ValueFilter<T> filter);

}
