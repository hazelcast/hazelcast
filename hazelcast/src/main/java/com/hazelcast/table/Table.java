package com.hazelcast.table;

public interface Table<K,E> {

    Pipeline newPipeline();

    void upsert(E item);

    void upsertAll(E[] items);

    void selectByKey(K key);

    void noop();

    void concurrentNoop(int concurrency);
}
