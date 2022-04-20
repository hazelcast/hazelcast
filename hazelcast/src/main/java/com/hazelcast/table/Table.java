package com.hazelcast.table;

public interface Table<K,E> {

    Pipeline newPipeline();

    void upsert(E item);

    void upsertAll(E[] items);

    void set(byte[] key, byte[] value);

    byte[] get(byte[] key);

    void noop();

    void concurrentNoop(int concurrency);
}
