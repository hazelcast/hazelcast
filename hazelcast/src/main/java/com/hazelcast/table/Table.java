package com.hazelcast.table;

/**
 * This API contains a lot of functionality that normally would be placed
 * over different APIs. But I don't want to jump to a more appropriate solution
 * yet.
 *
 * @param <K>
 * @param <E>
 */
public interface Table<K,E> {

    Pipeline newPipeline();

    void upsert(E item);

    void upsertAll(E[] items);

    void set(byte[] key, byte[] value);

    byte[] get(byte[] key);

    void bogusQuery();

    void noop();

    void concurrentNoop(int concurrency);
}
