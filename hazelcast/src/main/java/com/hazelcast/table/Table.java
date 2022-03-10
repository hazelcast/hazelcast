package com.hazelcast.table;

public interface Table<K,E> {

    void upsert(E item);

    void selectByKey(K key);
}
