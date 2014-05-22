package com.hazelcast.map;

import com.hazelcast.core.EntryView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Contains only key no value.
 *
 * @param <K>
 * @param <V>
 */
class NullEntryView<K,V> implements EntryView<K,V>, IdentifiedDataSerializable {

    private final K key;

    NullEntryView(K key) {
        this.key = key;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return null;
    }

    @Override
    public long getCost() {
        return 0;
    }

    @Override
    public long getCreationTime() {
        return 0;
    }

    @Override
    public long getExpirationTime() {
        return 0;
    }

    @Override
    public long getHits() {
        return 0;
    }

    @Override
    public long getLastAccessTime() {
        return 0;
    }

    @Override
    public long getLastStoredTime() {
        return 0;
    }

    @Override
    public long getLastUpdateTime() {
        return 0;
    }

    @Override
    public long getVersion() {
        return 0;
    }

    @Override
    public int getFactoryId() {
        return 0;
    }

    @Override
    public int getId() {
        return 0;
    }
}
