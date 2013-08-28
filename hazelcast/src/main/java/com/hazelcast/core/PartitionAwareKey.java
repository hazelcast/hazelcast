package com.hazelcast.core;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.ValidationUtil;

import java.io.IOException;

/**
 * A {@link PartitionAware} key. This is useful in combination with a Map where you want to control the
 * partition of a key.
 *
 * @param <K>
 * @param <P>
 */
public final class PartitionAwareKey<K,P> implements PartitionAware<Object>, DataSerializable {

    private K key;
    private P partitionKey;

    /**
     * Creates a new PartitionAwareKey.
     *
     * @param key the key
     * @param partitionKey the partitionKey
     * @throws IllegalArgumentException if key or partitionKey is null.
     */
    public PartitionAwareKey(K key, P partitionKey) {
        this.key = ValidationUtil.isNotNull(key,"key");
        this.partitionKey = ValidationUtil.isNotNull(partitionKey,"partitionKey");
    }

    public K getKey() {
        return key;
    }

    @Override
    public P getPartitionKey() {
        return partitionKey;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(partitionKey);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.key = in.readObject();
        this.partitionKey = in.readObject();
    }

    @Override
    public boolean equals(Object thatObject) {
        if (this == thatObject) return true;
        if (thatObject == null || getClass() != thatObject.getClass()) return false;

        PartitionAwareKey that = (PartitionAwareKey) thatObject;

        if (!key.equals(that.key)) return false;
        if (!partitionKey.equals(that.partitionKey)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + partitionKey.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PartitionAwareKey{" +
                "key=" + key +
                ", partitionKey=" + partitionKey +
                '}';
    }
}
