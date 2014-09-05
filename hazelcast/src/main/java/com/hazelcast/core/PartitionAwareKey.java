/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
public final class PartitionAwareKey<K, P> implements PartitionAware<Object>, DataSerializable {

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
        this.key = ValidationUtil.isNotNull(key, "key");
        this.partitionKey = ValidationUtil.isNotNull(partitionKey, "partitionKey");
    }

    //constructor needed for deserialization.
    private PartitionAwareKey() {
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
        if (this == thatObject) {
            return true;
        }
        if (thatObject == null || getClass() != thatObject.getClass()) {
            return false;
        }
        PartitionAwareKey that = (PartitionAwareKey) thatObject;
        return (key.equals(that.key) && partitionKey.equals(that.partitionKey));
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + partitionKey.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PartitionAwareKey{"
                + "key=" + key
                + ", partitionKey=" + partitionKey
                + '}';
    }
}
