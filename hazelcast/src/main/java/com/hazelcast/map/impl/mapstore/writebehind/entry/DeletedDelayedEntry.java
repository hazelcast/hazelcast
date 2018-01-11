/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind.entry;


/**
 * Represents a candidate entry to be deleted from {@link com.hazelcast.core.MapStore}
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
class DeletedDelayedEntry<K, V> implements DelayedEntry<K, V> {

    private final K key;
    private final int partitionId;
    private long storeTime;
    private long sequence;

    public DeletedDelayedEntry(K key, long storeTime, int partitionId) {
        this.key = key;
        this.storeTime = storeTime;
        this.partitionId = partitionId;
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
    public long getStoreTime() {
        return storeTime;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void setStoreTime(long storeTime) {
        this.storeTime = storeTime;
    }

    @Override
    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    /**
     * This method is used when we are cleaning processed instances of this class.
     * Caring only reference equality of objects because wanting exactly remove the same instance
     * otherwise this method should not cause any remove from staging area.
     *
     * @see com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore#removeFromStagingArea
     */
    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "DeletedDelayedEntry{"
                + "key=" + key
                + ", partitionId=" + partitionId
                + ", storeTime=" + storeTime
                + ", sequence=" + sequence
                + '}';
    }
}
