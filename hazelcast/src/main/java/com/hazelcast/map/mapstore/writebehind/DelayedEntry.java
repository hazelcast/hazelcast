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

package com.hazelcast.map.mapstore.writebehind;

/**
 * Immutable store entry for delayed store operations.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
public final class DelayedEntry<K, V> extends AbstractDelayedEntry<K> {

    private final V value;

    private DelayedEntry(K key, V value, long storeTime, int partitionId) {
        super(key, storeTime, partitionId);
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    public static <K, V> DelayedEntry<K, V> create(K key, V value, long storeTime, int partitionId) {
        return new DelayedEntry<K, V>(key, value, storeTime, partitionId);
    }

    public static <K, V> DelayedEntry<K, V> create(K key, V value, long storeTime) {
        return new DelayedEntry<K, V>(key, value, storeTime, -1);
    }

    /**
     * Used to put staging area.
     *
     * @param value     to put.
     * @param storeTime target store time
     * @param <K>       the key type.
     * @param <V>       the value type.
     * @return new delayed entry object with a null key.
     * @see WriteBehindStore#evictionStagingArea
     */
    public static <K, V> DelayedEntry<K, V> createWithNullKey(V value, long storeTime) {
        return new DelayedEntry<K, V>(null, value, storeTime, 0);
    }

    /**
     * Used to removal operations from map store.
     *
     * @param key       to put.
     * @param storeTime target store time
     * @param <K>       the key type.
     * @param <V>       the value type.
     * @return new delayed entry object with a null key.
     */
    public static <K, V> DelayedEntry<K, V> createWithNullValue(K key, long storeTime, int partitionId) {
        return new DelayedEntry<K, V>(key, null, storeTime, partitionId);
    }

    /**
     * Used for tests.
     */
    public static <K, V> DelayedEntry<K, V> createEmpty() {
        return new DelayedEntry<K, V>(null, null, 0L, 0);
    }

    @Override
    public String toString() {
        return "DelayedEntry{"
                + "key=" + key
                + ", value=" + value
                + ", storeTime=" + storeTime
                + ", partitionId=" + getPartitionId()
                + '}';
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (value == null ? 0 : value.hashCode());
        return result;
    }
}
