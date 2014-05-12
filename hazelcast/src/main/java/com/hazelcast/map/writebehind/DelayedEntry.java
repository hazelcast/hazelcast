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

package com.hazelcast.map.writebehind;

/**
 * Immutable store entry for delayed store operations.
 *
 * @param <K>
 * @param <V>
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

    /**
     * Used for tests.
     */
    public static <K, V> DelayedEntry<K, V> createEmpty() {
        return new DelayedEntry<K, V>(null, null, 0, 0);
    }


    @Override
    public String toString() {
        return String.format("DelayedEntry={key:%s, value:%s}", this.getKey(), this.getValue());
    }

}
