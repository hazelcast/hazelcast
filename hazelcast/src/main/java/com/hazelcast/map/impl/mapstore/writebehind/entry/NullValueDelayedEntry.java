/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.EntryLoader;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;

import java.util.Objects;
import java.util.UUID;

/**
 * Only key is set and other values are omitted. Only used to check whether
 * a {@link DelayedEntry} for the key is exist.
 *
 * @param <K> the key type.
 * @param <V> the value type
 * @see WriteBehindStore#flush
 */
class NullValueDelayedEntry<K, V> implements DelayedEntry<K, V> {

    private final K key;

    NullValueDelayedEntry(K key) {
        this.key = key;
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
    public long getExpirationTime() {
        return EntryLoader.MetadataAwareValue.NO_TIME_SET;
    }

    @Override
    public long getStoreTime() {
        return -1L;
    }

    @Override
    public int getPartitionId() {
        return -1;
    }

    @Override
    public void setStoreTime(long storeTime) {
    }

    @Override
    public void setSequence(long sequence) {

    }

    @Override
    public long getSequence() {
        return -1;
    }

    @Override
    public void setTxnId(UUID txnId) {

    }

    @Override
    public UUID getTxnId() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof NullValueDelayedEntry)) {
            return false;
        }

        NullValueDelayedEntry<?, ?> that = (NullValueDelayedEntry<?, ?>) o;
        return Objects.equals(key, that.key);

    }

    @Override
    public int hashCode() {
        return key != null ? key.hashCode() : 0;
    }
}
