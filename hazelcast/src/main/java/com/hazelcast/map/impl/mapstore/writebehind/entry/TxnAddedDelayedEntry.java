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

import com.hazelcast.map.MapStore;

import java.util.UUID;

/**
 * Represents a candidate entry to be
 * inserted into {@link MapStore} within a txn
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
class TxnAddedDelayedEntry<K, V> extends AddedDelayedEntry<K, V> {
    private UUID txnId;

    TxnAddedDelayedEntry(K key, V value,
                         long expirationTime, long storeTime,
                         int partitionId, UUID txnId) {
        super(key, value, expirationTime, storeTime, partitionId);
        this.txnId = txnId;
    }

    @Override
    public UUID getTxnId() {
        return txnId;
    }

    @Override
    public void setTxnId(UUID txnId) {
        this.txnId = txnId;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "TxnAddedDelayedEntry{"
                + "txnId=" + txnId
                + "} " + super.toString();
    }
}
