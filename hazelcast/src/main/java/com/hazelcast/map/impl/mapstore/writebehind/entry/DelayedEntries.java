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

import java.util.UUID;

/**
 * Mainly contains static factory methods to create various {@link DelayedEntry} instances.
 */
public final class DelayedEntries {

    private static final DelayedEntry EMPTY_DELAYED_ENTRY = new EmptyDelayedEntry();

    private DelayedEntries() {
    }

    public static <K, V> DelayedEntry<K, V> newAddedDelayedEntry(K key, V value,
                                                                 long expirationTime,
                                                                 long storeTime, int partitionId, UUID txnId) {
        if (txnId != null) {
            return new TxnAddedDelayedEntry<>(key, value, expirationTime, storeTime, partitionId, txnId);
        }
        return new AddedDelayedEntry<>(key, value, expirationTime, storeTime, partitionId);
    }

    public static <K, V> DelayedEntry<K, V> newAddedDelayedEntry(K key) {
        return new AddedDelayedEntry<>(key, null, Long.MAX_VALUE, -1, -1);
    }

    public static <K, V> DelayedEntry<K, V> newNullEntry(K key) {
        return new NullValueDelayedEntry<>(key);
    }

    public static <K, V> DelayedEntry<K, V> newDeletedEntry(K key,
                                                            long storeTime, int partitionId, UUID txnId) {
        if (txnId != null) {
            return new TxnDeletedDelayedEntry<>(key, storeTime, partitionId, txnId);
        }
        return new DeletedDelayedEntry<>(key, storeTime, partitionId);
    }

    public static <K, V> DelayedEntry<K, V> emptyDelayedEntry() {
        return EMPTY_DELAYED_ENTRY;
    }

    private static class EmptyDelayedEntry implements DelayedEntry {

        @Override
        public Object getKey() {
            return null;
        }

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public long getExpirationTime() {
            return Long.MAX_VALUE;
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
            return -1L;
        }

        @Override
        public void setTxnId(UUID txnId) {

        }

        @Override
        public UUID getTxnId() {
            return null;
        }
    }

}
