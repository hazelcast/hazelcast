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

package com.hazelcast.map;

import com.hazelcast.util.ConcurrencyUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PartitionContainer {
    private final MapService mapService;
    final int partitionId;
    final ConcurrentMap<String, PartitionRecordStore> maps = new ConcurrentHashMap<String, PartitionRecordStore>(1000);
    // TODO: @mm - migrate transaction items!
    final ConcurrentMap<TransactionKey, TransactionItem> transactionItems = new ConcurrentHashMap<TransactionKey, TransactionItem>();

    public PartitionContainer(final MapService mapService, final int partitionId) {
        this.mapService = mapService;
        this.partitionId = partitionId;
    }

    public MapService getMapService() {
        return mapService;
    }

    private final ConcurrencyUtil.ConstructorFunction<String, PartitionRecordStore> recordStoreConstructor
            = new ConcurrencyUtil.ConstructorFunction<String, PartitionRecordStore>() {
        public PartitionRecordStore createNew(String name) {
            return new PartitionRecordStore(name, PartitionContainer.this);
        }
    };

    public RecordStore getRecordStore(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(maps, name, recordStoreConstructor);
    }

    public TransactionItem addTransactionItem(TransactionItem item) {
        final TransactionKey key = new TransactionKey(item.getTxnId(), item.getName(), item.getKey());
        return transactionItems.put(key, item);
    }

    public TransactionItem getTransactionItem(TransactionKey key) {
        return transactionItems.get(key);
    }

    public TransactionItem removeTransactionItem(TransactionKey key) {
        return transactionItems.remove(key);
    }

    public int getMaxBackupCount() {
        int max = 1;
        for (PartitionRecordStore recordStore : maps.values()) {
            max = Math.max(max, recordStore.getMapContainer().getTotalBackupCount());
        }
        return max;
    }

    void destroyMap(String name) {
        PartitionRecordStore recordStore = maps.remove(name);
        if (recordStore != null)
            recordStore.clear();
    }

    void clear() {
        for (PartitionRecordStore store : maps.values()) {
            store.clear();
        }
        maps.clear();
        transactionItems.clear();
    }
}
