/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.impl.StoreAdapter;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;

/**
 * Record store adapter.
 */
public class RecordStoreAdapter implements StoreAdapter<Record> {

    private final RecordStore recordStore;

    public RecordStoreAdapter(RecordStore recordStore) {
        this.recordStore = recordStore;
    }

    @Override
    public boolean evictIfExpired(Data key, Record record, long now, boolean backup) {
        // evictIfExpired method is only used for HD indexes
        ensureCallingFromPartitionOperationThread();

        record = recordStore.getOrNullIfExpired(key, record, now, backup);
        if (record == null) {
            // free memory of expired record
            recordStore.disposeDeferredBlocks();
            return true;
        }

        // not expired record, update access info
        recordStore.accessRecord(record, now);
        return false;
    }

    @Override
    public boolean isTtlOrMaxIdleDefined(Record record) {
        // isTtlOrMaxIdleDefined method is only used for HD indexes
        ensureCallingFromPartitionOperationThread();

        return recordStore.isTtlOrMaxIdleDefined(record);
    }

    @Override
    public boolean isExpirable() {
        return recordStore.isExpirable();
    }

    private static void ensureCallingFromPartitionOperationThread() {
        if (Thread.currentThread().getClass() != PartitionOperationThread.class) {
            throw new IllegalThreadStateException(Thread.currentThread() + " cannot access data!");
        }
    }
}
