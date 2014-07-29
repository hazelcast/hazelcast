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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapInitChunkOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the pre-provisioning task on member startup
 *
 * @param <K> key type
 * @param <V> value type
 */
final class RemoteProvisionTask<K, V>
        implements Runnable {

    private final AbstractBaseReplicatedRecordStore<K, V> replicatedRecordStore;
    private final OperationService operationService;
    private final Address callerAddress;
    private final int chunkSize;

    private ReplicatedRecord[] recordCache;
    private int recordCachePos;

    RemoteProvisionTask(AbstractBaseReplicatedRecordStore<K, V> replicatedRecordStore, NodeEngine nodeEngine,
                        Address callerAddress, int chunkSize) {

        this.replicatedRecordStore = replicatedRecordStore;
        this.operationService = nodeEngine.getOperationService();
        this.callerAddress = callerAddress;
        this.chunkSize = chunkSize;
    }

    @Override
    public void run() {
        recordCache = new ReplicatedRecord[chunkSize];
        List<ReplicatedRecord<K, V>> replicatedRecords = new ArrayList<ReplicatedRecord<K, V>>(
                replicatedRecordStore.storage.values());

        for (int i = 0; i < replicatedRecords.size(); i++) {
            ReplicatedRecord<K, V> replicatedRecord = replicatedRecords.get(i);
            processReplicatedRecord(replicatedRecord, i == replicatedRecords.size() - 1);
        }
    }

    private void processReplicatedRecord(ReplicatedRecord<K, V> replicatedRecord, boolean finalRecord) {
        Object marshalledKey = replicatedRecordStore.marshallKey(replicatedRecord.getKey());
        synchronized (replicatedRecordStore.getMutex(marshalledKey)) {
            pushReplicatedRecord(replicatedRecord, finalRecord);
        }
    }

    private void pushReplicatedRecord(ReplicatedRecord<K, V> replicatedRecord, boolean finalRecord) {
        if (recordCachePos == chunkSize) {
            sendChunk(finalRecord);
        }

        int hash = replicatedRecord.getLatestUpdateHash();
        Object key = replicatedRecordStore.unmarshallKey(replicatedRecord.getKey());
        Object value = replicatedRecordStore.unmarshallValue(replicatedRecord.getValue());
        VectorClockTimestamp vectorClockTimestamp = replicatedRecord.getVectorClockTimestamp();
        long ttlMillis = replicatedRecord.getTtlMillis();
        recordCache[recordCachePos++] = new ReplicatedRecord(key, value, vectorClockTimestamp, hash, ttlMillis);

        if (finalRecord) {
            sendChunk(finalRecord);
        }
    }

    private void sendChunk(boolean finalChunk) {
        if (recordCachePos > 0) {
            String name = replicatedRecordStore.getName();
            Member localMember = replicatedRecordStore.localMember;
            Operation operation = new ReplicatedMapInitChunkOperation(name, localMember, recordCache, recordCachePos, finalChunk);
            operationService.send(operation, callerAddress);

            // Reset chunk cache and pos
            recordCache = new ReplicatedRecord[chunkSize];
            recordCachePos = 0;
        }
    }
}
