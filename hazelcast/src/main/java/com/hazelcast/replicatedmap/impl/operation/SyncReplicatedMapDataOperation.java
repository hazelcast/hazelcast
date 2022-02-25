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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.InternalReplicatedMapStorage;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * Carries set of replicated map records for a partition from one node to another
 */
public class SyncReplicatedMapDataOperation<K, V> extends AbstractSerializableOperation {

    private String name;
    private Set<RecordMigrationInfo> recordSet;
    private long version;

    public SyncReplicatedMapDataOperation() {
    }

    public SyncReplicatedMapDataOperation(String name, Set<RecordMigrationInfo> recordSet, long version) {
        this.name = name;
        this.recordSet = recordSet;
        this.version = version;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() throws Exception {
        ILogger logger = getLogger();
        if (logger.isFineEnabled()) {
            logger.fine("Syncing " + recordSet.size() + " records (version " + version
                    + ") for replicated map '" + name + "' (partitionId " + getPartitionId()
                    + ") from " + getCallerAddress() + " to " + getNodeEngine().getThisAddress());
        }
        ReplicatedMapService service = getService();
        AbstractReplicatedRecordStore store
                = (AbstractReplicatedRecordStore) service.getReplicatedRecordStore(name, true, getPartitionId());
        InternalReplicatedMapStorage<K, V> newStorage = new InternalReplicatedMapStorage<>();
        for (RecordMigrationInfo record : recordSet) {
            K key = (K) store.marshall(record.getKey());
            V value = (V) store.marshall(record.getValue());
            ReplicatedRecord<K, V> replicatedRecord = buildReplicatedRecord(key, value, record.getTtl());
            ReplicatedRecord oldRecord = store.getReplicatedRecord(key);
            if (oldRecord != null) {
                replicatedRecord.setHits(oldRecord.getHits());
            }
            newStorage.put(key, replicatedRecord);
            if (record.getTtl() > 0) {
                store.scheduleTtlEntry(record.getTtl(), key, value);
            }
        }

        newStorage.syncVersion(version);
        AtomicReference<InternalReplicatedMapStorage<K, V>> storageRef = store.getStorageRef();
        storageRef.set(newStorage);
        store.setLoaded(true);
    }

    private ReplicatedRecord<K, V> buildReplicatedRecord(K key, V value, long ttlMillis) {
        return new ReplicatedRecord<>(key, value, ttlMillis);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(version);
        out.writeInt(recordSet.size());
        for (RecordMigrationInfo record : recordSet) {
            record.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        version = in.readLong();
        int size = in.readInt();
        recordSet = createHashSet(size);
        for (int j = 0; j < size; j++) {
            RecordMigrationInfo record = new RecordMigrationInfo();
            record.readData(in);
            recordSet.add(record);
        }
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.SYNC_REPLICATED_DATA;
    }
}
