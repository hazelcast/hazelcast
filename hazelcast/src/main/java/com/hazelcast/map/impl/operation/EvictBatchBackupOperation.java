/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.ExpiredKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import static com.hazelcast.util.CollectionUtil.isNotEmpty;

public class EvictBatchBackupOperation extends MapOperation implements BackupOperation {

    private String name;
    private Collection<ExpiredKey> expiredKeys;
    private int ownerPartitionEntryCount;

    public EvictBatchBackupOperation() {
    }

    public EvictBatchBackupOperation(String name, Collection<ExpiredKey> expiredKeys, int ownerPartitionEntryCount) {
        super(name);

        assert isNotEmpty(expiredKeys);
        assert name != null;

        this.name = name;
        this.expiredKeys = expiredKeys;
        this.ownerPartitionEntryCount = ownerPartitionEntryCount;
    }

    @Override
    public void run() {
        assert getNodeEngine().getClusterService().getClusterVersion().isGreaterOrEqual(Versions.V3_9);

        for (ExpiredKey expiredKey : expiredKeys) {
            Record existingRecord = recordStore.getRecord(expiredKey.getKey());
            if (canEvictRecord(existingRecord, expiredKey)) {
                recordStore.evict(existingRecord.getKey(), true);
            }
        }

        // equalize backup entry count to owner entry count to have identical memory occupancy
        int diff = recordStore.size() - ownerPartitionEntryCount;
        for (int i = 0; i < diff; i++) {
            mapContainer.getEvictor().evict(recordStore, null);
        }
    }

    protected boolean canEvictRecord(Record existingRecord, ExpiredKey expiredKey) {
        if (existingRecord == null) {
            return false;
        }

        // creation time of a record is always same between all replicas.
        // by doing creation time check we can prevent un-wanted record deletion on replicas.
        // un-wanted record deletion example: on primary record was expired and queued but before
        // we send it to backups a new record is added with same key, when we send queued item
        // to backups, backups should not remove it. Comparing creation times to be sure that
        // we are deleting correct record.
        return existingRecord.getCreationTime() == expiredKey.getCreationTime();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.EVICT_BATCH_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(name);
        out.writeInt(expiredKeys.size());
        for (ExpiredKey expiredKey : expiredKeys) {
            out.writeData(expiredKey.getKey());
            out.writeLong(expiredKey.getCreationTime());
        }
        out.writeInt(ownerPartitionEntryCount);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        name = in.readUTF();
        int size = in.readInt();
        expiredKeys = new LinkedList<ExpiredKey>();
        for (int i = 0; i < size; i++) {
            expiredKeys.add(new ExpiredKey(in.readData(), in.readLong()));
        }
        ownerPartitionEntryCount = in.readInt();
    }
}
