/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.writebehind.Sequencer;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class PartitionWideEntryBackupOperation extends AbstractMultipleEntryOperation implements BackupOperation {

    private int storeableEntryCount;

    public PartitionWideEntryBackupOperation() {
    }

    public PartitionWideEntryBackupOperation(String name, EntryBackupProcessor backupProcessor,
                                             int storeableEntryCount) {
        super(name, backupProcessor);
        this.storeableEntryCount = storeableEntryCount;
    }

    @Override
    public void run() {
        long now = getNow();
        Iterator<Record> iterator = recordStore.iterator(now, true);
        while (iterator.hasNext()) {
            if (storeableEntryCount <= 0) {
                MapDataStore<Data, Object> mapDataStore = recordStore.getMapDataStore();
                if (mapDataStore.isWriteBehind()) {
                    Sequencer sequencer = ((WriteBehindStore) mapDataStore).getWriteBehindQueue().getSequencer();
                    long sequence = sequencer.tailSequence();
                    sequencer.setTailSequence(sequence - 1);
                }
            }

            Record record = iterator.next();
            Data dataKey = record.getKey();
            Object oldValue = record.getValue();

            if (!applyPredicate(dataKey, dataKey, oldValue)) {
                continue;
            }
            Map.Entry entry = createMapEntry(dataKey, oldValue);

            processBackup(entry);

            if (noOp(entry, oldValue)) {
                continue;
            }
            if (entryRemovedBackup(entry, dataKey)) {
                --storeableEntryCount;
                continue;
            }
            entryAddedOrUpdatedBackup(entry, dataKey);
            --storeableEntryCount;

            evict(true);
        }
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    public Object getResponse() {
        return true;
    }

    private boolean applyPredicate(Data dataKey, Object key, Object value) {
        if (getPredicate() == null) {
            return true;
        }
        final SerializationService ss = getNodeEngine().getSerializationService();
        QueryEntry queryEntry = new QueryEntry(ss, dataKey, key, value);
        return getPredicate().apply(queryEntry);
    }

    @Override
    public String toString() {
        return "PartitionWideEntryBackupOperation{}";
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupProcessor = in.readObject();
        storeableEntryCount = in.readInt();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(backupProcessor);
        out.writeInt(storeableEntryCount);
    }
}
