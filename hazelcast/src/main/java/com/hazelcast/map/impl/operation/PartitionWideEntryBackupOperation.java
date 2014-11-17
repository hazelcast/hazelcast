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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryBackupProcessor;
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

    public PartitionWideEntryBackupOperation() {
    }

    public PartitionWideEntryBackupOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name, backupProcessor);
    }

    @Override
    public void run() {
        final long now = getNow();

        final Iterator<Record> iterator = recordStore.iterator(now, true);
        while (iterator.hasNext()) {
            final Record record = iterator.next();
            final Data dataKey = record.getKey();
            final Object oldValue = record.getValue();

            final Object key = toObject(dataKey);
            final Object value = toObject(oldValue);

            if (!applyPredicate(dataKey, key, value)) {
                continue;
            }
            final Map.Entry entry = createMapEntry(key, value);

            processBackup(entry);

            if (noOp(entry, oldValue)) {
                continue;
            }
            if (entryRemovedBackup(entry, dataKey)) {
                continue;
            }
            entryAddedOrUpdatedBackup(entry, dataKey);

            evict(true);
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(backupProcessor);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public String toString() {
        return "PartitionWideEntryBackupOperation{}";
    }

    private boolean applyPredicate(Data dataKey, Object key, Object value) {
        if (getPredicate() == null) {
            return true;
        }
        final SerializationService ss = getNodeEngine().getSerializationService();
        QueryEntry queryEntry = new QueryEntry(ss, dataKey, key, value);
        return getPredicate().apply(queryEntry);
    }
}
