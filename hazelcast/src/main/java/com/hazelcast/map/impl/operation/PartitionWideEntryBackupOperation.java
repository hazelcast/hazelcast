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

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Iterator;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;

public class PartitionWideEntryBackupOperation extends AbstractMultipleEntryBackupOperation implements BackupOperation {

    public PartitionWideEntryBackupOperation() {
    }

    public PartitionWideEntryBackupOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name, backupProcessor);
    }

    @Override
    public void run() {
        responses = new MapEntries(recordStore.size());
        EntryOperator operator = operator(this, backupProcessor, getPredicate(), true);

        Iterator<Record> iterator = recordStore.iterator(Clock.currentTimeMillis(), true);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            operator.operateOnKey(record.getKey()).doPostOperateOps();
        }
    }

    @Override
    public Object getResponse() {
        return true;
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
    public int getId() {
        return MapDataSerializerHook.PARTITION_WIDE_ENTRY_BACKUP;
    }
}
