/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.internal.util.Clock;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;

public class PartitionWideEntryBackupOperation extends AbstractMultipleEntryBackupOperation
        implements BackupOperation {

    public PartitionWideEntryBackupOperation() {
    }

    public PartitionWideEntryBackupOperation(String name, EntryProcessor backupProcessor) {
        super(name, backupProcessor);
    }

    @Override
    protected void runInternal() {
        if (mapContainer.getMapConfig().getInMemoryFormat() == InMemoryFormat.NATIVE) {
            runWithPartitionScanForNative();
        } else {
            runWithPartitionScan();
        }
    }

    private void runWithPartitionScan() {
        EntryOperator operator = operator(this, backupProcessor, getPredicate());

        Iterator<Record> iterator = recordStore.iterator(Clock.currentTimeMillis(), true);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            operator.operateOnKey(record.getKey()).doPostOperateOps();
        }
    }

    // TODO unify this method with `runWithPartitionScan`
    protected void runWithPartitionScanForNative() {
        Queue<Object> outComes = null;
        EntryOperator operator = operator(this, backupProcessor, getPredicate());

        Iterator<Record> iterator = recordStore.iterator(Clock.currentTimeMillis(), true);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data dataKey = toHeapData(record.getKey());
            operator.operateOnKey(dataKey);

            EntryEventType eventType = operator.getEventType();
            if (eventType != null) {
                if (outComes == null) {
                    outComes = new LinkedList<>();
                }

                outComes.add(dataKey);
                outComes.add(operator.getOldValue());
                outComes.add(operator.getNewValue());
                outComes.add(eventType);
            }
        }

        if (outComes != null) {
            // This iteration is needed to work around an issue related with binary elastic hash map (BEHM).
            // Removal via map#remove() while iterating on BEHM distorts it and we can see some entries remain
            // in the map even we know that iteration is finished. Because in this case, iteration can miss some entries.
            do {
                Data dataKey = (Data) outComes.poll();
                Object oldValue = outComes.poll();
                Object newValue = outComes.poll();
                EntryEventType eventType = (EntryEventType) outComes.poll();

                operator.init(dataKey, oldValue, newValue, null, eventType)
                        .doPostOperateOps();

            } while (!outComes.isEmpty());
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
    public int getClassId() {
        return MapDataSerializerHook.PARTITION_WIDE_ENTRY_BACKUP;
    }
}
