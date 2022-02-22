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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
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
        recordStore.forEach((key, record) -> operator.operateOnKey(key).doPostOperateOps(), true);
    }

    // TODO unify this method with `runWithPartitionScan`
    protected void runWithPartitionScanForNative() {
        EntryOperator operator = operator(this, backupProcessor, getPredicate());

        Queue<Object> outComes = new LinkedList<>();
        recordStore.forEach((key, record) -> {
            Data dataKey = toHeapData(key);
            operator.operateOnKey(dataKey);

            EntryEventType eventType = operator.getEventType();
            if (eventType != null) {
                outComes.add(dataKey);
                outComes.add(operator.getOldValue());
                outComes.add(operator.getByPreferringDataNewValue());
                outComes.add(eventType);
                outComes.add(operator.getEntry().getNewTtl());
            }
        }, true);

        // This iteration is needed to work around an issue
        // related with binary elastic hash map (BEHM). Removal
        // via map#remove() while iterating on BEHM distorts
        // it and we can see some entries remain in the map
        // even we know that iteration is finished. Because
        // in this case, iteration can miss some entries.
        while (!outComes.isEmpty()) {
            Data dataKey = (Data) outComes.poll();
            Object oldValue = outComes.poll();
            Object newValue = outComes.poll();
            EntryEventType eventType = (EntryEventType) outComes.poll();
            long newTtl = (long) outComes.poll();

            operator.init(dataKey, oldValue, newValue, null, eventType,
                    null, newTtl).doPostOperateOps();
        }
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws
            IOException {
        super.readInternal(in);
        backupProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws
            IOException {
        super.writeInternal(out);
        out.writeObject(backupProcessor);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PARTITION_WIDE_ENTRY_BACKUP;
    }
}
