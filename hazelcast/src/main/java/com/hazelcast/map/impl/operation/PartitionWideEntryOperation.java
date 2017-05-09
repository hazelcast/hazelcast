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

import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Iterator;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;

/**
 * GOTCHA: This operation does NOT load missing keys from map-store for now.
 */

public class PartitionWideEntryOperation extends MapOperation
        implements MutatingOperation, PartitionAwareOperation, BackupAwareOperation {

    protected MapEntries responses;
    protected EntryProcessor entryProcessor;

    protected transient EntryOperator operator;

    public PartitionWideEntryOperation() {
    }

    public PartitionWideEntryOperation(String name, EntryProcessor entryProcessor) {
        super(name);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        SerializationService serializationService = getNodeEngine().getSerializationService();
        ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    public void run() {
        responses = new MapEntries(recordStore.size());
        operator = operator(this, entryProcessor, getPredicate(), true);

        Iterator<Record> iterator = recordStore.iterator(Clock.currentTimeMillis(), false);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data dataKey = record.getKey();

            Data response = operator.operateOnKey(dataKey).doPostOperateOps().getResult();
            if (response != null) {
                responses.add(dataKey, response);
            }
        }
    }

    @Override
    public Object getResponse() {
        return responses;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        PartitionWideEntryBackupOperation backupOperation = null;
        if (backupProcessor != null) {
            backupOperation = new PartitionWideEntryBackupOperation(name, backupProcessor);
            backupOperation.setWanEventList(operator.getWanEventList());
        }
        return backupOperation;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PARTITION_WIDE_ENTRY;
    }

}
