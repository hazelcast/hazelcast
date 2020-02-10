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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static com.hazelcast.map.impl.DirectBackupEntryProcessor.DIRECT_BACKUP_PROCESSOR;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;

public class MultipleEntryOperation extends MapOperation
        implements MutatingOperation, PartitionAwareOperation, BackupAwareOperation {

    protected Set<Data> keys;
    protected MapEntries responses;
    protected EntryProcessor entryProcessor;

    private transient List<Data> backupPairs;
    private transient Set<Data> deletions;

    public MultipleEntryOperation() {
    }

    public MultipleEntryOperation(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        super(name);
        this.keys = keys;
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        final SerializationService serializationService = getNodeEngine().getSerializationService();
        final ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
        if (shouldUseDirectBackup()) {
            backupPairs = new ArrayList<>(2 * keys.size());
            deletions = new HashSet<>();
        }
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    protected void runInternal() {
        responses = new MapEntries(keys.size());
        if (keys.isEmpty()) {
            return;
        }

        EntryOperator operator = operator(this, entryProcessor, getPredicate());
        for (Data key : keys) {
            Data response = operator.operateOnKey(key).doPostOperateOps().getResult();
            if (response != null) {
                responses.add(key, response);
            }
            if (backupPairs != null && getNodeEngine().getPartitionService().getPartitionId(key) == getPartitionId()) {
                if (operator.getEventType() == EntryEventType.REMOVED) {
                    deletions.add(key);
                } else {
                    Record record = recordStore.getRecord(key);
                    if (record != null) {
                        backupPairs.add(key);
                        backupPairs.add(mapServiceContext.toData(record.getValue()));
                    }
                }
            }
        }
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    public Object getResponse() {
        return responses;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryProcessor.getBackupProcessor() != null;
    }

    private boolean shouldUseDirectBackup() {
        return entryProcessor.getBackupProcessor() == DIRECT_BACKUP_PROCESSOR
                && mapContainer.getTotalBackupCount() > 0;
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
        EntryProcessor backupProcessor = entryProcessor.getBackupProcessor();
        Operation backupOperation = null;
        if (backupProcessor == DIRECT_BACKUP_PROCESSOR) {
            backupOperation = getDirectBackupOperation();
        } else if (backupProcessor != null) {
            backupOperation = getMultipleEntryBackupOperation(backupProcessor);
        }
        return backupOperation;
    }

    protected MultipleEntryBackupOperation getMultipleEntryBackupOperation(EntryProcessor backupProcessor) {
        return new MultipleEntryBackupOperation(name, keys, backupProcessor);
    }

    @Nonnull
    private Operation getDirectBackupOperation() {
        List<Object> toBackupList = new ArrayList<>(backupPairs.size());
        for (int i = 0; i < backupPairs.size(); i += 2) {
            Data dataKey = backupPairs.get(i);
            Record record = recordStore.getRecord(dataKey);
            if (record == null) {
                deletions.add(dataKey);
            } else {
                toBackupList.add(dataKey);
                toBackupList.add(backupPairs.get(i + 1));
                toBackupList.add(record);
            }
        }
        return new PutAllDeleteAllBackupOperation(name, toBackupList, deletions, false);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
        int size = in.readInt();
        keys = createHashSet(size);
        for (int i = 0; i < size; i++) {
            Data key = IOUtil.readData(in);
            keys.add(key);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
        out.writeInt(keys.size());
        for (Data key : keys) {
            IOUtil.writeData(out, key);
        }
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MULTIPLE_ENTRY;
    }
}
