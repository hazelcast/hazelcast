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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.DirectBackupEntryProcessor.DIRECT_BACKUP_PROCESSOR;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;

/**
 * GOTCHA: This operation does NOT load missing keys from map-store for now.
 */
public class PartitionWideEntryOperation extends MapOperation
        implements MutatingOperation, PartitionAwareOperation, BackupAwareOperation {

    protected MapEntries responses;
    protected EntryProcessor entryProcessor;

    protected transient EntryOperator operator;
    protected transient Set<Data> keysFromIndex;
    protected transient QueryOptimizer queryOptimizer;

    protected transient List<Data> backupPairs;
    protected transient Set<Data> deletions;

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

        keysFromIndex = null;
        queryOptimizer = mapServiceContext.getQueryOptimizer();

        backupPairs = null;
        deletions = null;
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    protected void runInternal() {
        if (mapContainer.getMapConfig().getInMemoryFormat() == InMemoryFormat.NATIVE) {
            runForNative();
        } else {
            runWithPartitionScan();
        }
    }

    private void runForNative() {
        if (runWithIndex()) {
            return;
        }
        runWithPartitionScanForNative();
    }

    /**
     * @return {@code true} if index has been used and the EP
     * has been executed on its keys, {@code false} otherwise
     */
    private boolean runWithIndex() {
        // here we try to query the partitioned-index
        Predicate predicate = getPredicate();
        if (predicate == null) {
            return false;
        }

        // we use the partitioned-index to operate on the selected keys only
        Indexes indexes = mapContainer.getIndexes(getPartitionId());
        Set<QueryableEntry> entries = indexes.query(queryOptimizer.optimize(predicate, indexes), 1);
        if (entries == null) {
            return false;
        }

        responses = new MapEntries(entries.size());

        // when NATIVE we can pass null as predicate since it's all
        // happening on partition thread so no data-changes may occur
        operator = operator(this, entryProcessor, null);
        if (shouldUseDirectBackup()) {
            backupPairs = new ArrayList<>(entries.size());
            deletions = new HashSet<>();
        } else {
            keysFromIndex = new HashSet<>(entries.size());
        }
        for (QueryableEntry entry : entries) {
            Data response = operator.operateOnKey(entry.getKeyData()).doPostOperateOps().getResult();
            if (response != null) {
                responses.add(entry.getKeyData(), response);
            }
            if (keysFromIndex != null) {
                keysFromIndex.add(entry.getKeyData());
            }
            addDirectBackup(operator.getEventType(), entry.getKeyData(), operator.getValueData());
        }

        return true;
    }

    private void runWithPartitionScan() {
        int totalEntryCount = recordStore.size();
        responses = new MapEntries(totalEntryCount);
        if (shouldUseDirectBackup()) {
            backupPairs = new ArrayList<>(totalEntryCount);
            deletions = new HashSet<>();
        }
        operator = operator(this, entryProcessor, getPredicate());
        recordStore.forEach((dataKey, record) -> {
            Data response = operator.operateOnKey(dataKey).doPostOperateOps().getResult();
            if (response != null) {
                responses.add(dataKey, response);
            }
            addDirectBackup(operator.getEventType(), dataKey, mapServiceContext.toData(record.getValue()));
        }, false);
    }

    // TODO unify this method with `runWithPartitionScan`
    private void runWithPartitionScanForNative() {
        // if we reach here, it means we didn't manage to leverage index and we fall-back to full-partition scan
        int totalEntryCount = recordStore.size();
        responses = new MapEntries(totalEntryCount);
        if (shouldUseDirectBackup()) {
            backupPairs = new ArrayList<>(totalEntryCount);
            deletions = new HashSet<>();
        }
        Queue<Object> outComes = new LinkedList<>();
        operator = operator(this, entryProcessor, getPredicate());

        recordStore.forEach((key, record) -> {
            Data dataKey = toHeapData(key);

            Data response = operator.operateOnKey(dataKey).getResult();
            if (response != null) {
                responses.add(dataKey, response);
            }

            EntryEventType eventType = operator.getEventType();
            if (eventType != null) {
                outComes.add(dataKey);
                outComes.add(operator.getOldValue());
                outComes.add(operator.getByPreferringDataNewValue());
                outComes.add(eventType);
            }
        }, false);

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

            operator.init(dataKey, oldValue, newValue, null, eventType, null)
                    .doPostOperateOps();
            addDirectBackup(eventType, dataKey, mapServiceContext.toData(newValue));
        }
    }

    private void addDirectBackup(EntryEventType eventType, Data dataKey, Data valueData) {
        if (backupPairs != null) {
            if (eventType == EntryEventType.REMOVED) {
                deletions.add(dataKey);
            } else {
                backupPairs.add(dataKey);
                backupPairs.add(valueData);
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
            // direct copy backup
            backupOperation = getDirectBackupOperation();
        } else if (backupProcessor != null) {
            if (keysFromIndex != null) {
                // if we used index we leverage it for the backup too
                backupOperation = new MultipleEntryBackupOperation(name, keysFromIndex, backupProcessor);
            } else {
                // if no index used we will do a full partition-scan on backup too
                backupOperation = getPartitionWideEntryBackupOperation(backupProcessor);
            }
        }
        return backupOperation;
    }

    protected Operation getPartitionWideEntryBackupOperation(EntryProcessor backupProcessor) {
        return new PartitionWideEntryBackupOperation(name, backupProcessor);
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
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", entryProcessor=").append(entryProcessor);
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
    public int getClassId() {
        return MapDataSerializerHook.PARTITION_WIDE_ENTRY;
    }

}
