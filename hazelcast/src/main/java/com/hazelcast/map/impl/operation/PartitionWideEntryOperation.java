/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.operation.steps.PartitionWideEntryOpSteps;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.wan.impl.CallerProvenance;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
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
        entryProcessor = (EntryProcessor) managedContext.initialize(entryProcessor);

        keysFromIndex = null;
        queryOptimizer = mapServiceContext.getQueryOptimizer();
    }

    @Override
    public State createState() {
        return super.createState()
                .setPredicate(getPredicate())
                .setCallerProvenance(CallerProvenance.NOT_WAN)
                .setEntryProcessor(entryProcessor)
                .setStaticPutParams(StaticParams.SET_WITH_NO_ACCESS_PARAMS);
    }

    @Override
    public Step getStartingStep() {
        return PartitionWideEntryOpSteps.PROCESS;
    }

    @Override
    public void applyState(State state) {
        super.applyState(state);
        responses = (MapEntries) state.getResult();
        keysFromIndex = state.getKeysFromIndex();
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    protected void runInternal() {
        if (isHDMap()) {
            runForNative();
        } else {
            runWithPartitionScan();
        }
    }

    public boolean isTieredStoreMap() {
        return mapContainer.getMapConfig()
                .getTieredStoreConfig().isEnabled();
    }

    private boolean isHDMap() {
        return mapContainer.getMapConfig()
                .getInMemoryFormat() == InMemoryFormat.NATIVE;
    }

    private void runForNative() {
        // try run with partitioned index
        if (!isTieredStoreMap()
                && runWithPartitionedIndex()) {
            return;
        }

        // as a fallback scan whole partition
        runWithPartitionScanForNative();
    }

    /**
     * @return {@code true} if index has been used and the EP
     * has been executed on its keys, {@code false} otherwise
     */
    private boolean runWithPartitionedIndex() {
        // here we try to query the partitioned-index
        Predicate predicate = getPredicate();
        if (predicate == null) {
            return false;
        }

        // we use the partitioned-index to operate on the selected keys only
        IndexRegistry indexRegistry = mapContainer.getOrCreateIndexRegistry(getPartitionId());
        Iterable<QueryableEntry> entries = indexRegistry.query(queryOptimizer.optimize(predicate, indexRegistry), 1);
        if (entries == null) {
            return false;
        }

        responses = new MapEntries();

        // when NATIVE we can pass null as predicate since it's all
        // happening on partition thread so no data-changes may occur
        operator = operator(this, entryProcessor, null);
        keysFromIndex = new HashSet<>();
        for (QueryableEntry entry : entries) {
            keysFromIndex.add(entry.getKeyData());
            Data response = operator.operateOnKey(entry.getKeyData()).doPostOperateOps().getResult();
            if (response != null) {
                responses.add(entry.getKeyData(), response);
            }
        }

        return true;
    }

    private void runWithPartitionScan() {
        responses = new MapEntries(recordStore.size());
        operator = operator(this, entryProcessor, getPredicate());
        recordStore.forEach((dataKey, record) -> {
            Data response = operator.operateOnKey(dataKey).doPostOperateOps().getResult();
            if (response != null) {
                responses.add(dataKey, response);
            }
        }, false);
    }

    // TODO unify this method with `runWithPartitionScan`
    private void runWithPartitionScanForNative() {
        // if we reach here, it means we didn't manage to leverage index and we fall-back to full-partition scan
        int totalEntryCount = recordStore.size();
        responses = new MapEntries(totalEntryCount);
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
                outComes.add(operator.getEntry().getNewTtl());
                outComes.add(operator.getEntry().isChangeExpiryOnUpdate());
            } else {
                // when event type is null, it means that there was no modification
                operator.doPostOperateOps();
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
            long newTtl = (long) outComes.poll();
            boolean changeExpiryOnUpdate = (boolean) outComes.poll();

            operator.init(dataKey, oldValue, newValue, null, eventType,
                    null, changeExpiryOnUpdate, newTtl).doPostOperateOps();
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
    @SuppressFBWarnings(
            value = {"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"},
            justification = "backupProcessor can indeed be null so check is not redundant")
    public Operation getBackupOperation() {
        EntryProcessor backupProcessor = entryProcessor.getBackupProcessor();
        if (backupProcessor == null) {
            return null;
        }
        if (keysFromIndex != null) {
            // if we used index we leverage it for the backup too
            return new MultipleEntryBackupOperation(name, keysFromIndex, backupProcessor);
        } else {
            // if no index used we will do a full partition-scan on backup too
            return new PartitionWideEntryBackupOperation(name, backupProcessor);
        }
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
