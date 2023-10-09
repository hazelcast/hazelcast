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

package com.hazelcast.map.impl.operation.steps.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.operation.EntryOperator;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static com.hazelcast.map.impl.record.Record.UNSET;

/**
 * {@link Step} uses this state object to
 * transfer state between subsequent steps.
 */
@SuppressWarnings("checkstyle:methodcount")
public class State {

    private final RecordStore recordStore;
    private final MapOperation operation;

    // fields coming from operation
    private int partitionId = UNSET;
    private long threadId;
    private long ttl = UNSET;
    private long maxIdle = UNSET;
    private long version;
    private long now = Clock.currentTimeMillis();
    private Data key;
    private Address callerAddress;
    private Object expect;
    private StaticParams staticParams;
    private UUID txnId;
    private UUID ownerUuid;
    private CallerProvenance callerProvenance;

    // fields used for inter thread communication
    private volatile boolean blockReads;
    private volatile boolean stopExecution;
    private volatile boolean recordExistsInMemory;
    private volatile boolean disableWanReplicationEvent;
    private volatile boolean triggerMapLoader;
    private volatile boolean shouldLoad;
    private volatile boolean changeExpiryOnUpdate = true;
    private volatile boolean entryProcessorOffloadable;
    private volatile Object oldValue;
    private volatile Object newValue;
    private volatile Object result;
    private volatile Collection<Data> keysToLoad = Collections.emptyList();
    private volatile Map loadedKeyValuePairs = Collections.emptyMap();
    private volatile Collection<Data> keys;
    private volatile List<Record> records;
    private volatile EntryProcessor entryProcessor;
    private volatile EntryOperator operator;
    private volatile List<State> toStore;
    private volatile List<State> toRemove;
    private volatile List backupPairs;
    private volatile Predicate predicate;
    private volatile List<SplitBrainMergeTypes.MapMergeTypes<Object, Object>> mergingEntries;
    private volatile SplitBrainMergePolicy<Object,
            SplitBrainMergeTypes.MapMergeTypes<Object, Object>, Object> mergePolicy;
    private volatile MapEntries mapEntries;
    private volatile EntryEventType entryEventType;
    private volatile Record record;
    private volatile Queue<InternalIndex> notMarkedIndexes;
    private volatile Set keysFromIndex;
    private volatile Throwable throwable;
    private volatile Consumer backupOpAfterRun;

    public State(RecordStore recordStore, MapOperation operation) {
        this.recordStore = recordStore;
        this.operation = operation;
    }

    public State(State state) {
        this.recordStore = state.getRecordStore();
        this.operation = state.getOperation();

        setTtl(state.getTtl())
                .setMaxIdle(state.getMaxIdle())
                .setChangeExpiryOnUpdate(state.isChangeExpiryOnUpdate())
                .setVersion(state.getVersion())
                .setNow(state.getNow())
                .setStaticPutParams(state.getStaticParams())
                .setOwnerUuid(state.getOwnerUuid())
                .setTxnId(state.getTxnId())
                .setCallerProvenance(state.getCallerProvenance())
                .setEntryProcessor(state.getEntryProcessor())
                .setCallerAddress(state.getCallerAddress())
                .setPartitionId(state.getPartitionId());
    }

    public RecordStore getRecordStore() {
        return recordStore;
    }

    public MapOperation getOperation() {
        return operation;
    }

    public State setVersion(long version) {
        this.version = version;
        return this;
    }

    public State setPartitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public State setKey(Data key) {
        this.key = key;
        return this;
    }

    public State setNewValue(Object newValue) {
        this.newValue = newValue;
        return this;
    }

    public State setOldValue(Object oldValue) {
        this.oldValue = oldValue;
        return this;
    }

    public State setRecordExistsInMemory(boolean recordExistsInMemory) {
        this.recordExistsInMemory = recordExistsInMemory;
        return this;
    }

    public State setExpect(Object expect) {
        this.expect = expect;
        return this;
    }

    public State setTxnId(UUID uuid) {
        this.txnId = uuid;
        return this;
    }

    public State setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public State setMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    public State setStaticPutParams(StaticParams staticStaticParams) {
        this.staticParams = staticStaticParams;
        return this;
    }

    public State setCallerAddress(Address callerAddress) {
        this.callerAddress = callerAddress;
        return this;
    }

    public State setCallerProvenance(CallerProvenance callerProvenance) {
        this.callerProvenance = callerProvenance;
        return this;
    }

    public State setEntryProcessor(EntryProcessor entryProcessor) {
        this.entryProcessor = entryProcessor;
        return this;
    }

    public boolean isEntryProcessorOffloadable() {
        return entryProcessorOffloadable;
    }

    public State setEntryProcessorOffloadable(boolean entryProcessorOffloadable) {
        this.entryProcessorOffloadable = entryProcessorOffloadable;
        return this;
    }

    public State setThreadId(long threadId) {
        this.threadId = threadId;
        return this;
    }

    public long getThreadId() {
        return threadId;
    }

    public EntryProcessor getEntryProcessor() {
        return entryProcessor;
    }

    public boolean isRecordExistsInMemory() {
        return recordExistsInMemory;
    }

    public Object getOldValue() {
        return oldValue;
    }

    public Object getNewValue() {
        return newValue;
    }

    public long getTtl() {
        return ttl;
    }

    public long getMaxIdle() {
        return maxIdle;
    }

    public long getVersion() {
        return version;
    }

    public long getNow() {
        return now;
    }

    public State setNow(long now) {
        this.now = now;
        return this;
    }

    public Data getKey() {
        return key;
    }

    public Address getCallerAddress() {
        return callerAddress;
    }

    public Object getExpect() {
        return expect;
    }

    public StaticParams getStaticParams() {
        return staticParams;
    }

    public UUID getTxnId() {
        return txnId;
    }

    public UUID getOwnerUuid() {
        return ownerUuid;
    }

    public State setOwnerUuid(UUID ownerUuid) {
        this.ownerUuid = ownerUuid;
        return this;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public CallerProvenance getCallerProvenance() {
        return callerProvenance;
    }

    public boolean isStopExecution() {
        return stopExecution;
    }

    public void setStopExecution(boolean stopExecution) {
        this.stopExecution = stopExecution;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Object getResult() {
        return result;
    }

    public void setRecord(Record record) {
        this.record = record;
    }

    public Record getRecord() {
        return record;
    }

    public State setKeys(Collection<Data> keys) {
        this.keys = keys;
        return this;
    }

    public Collection<Data> getKeys() {
        return keys;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
    }

    public Collection<Data> getKeysToLoad() {
        return keysToLoad;
    }

    public State setKeysToLoad(Collection<Data> keysToLoad) {
        this.keysToLoad = keysToLoad;
        return this;
    }

    public State setLoadedKeyValuePairs(Map loadedKeyValuePairs) {
        this.loadedKeyValuePairs = loadedKeyValuePairs;
        return this;
    }

    public Map getLoadedKeyValuePairs() {
        return loadedKeyValuePairs;
    }

    public List<Record> getRecords() {
        return records;
    }

    public State setEntryOperator(EntryOperator operator) {
        this.operator = operator;
        return this;
    }

    public EntryOperator getOperator() {
        return operator;
    }

    public void setToStore(List<State> toStore) {
        this.toStore = toStore;
    }

    public void setToRemove(List<State> toRemove) {
        this.toRemove = toRemove;
    }

    public List<State> getToStore() {
        return toStore;
    }

    public List<State> getToRemove() {
        return toRemove;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public State setPredicate(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    public State setMergingEntries(List<SplitBrainMergeTypes.MapMergeTypes<Object, Object>> mergingEntries) {
        this.mergingEntries = mergingEntries;
        return this;
    }

    public List<SplitBrainMergeTypes.MapMergeTypes<Object, Object>> getMergingEntries() {
        return mergingEntries;
    }

    public State setMergePolicy(SplitBrainMergePolicy<Object,
            SplitBrainMergeTypes.MapMergeTypes<Object, Object>, Object> mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }

    public SplitBrainMergePolicy<Object,
            SplitBrainMergeTypes.MapMergeTypes<Object, Object>, Object> getMergePolicy() {
        return mergePolicy;
    }

    public State setDisableWanReplicationEvent(boolean disableWanReplicationEvent) {
        this.disableWanReplicationEvent = disableWanReplicationEvent;
        return this;
    }

    public boolean isDisableWanReplicationEvent() {
        return disableWanReplicationEvent;
    }

    public State setMapEntries(MapEntries mapEntries) {
        this.mapEntries = mapEntries;
        return this;
    }

    public MapEntries getMapEntries() {
        return mapEntries;
    }

    public EntryEventType getEntryEventType() {
        return this.entryEventType;
    }

    public void setEntryEventType(EntryEventType entryEventType) {
        this.entryEventType = entryEventType;
    }

    public void setNotMarkedIndexes(Queue<InternalIndex> notMarkedIndexes) {
        this.notMarkedIndexes = notMarkedIndexes;
    }

    public Queue<InternalIndex> getNotMarkedIndexes() {
        return notMarkedIndexes;
    }

    public void setKeysFromIndex(Set keysFromIndex) {
        this.keysFromIndex = keysFromIndex;
    }

    public Set getKeysFromIndex() {
        return keysFromIndex;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public State setTriggerMapLoader(boolean triggerMapLoader) {
        this.triggerMapLoader = triggerMapLoader;
        return this;
    }

    public boolean isTriggerMapLoader() {
        return triggerMapLoader;
    }

    public State setShouldLoad(boolean shouldLoad) {
        this.shouldLoad = shouldLoad;
        return this;
    }

    public boolean isShouldLoad() {
        return shouldLoad;
    }

    public boolean isBlockReads() {
        return blockReads;
    }

    public State setBlockReads(boolean blockReads) {
        this.blockReads = blockReads;
        return this;
    }

    public void setBackupPairs(List backupPairs) {
        this.backupPairs = backupPairs;
    }

    public List getBackupPairs() {
        return backupPairs;
    }

    public State setChangeExpiryOnUpdate(boolean changeExpiryOnUpdate) {
        this.changeExpiryOnUpdate = changeExpiryOnUpdate;
        return this;
    }

    public boolean isChangeExpiryOnUpdate() {
        return changeExpiryOnUpdate;
    }

    public State setBackupOpAfterRun(Consumer backupOpAfterRun) {
        this.backupOpAfterRun = backupOpAfterRun;
        return this;
    }

    @Nullable
    public Consumer getBackupOpAfterRun() {
        return backupOpAfterRun;
    }
}
