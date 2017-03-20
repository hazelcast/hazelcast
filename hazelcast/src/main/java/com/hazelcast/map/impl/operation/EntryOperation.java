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

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.core.Offloadable.NO_OFFLOADING;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;
import static com.hazelcast.spi.ExecutionService.OFFLOADABLE_EXECUTOR;

/**
 * Contains implementation of the off-loadable contract for EntryProcessor execution on a single key.
 *
 * ### Overview
 *
 * Implementation of the the Offloadable contract for the EntryProcessor execution on a single key.
 *
 * Allows off-loading the processing unit implementing this interface to the specified or default Executor.
 * Currently supported in:
 *
 * IMap.executeOnKey(Object, EntryProcessor)
 * IMap.submitToKey(Object, EntryProcessor)
 * IMap.submitToKey(Object, EntryProcessor, ExecutionCallback)
 *
 * ### Offloadable (for reading & writing)
 *
 * If the EntryProcessor implements the Offloadable interface the processing will be offloaded to the given
 * ExecutorService allowing unblocking the partition-thread. The key will be locked for the time-span of the processing
 * in order to not generate a write-conflict.
 *
 * If the EntryProcessor implements Offloadable the invocation scenario looks as follows:
 * - EntryOperation fetches the entry and locks the given key on partition-thread
 * - Then the processing is offloaded to the given executor
 * - When the processing finishes
 * if there is a change to the entry, a EntryOffloadableSetUnlockOperation is spawned
 * which sets the new value and unlocks the given key on partition-thread
 * if there is no change to the entry, a UnlockOperation is spawned, which just unlocks the kiven key
 * on partition thread
 *
 * There will not be a conflict on a write due to the pessimistic locking of the key.
 * The threading looks as follows:
 *
 * 1. partition-thread (fetch & lock)
 * 2. execution-thread (process)
 * 3. partition-thread (set & unlock, or just unlock if no changes)
 *
 * ### Offloadable (for reading only)
 *
 * If the EntryProcessor implements the Offloadable and ReadOnly interfaces the processing will be offloaded to the
 * givenExecutorService allowing unblocking the partition-thread. Since the EntryProcessor is not supposed to do any
 * changes to the Entry the key will NOT be locked for the time-span of the processing.
 *
 * If the EntryProcessor implements Offloadable and ReadOnly the invocation scenario looks as follows:
 * - EntryOperation fetches the entry and DOES NOT lock the given key on partition-thread
 * - Then the processing is offloaded to the given executor
 * - When the processing finishes
 * if there is a change to the entry -> exception is thrown
 * if there is no change to the entry -> the result is returned to the user from the executor-thread.
 *
 * In the read-only case the threading looks as follows:
 *
 * 1. partition-thread (fetch)
 * 2. execution-thread (process)
 *
 * ### Primary partition - main actors
 *
 * - EntryOperation
 * - EntryOffloadableSetUnlockOperation
 *
 * ### Backup partitions
 *
 * Offloading will not be applied to backup partitions. It is possible to initialize the EntryBackupProcessor
 * with some input provided by the EntryProcessor in the EntryProcessor.getBackupProcessor() method.
 * The input allows providing context to the EntryBackupProcessor - for example the "delta"
 * so that the EntryBackupProcessor does not have to calculate the "delta" but it may just apply it.
 *
 * ### Locking
 *
 * The locking takes place only locally. If a partition locked by an off-loaded task gets migrated, the lock will not
 * be migrated. In this situation the off-loaded task "relying" on the lock will fail on the unlock operation, since it
 * will notice that there is no such a lock and therefore the processing for the key will get retried.
 * The reason behind is that the off-loadable backup-processing does not use locking there cannot be any transfer of
 * off-loadable locks from the primary replica to backup replicas.
 *
 * GOTCHA : This operation LOADS missing keys from map-store, in contrast with PartitionWideEntryOperation.
 */
@SuppressWarnings("checkstyle:methodcount")
public class EntryOperation extends MutatingKeyBasedMapOperation implements BackupAwareOperation, BlockingOperation {

    private EntryProcessor entryProcessor;

    private transient boolean offloading;

    // EntryOperation
    private transient Object oldValue;
    private transient EntryEventType eventType;
    private transient Object response;
    private transient Object dataValue;

    // EntryOffloadableOperation
    private transient boolean readOnly;
    private transient long begin;
    private transient OperationServiceImpl ops;

    public EntryOperation() {
    }

    public EntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        if (entryProcessor instanceof Offloadable) {
            String executorName = ((Offloadable) entryProcessor).getExecutorName();
            if (!executorName.equals(NO_OFFLOADING)) {
                offloading = true;
            }
        }

        if (offloading) {
            this.ops = (OperationServiceImpl) getNodeEngine().getOperationService();
            this.begin = getNow();
            this.readOnly = entryProcessor instanceof ReadOnly;
        }

        final SerializationService serializationService = getNodeEngine().getSerializationService();
        final ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    public void run() {
        if (offloading) {
            runOffloaded();
        } else {
            runVanilla();
        }
    }

    public void runOffloaded() {
        if (!(entryProcessor instanceof Offloadable)) {
            throw new HazelcastException("EntryProcessor is expected to implement Offloadable for this operation");
        }
        if (readOnly && entryProcessor.getBackupProcessor() != null) {
            throw new HazelcastException("EntryProcessor.getBackupProcessor() should return null if ReadOnly implemented");
        }

        boolean shouldCloneForOffloading = OBJECT.equals(mapContainer.getMapConfig().getInMemoryFormat());
        Object value = recordStore.get(dataKey, false);
        value = shouldCloneForOffloading ? toData(value) : value;

        String executorName = ((Offloadable) entryProcessor).getExecutorName();
        executorName = executorName.equals(Offloadable.OFFLOADABLE_EXECUTOR) ? OFFLOADABLE_EXECUTOR : executorName;

        if (readOnly) {
            runOffloadedReadOnlyEntryProcessor(value, executorName);
        } else {
            runOffloadedModifyingEntryProcessor(value, executorName);
        }
    }

    @SuppressWarnings("unchecked")
    private void runOffloadedReadOnlyEntryProcessor(final Object previousValue, String executorName) {
        ops.onStartAsyncOperation(this);
        getNodeEngine().getExecutionService().execute(executorName, new Runnable() {
            @Override
            public void run() {
                try {
                    final Map.Entry entry = createMapEntry(dataKey, previousValue);
                    final Data result = process(entry);
                    if (!noOp(entry, previousValue)) {
                        throw new HazelcastException("It is not allowed to modify an entry in a ReadOnly EntryProcessor");
                    }
                    getOperationResponseHandler().sendResponse(EntryOperation.this, result);
                } catch (Throwable t) {
                    getOperationResponseHandler().sendResponse(EntryOperation.this, t);
                } finally {
                    ops.onCompletionAsyncOperation(EntryOperation.this);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void runOffloadedModifyingEntryProcessor(final Object previousValue, String executorName) {
        final OperationServiceImpl ops = (OperationServiceImpl) getNodeEngine().getOperationService();

        final Data finalDataKey = dataKey;
        final String finalCaller = getCallerUuid();
        final long finalThreadId = threadId;
        final long finalCallId = getCallId();
        final long finalBegin = begin;

        // The off-loading uses local locks, since the locking is used only on primary-replica.
        // The locks are not supposed to be migrated on partition migration or partition promotion & downgrade.
        recordStore.localLock(finalDataKey, finalCaller, finalThreadId, finalCallId, -1);

        ops.onStartAsyncOperation(this);
        getNodeEngine().getExecutionService().execute(executorName, new Runnable() {
            @Override
            public void run() {
                try {
                    final Map.Entry entry = createMapEntry(dataKey, previousValue);
                    final Data result = process(entry);
                    if (!noOp(entry, previousValue)) {
                        Data newValue = toData(entry.getValue());

                        EntryEventType modificationType;
                        if (entry.getValue() == null) {
                            modificationType = REMOVED;
                        } else {
                            modificationType = (previousValue == null) ? ADDED : UPDATED;
                        }

                        updateAndUnlock(toData(previousValue), newValue, modificationType, finalCaller, finalThreadId,
                                result, finalBegin);
                    } else {
                        unlockOnly(result, finalCaller, finalThreadId, finalBegin);
                    }
                } catch (Throwable t) {
                    unlockOnly(t, finalCaller, finalThreadId, finalBegin);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void updateAndUnlock(Data previousValue, Data newValue, EntryEventType modificationType, String caller,
                                 long threadId, final Object result, long now) {
        EntryOffloadableSetUnlockOperation updateOperation = new EntryOffloadableSetUnlockOperation(name, modificationType,
                dataKey, previousValue, newValue, caller, threadId, now, entryProcessor.getBackupProcessor());

        updateOperation.setPartitionId(getPartitionId());
        ops.invokeOnPartition(updateOperation).andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                try {
                    getOperationResponseHandler().sendResponse(EntryOperation.this, result);
                } finally {
                    ops.onCompletionAsyncOperation(EntryOperation.this);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    // EntryOffloadableLockMismatchException is a marker send from the EntryOffloadableSetUnlockOperation
                    // meaning that the whole invocation of the EntryOffloadableOperation should be retried
                    if (t instanceof EntryOffloadableLockMismatchException) {
                        t = new RetryableHazelcastException(t.getMessage(), t);
                    }
                    getOperationResponseHandler().sendResponse(EntryOperation.this, t);
                } finally {
                    ops.onCompletionAsyncOperation(EntryOperation.this);
                }
            }
        });
    }

    private void unlockOnly(final Object result, String caller, long threadId, long now) {
        updateAndUnlock(null, null, null, caller, threadId, result, now);
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        if (offloading) {
            // This is required since if the returnsResponse() method returns false there won't be any response sent
            // to the invoking party - this means that the operation won't be retried if the exception is instanceof
            // HazelcastRetryableException
            sendResponse(e);
        } else {
            super.onExecutionFailure(e);
        }
    }

    @Override
    public boolean returnsResponse() {
        if (offloading) {
            // This has to be false, since the operation uses the deferred-response mechanism.
            // This method returns false, but the response will be send later on using the response handler
            return false;
        } else {
            return super.returnsResponse();
        }
    }

    private void runVanilla() {
        final long now = getNow();
        boolean shouldClone = mapContainer.shouldCloneOnEntryProcessing();
        SerializationService serializationService = getNodeEngine().getSerializationService();
        oldValue = recordStore.get(dataKey, false);
        Object value = shouldClone ? serializationService.toObject(serializationService.toData(oldValue)) : oldValue;

        Map.Entry entry = createMapEntry(dataKey, value);

        response = process(entry);

        // first call noOp, other if checks below depends on it.
        if (noOp(entry, oldValue)) {
            return;
        }

        // at this stage we see that the entry has been modified which is not allowed for readOnly processors
        if (entryProcessor instanceof ReadOnly) {
            throw new HazelcastException("ReadOnly processor is not allowed to make any changes to the processed Entry");
        }

        if (entryRemoved(entry, now)) {
            return;
        }
        entryAddedOrUpdated(entry, now);
    }

    @Override
    public void afterRun() throws Exception {
        if (!offloading) {
            super.afterRun();
            if (eventType == null) {
                return;
            }
            mapServiceContext.interceptAfterPut(name, dataValue);
            if (isPostProcessing(recordStore)) {
                Record record = recordStore.getRecord(dataKey);
                dataValue = record == null ? null : record.getValue();
            }
            invalidateNearCache(dataKey);
            publishEntryEvent();
            publishWanReplicationEvent();
            evict(dataKey);
        }
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DefaultObjectNamespace(MapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public boolean shouldWait() {
        // optimisation for ReadOnly processors -> they will not wait for the lock
        if (entryProcessor instanceof ReadOnly) {
            return false;
        }
        return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public Object getResponse() {
        if (offloading) {
            return null;
        }
        return response;
    }

    @Override
    public Operation getBackupOperation() {
        if (offloading) {
            return null;
        }
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new EntryBackupOperation(name, dataKey, backupProcessor) : null;
    }

    @Override
    public boolean shouldBackup() {
        if (offloading) {
            return false;
        }
        return mapContainer.getTotalBackupCount() > 0 && entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    private long getLatencyFrom(long begin) {
        return Clock.currentTimeMillis() - begin;
    }

    private Data toData(Object obj) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.toData(obj);
    }

    private boolean entryRemoved(Map.Entry entry, long now) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.delete(dataKey);
            getLocalMapStats().incrementRemoves(getLatencyFrom(now));
            eventType = REMOVED;
            return true;
        }
        return false;
    }

    /**
     * Only difference between add and update is event type to be published.
     */
    private void entryAddedOrUpdated(Map.Entry entry, long now) {
        dataValue = entry.getValue();
        recordStore.set(dataKey, dataValue, DEFAULT_TTL);

        getLocalMapStats().incrementPuts(getLatencyFrom(now));

        eventType = oldValue == null ? ADDED : UPDATED;
    }

    private Data process(Map.Entry entry) {
        final Object result = entryProcessor.process(entry);
        return toData(result);
    }

    private boolean hasRegisteredListenerForThisMap() {
        final EventService eventService = getNodeEngine().getEventService();
        return eventService.hasEventRegistration(SERVICE_NAME, name);
    }

    /**
     * Nullify old value if in memory format is object and operation is not removal
     * since old and new value in fired event {@link com.hazelcast.core.EntryEvent}
     * may be same due to the object in memory format.
     */
    private void nullifyOldValueIfNecessary() {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final InMemoryFormat format = mapConfig.getInMemoryFormat();
        if (format == InMemoryFormat.OBJECT && eventType != REMOVED) {
            oldValue = null;
        }
    }

    private void publishEntryEvent() {
        if (hasRegisteredListenerForThisMap()) {
            nullifyOldValueIfNecessary();
            mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, dataKey, oldValue, dataValue);
        }
    }

    private void publishWanReplicationEvent() {
        final MapContainer mapContainer = this.mapContainer;
        if (mapContainer.getWanReplicationPublisher() == null
                && mapContainer.getWanMergePolicy() == null) {
            return;
        }
        final Data key = dataKey;

        if (REMOVED.equals(eventType)) {
            mapEventPublisher.publishWanReplicationRemove(name, key, getNow());
        } else {
            final Record record = recordStore.getRecord(key);
            if (record != null) {
                dataValue = toData(dataValue);
                final EntryView entryView = createSimpleEntryView(key, dataValue, record);
                mapEventPublisher.publishWanReplicationUpdate(name, entryView);
            }
        }
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
        return MapDataSerializerHook.ENTRY_OPERATION;
    }
}
