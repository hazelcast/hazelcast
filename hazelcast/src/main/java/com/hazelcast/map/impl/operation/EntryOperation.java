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
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.core.Offloadable.NO_OFFLOADING;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;
import static com.hazelcast.spi.ExecutionService.OFFLOADABLE_EXECUTOR;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

/**
 * Contains implementation of the off-loadable contract for EntryProcessor execution on a single key.
 * <p>
 * ### Overview
 * <p>
 * Implementation of the the Offloadable contract for the EntryProcessor execution on a single key.
 * <p>
 * Allows off-loading the processing unit implementing this interface to the specified or default Executor.
 * Currently supported in:
 * <p>
 * IMap.executeOnKey(Object, EntryProcessor)
 * IMap.submitToKey(Object, EntryProcessor)
 * IMap.submitToKey(Object, EntryProcessor, ExecutionCallback)
 * <p>
 * ### Offloadable (for reading & writing)
 * <p>
 * If the EntryProcessor implements the Offloadable interface the processing will be offloaded to the given
 * ExecutorService allowing unblocking the partition-thread. The key will be locked for the time-span of the processing
 * in order to not generate a write-conflict.
 * <p>
 * If the EntryProcessor implements Offloadable the invocation scenario looks as follows:
 * - EntryOperation fetches the entry and locks the given key on partition-thread
 * - Then the processing is offloaded to the given executor
 * - When the processing finishes
 * if there is a change to the entry, a EntryOffloadableSetUnlockOperation is spawned
 * which sets the new value and unlocks the given key on partition-thread
 * if there is no change to the entry, a UnlockOperation is spawned, which just unlocks the kiven key
 * on partition thread
 * <p>
 * There will not be a conflict on a write due to the pessimistic locking of the key.
 * The threading looks as follows:
 * <p>
 * 1. partition-thread (fetch & lock)
 * 2. execution-thread (process)
 * 3. partition-thread (set & unlock, or just unlock if no changes)
 * <p>
 * ### Offloadable (for reading only)
 * <p>
 * If the EntryProcessor implements the Offloadable and ReadOnly interfaces the processing will be offloaded to the
 * givenExecutorService allowing unblocking the partition-thread. Since the EntryProcessor is not supposed to do any
 * changes to the Entry the key will NOT be locked for the time-span of the processing.
 * <p>
 * If the EntryProcessor implements Offloadable and ReadOnly the invocation scenario looks as follows:
 * - EntryOperation fetches the entry and DOES NOT lock the given key on partition-thread
 * - Then the processing is offloaded to the given executor
 * - When the processing finishes
 * if there is a change to the entry -> exception is thrown
 * if there is no change to the entry -> the result is returned to the user from the executor-thread.
 * <p>
 * In the read-only case the threading looks as follows:
 * <p>
 * 1. partition-thread (fetch)
 * 2. execution-thread (process)
 * <p>
 * ### Primary partition - main actors
 * <p>
 * - EntryOperation
 * - EntryOffloadableSetUnlockOperation
 * <p>
 * ### Backup partitions
 * <p>
 * Offloading will not be applied to backup partitions. It is possible to initialize the EntryBackupProcessor
 * with some input provided by the EntryProcessor in the EntryProcessor.getBackupProcessor() method.
 * The input allows providing context to the EntryBackupProcessor - for example the "delta"
 * so that the EntryBackupProcessor does not have to calculate the "delta" but it may just apply it.
 * <p>
 * ### Locking
 * <p>
 * The locking takes place only locally. If a partition locked by an off-loaded task gets migrated, the lock will not
 * be migrated. In this situation the off-loaded task "relying" on the lock will fail on the unlock operation, since it
 * will notice that there is no such a lock and therefore the processing for the key will get retried.
 * The reason behind is that the off-loadable backup-processing does not use locking there cannot be any transfer of
 * off-loadable locks from the primary replica to backup replicas.
 * <p>
 * GOTCHA: This operation LOADS missing keys from map-store, in contrast with PartitionWideEntryOperation.
 */
@SuppressWarnings("checkstyle:methodcount")
public class EntryOperation extends MutatingKeyBasedMapOperation implements BackupAwareOperation, BlockingOperation {

    private static final int SET_UNLOCK_FAST_RETRY_LIMIT = 10;

    private EntryProcessor entryProcessor;

    private transient boolean offloading;

    // EntryOperation
    private transient Object response;

    // EntryOffloadableOperation
    private transient boolean readOnly;
    private transient int setUnlockRetryCount;
    private transient long begin;
    private transient OperationServiceImpl ops;
    private transient ExecutionService exs;

    public EntryOperation() {
    }

    public EntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        this.ops = (OperationServiceImpl) getNodeEngine().getOperationService();
        this.exs = getNodeEngine().getExecutionService();
        this.begin = Clock.currentTimeMillis();
        this.readOnly = entryProcessor instanceof ReadOnly;

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
        Object oldValue = recordStore.get(dataKey, false);
        Object clonedOldValue = shouldCloneForOffloading ? toData(oldValue) : oldValue;

        String executorName = ((Offloadable) entryProcessor).getExecutorName();
        executorName = executorName.equals(Offloadable.OFFLOADABLE_EXECUTOR) ? OFFLOADABLE_EXECUTOR : executorName;

        if (readOnly) {
            runOffloadedReadOnlyEntryProcessor(clonedOldValue, executorName);
        } else {
            runOffloadedModifyingEntryProcessor(clonedOldValue, executorName);
        }
    }

    @SuppressWarnings("unchecked")
    private void runOffloadedReadOnlyEntryProcessor(final Object oldValue, String executorName) {
        ops.onStartAsyncOperation(this);
        getNodeEngine().getExecutionService().execute(executorName, new Runnable() {
            @Override
            public void run() {
                try {
                    Data result = operator(EntryOperation.this, entryProcessor)
                            .operateOnKeyValue(dataKey, oldValue).getResult();
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
    private void runOffloadedModifyingEntryProcessor(final Object oldValue, String executorName) {
        final OperationServiceImpl ops = (OperationServiceImpl) getNodeEngine().getOperationService();

        // callerId is random since the local locks are NOT re-entrant
        // using a randomID every time prevents from re-entering the already acquired lock
        final String finalCaller = UuidUtil.newUnsecureUuidString();
        final Data finalDataKey = dataKey;
        final long finalThreadId = threadId;
        final long finalCallId = getCallId();
        final long finalBegin = begin;

        // The off-loading uses local locks, since the locking is used only on primary-replica.
        // The locks are not supposed to be migrated on partition migration or partition promotion & downgrade.
        lock(finalDataKey, finalCaller, finalThreadId, finalCallId);

        try {
            ops.onStartAsyncOperation(this);
            getNodeEngine().getExecutionService().execute(executorName, new Runnable() {
                @Override
                public void run() {
                    try {
                        EntryOperator entryOperator = operator(EntryOperation.this, entryProcessor)
                                .operateOnKeyValue(dataKey, oldValue);
                        Data result = entryOperator.getResult();
                        EntryEventType modificationType = entryOperator.getEventType();
                        if (modificationType != null) {
                            Data newValue = toData(entryOperator.getNewValue());
                            updateAndUnlock(toData(oldValue), newValue, modificationType, finalCaller, finalThreadId,
                                    result, finalBegin);
                        } else {
                            unlockOnly(result, finalCaller, finalThreadId, finalBegin);
                        }
                    } catch (Throwable t) {
                        getLogger().severe("Unexpected error on Offloadable execution", t);
                        unlockOnly(t, finalCaller, finalThreadId, finalBegin);
                    }
                }
            });
        } catch (Throwable t) {
            try {
                unlock(finalDataKey, finalCaller, finalThreadId, finalCallId, t);
                sneakyThrow(t);
            } finally {
                ops.onCompletionAsyncOperation(this);
            }
        }
    }

    private Data toData(Object obj) {
        return mapServiceContext.toData(obj);
    }

    private void lock(Data finalDataKey, String finalCaller, long finalThreadId, long finalCallId) {
        boolean locked = recordStore.localLock(finalDataKey, finalCaller, finalThreadId, finalCallId, -1);
        if (!locked) {
            // should not happen since it's a lock-awaiting operation and we are on a partition-thread, but just to make sure
            throw new IllegalStateException(
                    String.format("Could not obtain a lock by the caller=%s and threadId=%d", finalCaller, threadId));
        }
    }

    private void unlock(Data finalDataKey, String finalCaller, long finalThreadId, long finalCallId, Throwable cause) {
        boolean unlocked = recordStore.unlock(finalDataKey, finalCaller, finalThreadId, finalCallId);
        if (!unlocked) {
            throw new IllegalStateException(
                    String.format("Could not unlock by the caller=%s and threadId=%d", finalCaller, threadId), cause);
        }
    }

    @SuppressWarnings({"unchecked", "checkstyle:methodlength"})
    private void updateAndUnlock(Data previousValue, Data newValue, EntryEventType modificationType, String caller,
                                 long threadId, final Object result, long now) {
        EntryOffloadableSetUnlockOperation updateOperation = new EntryOffloadableSetUnlockOperation(name, modificationType,
                dataKey, previousValue, newValue, caller, threadId, now, entryProcessor.getBackupProcessor());

        updateOperation.setPartitionId(getPartitionId());
        updateOperation.setReplicaIndex(0);
        updateOperation.setNodeEngine(getNodeEngine());
        updateOperation.setCallerUuid(getCallerUuid());
        OperationAccessor.setCallerAddress(updateOperation, getCallerAddress());
        @SuppressWarnings("checkstyle:anoninnerlength")
        OperationResponseHandler setUnlockResponseHandler = new OperationResponseHandler() {
            @Override
            public void sendResponse(Operation op, Object response) {
                if (isRetryable(response) || isTimeout(response)) {
                    retry(op);
                } else {
                    handleResponse(response);
                }
            }

            private void retry(final Operation op) {
                setUnlockRetryCount++;
                if (isFastRetryLimitReached()) {
                    exs.schedule(new Runnable() {
                        @Override
                        public void run() {
                            ops.execute(op);
                        }
                    }, DEFAULT_TRY_PAUSE_MILLIS, TimeUnit.MILLISECONDS);
                } else {
                    ops.execute(op);
                }
            }

            private void handleResponse(Object response) {
                if (response instanceof Throwable) {
                    Throwable t = (Throwable) response;
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
                } else {
                    try {
                        getOperationResponseHandler().sendResponse(EntryOperation.this, result);
                    } finally {
                        ops.onCompletionAsyncOperation(EntryOperation.this);
                    }
                }
            }
        };
        updateOperation.setOperationResponseHandler(setUnlockResponseHandler);
        ops.execute(updateOperation);
    }

    private boolean isRetryable(Object response) {
        return response instanceof RetryableHazelcastException && !(response instanceof WrongTargetException);
    }

    private boolean isTimeout(Object response) {
        return response instanceof CallTimeoutResponse;
    }

    private boolean isFastRetryLimitReached() {
        return setUnlockRetryCount > SET_UNLOCK_FAST_RETRY_LIMIT;
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
        response = operator(this, entryProcessor)
                .operateOnKey(dataKey)
                .doPostOperateOps()
                .getResult();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }

    @Override
    public boolean shouldWait() {
        // optimisation for ReadOnly processors -> they will not wait for the lock
        if (entryProcessor instanceof ReadOnly) {
            offloading = isOffloadingRequested(entryProcessor);
            return false;
        }
        // mutating offloading -> only if key not locked, since it uses locking too (but on reentrant one)
        if (!recordStore.isLocked(dataKey) && isOffloadingRequested(entryProcessor)) {
            offloading = true;
            return false;
        }
        //at this point we cannot offload. the entry is locked or the EP does not support offloading
        //if the entry is locked by us then we can still run the EP on the partition thread
        offloading = false;
        return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
    }

    private boolean isOffloadingRequested(EntryProcessor entryProcessor) {
        if (entryProcessor instanceof Offloadable) {
            String executorName = ((Offloadable) entryProcessor).getExecutorName();
            if (!executorName.equals(NO_OFFLOADING)) {
                return true;
            }
        }
        return false;
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
