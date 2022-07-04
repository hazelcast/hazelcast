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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.ExecutorStats;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.executionservice.impl.StatsAwareRunnable;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.core.Offloadable.NO_OFFLOADING;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;
import static com.hazelcast.map.impl.record.Record.UNSET;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.OFFLOADABLE_EXECUTOR;
import static com.hazelcast.spi.impl.operationservice.CallStatus.RESPONSE;
import static com.hazelcast.spi.impl.operationservice.CallStatus.WAIT;
import static com.hazelcast.spi.impl.operationservice.InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
 * ### Offloadable (for reading &amp; writing)
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
 * 1. partition-thread (fetch &amp; lock)
 * 2. execution-thread (process)
 * 3. partition-thread (set &amp; unlock, or just unlock if no changes)
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
 * if there is a change to the entry -&gt; exception is thrown
 * if there is no change to the entry -&gt; the result is returned to the user from the executor-thread.
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
 * Offloading will not be applied to backup partitions. It is possible to initialize the entry backup processor
 * with some input provided by the EntryProcessor in the EntryProcessor.getBackupProcessor() method.
 * The input allows providing context to the entry backup processor - for example the "delta"
 * so that the entry backup processor does not have to calculate the "delta" but it may just apply it.
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
public class
EntryOperation extends LockAwareOperation
        implements BackupAwareOperation, BlockingOperation, MutatingOperation {

    private static final int SET_UNLOCK_FAST_RETRY_LIMIT = 10;

    private EntryProcessor entryProcessor;

    private transient boolean offload;

    // EntryOperation
    private transient Object response;

    // EntryOffloadableOperation
    private transient boolean readOnly;
    private transient int setUnlockRetryCount;
    private transient long begin;

    public EntryOperation() {
    }

    public EntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        this.begin = Clock.currentTimeMillis();
        this.readOnly = entryProcessor instanceof ReadOnly;

        SerializationService serializationService = getNodeEngine().getSerializationService();
        ManagedContext managedContext = serializationService.getManagedContext();
        entryProcessor = (EntryProcessor) managedContext.initialize(entryProcessor);
    }

    @Override
    public CallStatus call() {
        if (shouldWait()) {
            return WAIT;
        }
        // when offloading is enabled, left disposing
        // to EntryOffloadableSetUnlockOperation
        disposeDeferredBlocks = !offload;

        if (offload) {
            return new EntryOperationOffload(getCallerAddress());
        } else {
            response = operator(this, entryProcessor)
                    .operateOnKey(dataKey)
                    .doPostOperateOps()
                    .getResult();
            return RESPONSE;
        }
    }

    @Override
    protected void runInternal() {
        // NOP
    }

    @Override
    public boolean shouldWait() {
        // optimisation for ReadOnly processors -> they will not wait for the lock
        if (entryProcessor instanceof ReadOnly) {
            offload = isOffloadingRequested(entryProcessor);
            return false;
        }
        // mutating offload -> only if key not locked, since it uses locking too (but on reentrant one)
        if (!recordStore.isLocked(dataKey) && isOffloadingRequested(entryProcessor)) {
            offload = true;
            return false;
        }
        //at this point we cannot offload. the entry is locked or the EP does not support offload
        //if the entry is locked by us then we can still run the EP on the partition thread
        offload = false;
        return super.shouldWait();
    }

    private boolean isOffloadingRequested(EntryProcessor entryProcessor) {
        if (entryProcessor instanceof Offloadable) {
            String executorName = ((Offloadable) entryProcessor).getExecutorName();
            return !executorName.equals(NO_OFFLOADING);
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public Object getResponse() {
        if (offload) {
            return null;
        }
        return response;
    }

    @Override
    public boolean returnsResponse() {
        if (offload) {
            // This has to be false, since the operation uses the
            // deferred-response mechanism. This method returns false, but
            // the response will be send later on using the response handler
            return false;
        } else {
            return super.returnsResponse();
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        if (offload) {
            // This is required since if the returnsResponse() method returns
            // false there won't be any response sent to the invoking
            // party - this means that the operation won't be retried if
            // the exception is instanceof HazelcastRetryableException
            sendResponse(e);
        } else {
            super.onExecutionFailure(e);
        }
    }

    @Override
    @SuppressFBWarnings(
            value = {"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"},
            justification = "backupProcessor can indeed be null so check is not redundant")
    public Operation getBackupOperation() {
        if (offload) {
            return null;
        }
        EntryProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new EntryBackupOperation(name, dataKey, backupProcessor) : null;
    }

    @Override
    public boolean shouldBackup() {
        if (offload) {
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
    public int getClassId() {
        return MapDataSerializerHook.ENTRY_OPERATION;
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

    private final class EntryOperationOffload extends Offload {
        private Address callerAddress;

        private EntryOperationOffload(Address callerAddress) {
            super(EntryOperation.this);
            this.callerAddress = callerAddress;
        }

        @Override
        public void start() {
            verifyEntryProcessor();

            Object oldValue = getOldValueByInMemoryFormat();
            String executorName = ((Offloadable) entryProcessor).getExecutorName();
            executorName = executorName.equals(Offloadable.OFFLOADABLE_EXECUTOR) ? OFFLOADABLE_EXECUTOR : executorName;

            if (readOnly) {
                executeReadOnlyEntryProcessor(oldValue, executorName);
            } else {
                executeMutatingEntryProcessor(oldValue, executorName);
            }
        }

        private Object getOldValueByInMemoryFormat() {
            Object oldValue = recordStore.get(dataKey, false, callerAddress);
            InMemoryFormat inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();
            switch (inMemoryFormat) {
                case NATIVE:
                    return toHeapData((Data) oldValue);
                case OBJECT:
                    return serializationService.toData(oldValue);
                case BINARY:
                    return oldValue;
                default:
                    throw new IllegalArgumentException("Unknown in memory format: " + inMemoryFormat);
            }
        }

        private void verifyEntryProcessor() {
            if (!(entryProcessor instanceof Offloadable)) {
                throw new HazelcastException("EntryProcessor is expected to implement Offloadable for this operation");
            }
            if (readOnly && entryProcessor.getBackupProcessor() != null) {
                throw new HazelcastException("EntryProcessor.getBackupProcessor() should return null if ReadOnly implemented");
            }
        }

        @SuppressWarnings("unchecked")
        private void executeReadOnlyEntryProcessor(final Object oldValue, String executorName) {
            doExecute(executorName, () -> {
                try {
                    Data result = operator(EntryOperation.this, entryProcessor)
                            .operateOnKeyValue(dataKey, oldValue).getResult();
                    sendResponse(result);
                } catch (Throwable t) {
                    sendResponse(t);
                }
            });
        }

        @SuppressWarnings("unchecked")
        private void executeMutatingEntryProcessor(final Object oldValue, String executorName) {
            // callerId is random since the local locks are NOT re-entrant
            // using a randomID every time prevents from re-entering the already acquired lock
            final UUID finalCaller = UuidUtil.newUnsecureUUID();
            final Data finalDataKey = dataKey;
            final long finalThreadId = threadId;
            final long finalCallId = getCallId();
            final long finalBegin = begin;

            // The off-loading uses local locks, since the locking is used only on primary-replica.
            // The locks are not supposed to be migrated on partition migration or partition promotion & downgrade.
            lock(finalDataKey, finalCaller, finalThreadId, finalCallId);

            try {
                doExecute(executorName, () -> {
                    try {
                        EntryOperator entryOperator = operator(EntryOperation.this, entryProcessor)
                                .operateOnKeyValue(dataKey, oldValue);
                        Data result = entryOperator.getResult();
                        EntryEventType modificationType = entryOperator.getEventType();
                        if (modificationType != null) {
                            long newTtl = entryOperator.getEntry().getNewTtl();
                            Data newValue = serializationService.toData(entryOperator.getByPreferringDataNewValue());
                            updateAndUnlock(serializationService.toData(oldValue),
                                    newValue, modificationType, newTtl, finalCaller, finalThreadId, result, finalBegin);
                        } else {
                            unlockOnly(result, finalCaller, finalThreadId, finalBegin);
                        }
                    } catch (Throwable t) {
                        getLogger().severe("Unexpected error on Offloadable execution", t);
                        unlockOnly(t, finalCaller, finalThreadId, finalBegin);
                    }
                });
            } catch (Throwable t) {
                unlock(finalDataKey, finalCaller, finalThreadId, finalCallId, t);
                sneakyThrow(t);
            }
        }

        private void doExecute(String executorName, Runnable runnable) {
            boolean statisticsEnabled = mapContainer.getMapConfig().isStatisticsEnabled();
            ExecutorStats executorStats = mapServiceContext.getOffloadedEntryProcessorExecutorStats();
            try {
                Runnable command = statisticsEnabled
                        ? new StatsAwareRunnable(runnable, executorName, executorStats) : runnable;
                executionService.execute(executorName, command);
            } catch (RejectedExecutionException e) {
                if (statisticsEnabled) {
                    executorStats.rejectExecution(executorName);
                }

                throw e;
            }
        }

        private void lock(Data finalDataKey, UUID finalCaller, long finalThreadId, long finalCallId) {
            boolean locked = recordStore.localLock(finalDataKey, finalCaller, finalThreadId, finalCallId, -1);
            if (!locked) {
                // should not happen since it's a lock-awaiting operation and we are on a partition-thread, but just to make sure
                throw new IllegalStateException(
                        format("Could not obtain a lock by the caller=%s and threadId=%d", finalCaller, threadId));
            }
        }

        private void unlock(Data finalDataKey, UUID finalCaller, long finalThreadId, long finalCallId, Throwable cause) {
            boolean unlocked = recordStore.unlock(finalDataKey, finalCaller, finalThreadId, finalCallId);
            if (!unlocked) {
                throw new IllegalStateException(
                        format("Could not unlock by the caller=%s and threadId=%d", finalCaller, threadId), cause);
            }
        }

        private void unlockOnly(final Object result, UUID caller, long threadId, long now) {
            updateAndUnlock(null, null, null, UNSET, caller, threadId, result, now);
        }

        @SuppressWarnings({"unchecked", "checkstyle:methodlength"})
        private void updateAndUnlock(Data previousValue, Data newValue,
                                     EntryEventType modificationType,
                                     long newTtl, UUID caller,
                                     long threadId, final Object result, long now) {
            EntryOffloadableSetUnlockOperation updateOperation = new EntryOffloadableSetUnlockOperation(name, modificationType,
                    newTtl, dataKey, previousValue, newValue, caller,
                    threadId, now, entryProcessor.getBackupProcessor());

            updateOperation.setPartitionId(getPartitionId());
            updateOperation.setReplicaIndex(0);
            updateOperation.setNodeEngine(nodeEngine);
            updateOperation.setCallerUuid(getCallerUuid());
            OperationAccessor.setCallerAddress(updateOperation, getCallerAddress());
            @SuppressWarnings("checkstyle:anoninnerlength")
            OperationResponseHandler setUnlockResponseHandler = new OperationResponseHandler() {
                @Override
                public void sendResponse(Operation op, Object response) {
                    if (isRetryable(response) || isTimeout(response)) {
                        retry(op);
                    } else {
                        EntryOperation.this.sendResponse(toResponse(response));
                    }
                }

                private void retry(final Operation op) {
                    setUnlockRetryCount++;
                    if (isFastRetryLimitReached()) {
                        executionService.schedule(() -> operationService.execute(op), DEFAULT_TRY_PAUSE_MILLIS, MILLISECONDS);
                    } else {
                        operationService.execute(op);
                    }
                }

                private Object toResponse(Object response) {
                    if (response instanceof Throwable) {
                        Throwable t = (Throwable) response;
                        // EntryOffloadableLockMismatchException is a marker send from the EntryOffloadableSetUnlockOperation
                        // meaning that the whole invocation of the EntryOffloadableOperation should be retried
                        if (t instanceof EntryOffloadableLockMismatchException) {
                            t = new RetryableHazelcastException(t.getMessage(), t);
                        }
                        return t;
                    } else {
                        return result;
                    }
                }
            };

            updateOperation.setOperationResponseHandler(setUnlockResponseHandler);
            operationService.execute(updateOperation);
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
    }
}
