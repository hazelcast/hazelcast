/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationparker.OperationParker;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.vector.impl.VectorCollectionOptimizationManager;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;
import com.hazelcast.vector.impl.VectorCollectionService;
import com.hazelcast.vector.impl.storage.VectorCollectionStorage;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Consumer;

public abstract class BaseOptimizeOperation extends AbstractNamedOperation
        implements PartitionAwareOperation, ServiceNamespaceAware, BlockingOperation {
    protected UUID uuid;
    protected String indexName;

    @Nullable
    protected transient VectorCollectionStorage storage;

    @Nullable
    protected transient Consumer<Operation> backupOpAfterRun;

    // if the operation waited before
    protected transient boolean waited;

    /**
     * {@link #indexName} can be null meaning the only index. This field always contains
     * index name as defined in {@link com.hazelcast.config.vector.VectorIndexConfig}.
     * It can be null for unnamed index.
     */
    @Nullable
    protected transient String actualIndexName;

    protected BaseOptimizeOperation() {
    }

    protected BaseOptimizeOperation(UUID uuid, String vectorCollectionName, String indexName) {
        super(vectorCollectionName);
        this.uuid = uuid;
        this.indexName = indexName;
    }

    @Override
    public void beforeRun() {
        storage = getVectorCollectionService().getStorageOrNull(name, getPartitionId());
        actualIndexName = storage != null ? storage.validateAndGetIndexName(indexName) : null;
    }

    @Override
    public CallStatus call() throws Exception {
        if (storage == null) {
            return CallStatus.RESPONSE;
        }

        // first check if index is locked - this will not change during this invocation
        // If this index is locked by another optimization, we will be notified by coarser
        // VectorOptimizeWaitNotifyKey which is sufficient though may be slightly redundant
        // if there are many collections with multiple indexes on which optimizations
        // are executed concurrently and there are many permits configured.
        if (storage.isIndexLocked(indexName)) {
            if (waited) {
                // this will result in out-of-order execution and increased latency
                getLogger().fine("Moving to the end of the queue because the index is locked: %s", this);
            }
            getLogger().finest("Index is locked, waiting with key %s, operation %s", getWaitKey(), this);
            waited = true;
            return CallStatus.WAIT;
        }

        if (!getOptimizationManager().tryAcquireOptimizationPermit()) {
            if (waited) {
                // this will result in out-of-order execution and increased latency
                getLogger().fine("Moving to the end of the queue because did not get permit: %s", this);
            }
            getLogger().finest("Did not get permit, waiting with key %s, operation %s", getWaitKey(), this);
            waited = true;
            return CallStatus.WAIT;
        } else {
            getLogger().finest("Got permit %s", this);
        }

        if (waited) {
            getLogger().finest("Executing after waiting %s", this);
        }

        return new IndexOptimizeOffloaded();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return getOptimizationManager().getOptimizePartitionWaitNotifyKey(getPartitionId());
    }

    @Override
    public boolean shouldWait() {
        // this will be invoked from WaitSet to check if this operation can be scheduled again
        // but it should not yet grab the permit, because some other conditions may not be met
        return storage.isIndexLocked(indexName)
                || !getOptimizationManager().hasAvailableOptimizationPermit();
    }

    @Override
    public void onWaitExpire() {
        // this should not happen as at present vector collection does not use wait timeout
        // but return meaningful response just in case.
        sendResponse(new OperationTimeoutException("Timeout elapsed while waiting for index to be unlocked"));
    }

    @Override
    public String getServiceName() {
        return VectorCollectionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, uuid);
        out.writeString(indexName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.uuid = UUIDSerializationUtil.readUUID(in);
        this.indexName = in.readString();
    }

    @Override
    public ObjectNamespace getServiceNamespace() {
        return getVectorCollectionService().getObjectNamespace(getName());
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", uuid=").append(uuid)
                .append(", indexName=").append(indexName)
                .append(", actualIndexName=").append(actualIndexName);
    }

    private final class IndexOptimizeOffloaded extends Offload {

        private IndexOptimizeOffloaded() {
            super(BaseOptimizeOperation.this);
        }

        @Override
        public void start() {
            // it was previously checked on partition thread that index is not locked (in call/shouldWait)
            // and the execution is still on partition thread so lockIndexMutation should not throw.
            // if it throws, it is a bug in locking logic
            storage.lockIndexMutation(indexName, uuid);

            ILogger logger = getLogger();
            logger.finest("Locked index for %s", BaseOptimizeOperation.this);
            var blocked = Timer.nanos();

            nodeEngine.getExecutionService().execute(
                    ExecutionService.OFFLOADABLE_EXECUTOR,
                    () -> {
                        try {
                            try {
                                var start = Timer.nanos();
                                storage.optimize(indexName);
                                var elapsed = Timer.millisElapsed(start);
                                logger.fine("Optimization on index %s collection %s partitionId=%d took %d ms ",
                                        actualIndexName, name, getPartitionId(), elapsed);
                            } finally {
                                // order is important. Unblock index first so it can be immediately locked again
                                // by any waiting operation unparked as a result of notifyAfterOptimization
                                storage.unlockIndexMutation(indexName);
                                var blockedMs = Timer.millisElapsed(blocked);
                                logger.fine("Index %s on collection %s partitionId=%d was blocked for %d ms ",
                                        actualIndexName, name, getPartitionId(), blockedMs);

                                getOptimizationManager().releaseOptimizationPermit();

                                notifyAfterOptimization();
                            }

                            // handleBackupAndSendResponse makes sense only for OptimizeOperation,
                            // backupOpAfterRun only for OptimizeBackupOperation, but they are ignored
                            // when not needed.
                            BackupUtil.handleBackupAndSendResponse(BaseOptimizeOperation.this);

                            if (backupOpAfterRun != null) {
                                // we were waiting - send backup acks
                                backupOpAfterRun.accept(BaseOptimizeOperation.this);
                            }
                        } catch (Throwable t) {
                            onExecutionFailure(t);
                            // do not send backups for failures
                            sendResponse(t);
                        }
                    }
            );
        }
    }

    /**
     * Notify optimizations on all partition threads and mutating operations on partition
     * that the optimization has finished.
     */
    private void notifyAfterOptimization() {
        // thread which is assigned to the partition
        var operationThreadId = getOperationExecutor().getPartitionThreadId(getPartitionId());
        ILogger logger = getLogger();

        getOperationExecutor().executeOnPartitionThreads(() -> {
            OperationParker operationParker = ((NodeEngineImpl) getNodeEngine()).getOperationParker();
            var threadId = ((PartitionOperationThread) Thread.currentThread()).getThreadId();

            // notify mutating operations on appropriate partition thread.
            // mutating operations are notified before optimizations to give them better latency.
            if (threadId == operationThreadId) {
                VectorMutationWaitNotifyKey mutationWaitNotifyKey = new VectorMutationWaitNotifyKey(
                        getName(), actualIndexName, getPartitionId());
                logger.finest("Notifying %s from partitionId=%d", mutationWaitNotifyKey, getPartitionId());
                operationParker.unpark(mutationWaitNotifyKey);
            }

            // notify other waiting optimizations on all partition threads
            var optimizationWaitNotifyKey = getOptimizationManager().getOptimizeWaitNotifyKey(threadId);
            logger.finest("Notifying %s from partitionId=%d", optimizationWaitNotifyKey, getPartitionId());
            operationParker.unpark(optimizationWaitNotifyKey);
        });
    }

    private VectorCollectionService getVectorCollectionService() {
        return getService();
    }

    private VectorCollectionOptimizationManager getOptimizationManager() {
        return getVectorCollectionService().getOptimizationManager();
    }

    private OperationExecutor getOperationExecutor() {
        return getNodeEngine().getOperationService().getOperationExecutor();
    }
}
