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

package com.hazelcast.vector.internal.impl.ops;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.SelfResponseOperation;
import com.hazelcast.spi.impl.operationservice.impl.OperationRunnerImpl;
import com.hazelcast.vector.impl.DataVectorDocument;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;
import com.hazelcast.vector.internal.impl.service.VectorCollectionServiceImpl;

import java.io.IOException;

/**
 * Inserts the {@link VectorEntries} for a single partition to the
 * local {@link com.hazelcast.map.impl.recordstore.RecordStore}.
 * <p>
 * Used to reduce the number of remote invocations
 * of an {@link com.hazelcast.vector.VectorCollection#putAllAsync} call.
 * <p>
 * This operation may not execute atomically if {@link VectorCollectionServiceImpl#MAX_SUCCESSIVE_OFFLOADED_OP_RUN_MILLIS}
 * is configured.
 * <p>
 * Marked as {@link SelfResponseOperation} due to responses being expected for offloaded operations.
 */
public class PutAllOperation extends BaseMutatingOperation
        implements PartitionAwareOperation, MutatingOperation, BackupAwareOperation, SelfResponseOperation {

    private VectorEntries vectorEntries;
    private transient VectorCollectionServiceImpl service;
    // tracks current progress between offloaded calls
    private transient int currentIndex;

    public PutAllOperation() {
    }

    public PutAllOperation(String name, VectorEntries vectorEntries) {
        super(name);
        this.vectorEntries = vectorEntries;
    }

    @Override
    public void beforeRun() {
        super.beforeRun();
        service = getService();
    }

    @Override
    public CallStatus call() throws Exception {
        if (waited) {
            getLogger().finest("Executing after waiting %s", this);
        }

        // Offloading means that the operation may fail in the middle
        // if a migration is initiated, with only some records added. This does not happen
        // without offloads, when entire operation executes in one go on partition thread.

        if (shouldWait()) {
            // Note that in case of offloaded putAll execution waiting happens only at the beginning.
            // Subsequently scheduled batches cannot wait and will fail if optimization starts in the
            // meantime. This is currently kept this way as putAll offloading is an experiment.
            return CallStatus.WAIT;
        }

        if (isOffloadable()) {
            // potentially offload
            putBatch();
            if (!isDone()) {
                return new PutAllOffload();
            }
        } else {
            putAll();
        }

        // done without offload
        return CallStatus.RESPONSE;
    }

    private boolean isOffloadable() {
        // Offloaded execution is allowed only when there are no backups.
        // Otherwise, execution on backup could produce different results than on owner
        // if there were some interleaving operations on the same collection partition.
        return service.getMaxOffloadedRunNanos() > 0 && storage.getConfig().getTotalBackupCount() == 0;
    }

    private void putAll() {
        while (currentIndex < vectorEntries.size()) {
            DataVectorDocument value = vectorEntries.getDocument(currentIndex);
            storage.set(vectorEntries.getKey(currentIndex), value.getValue(), value.getVectors());
            currentIndex++;
        }
    }

    private void putBatch() {
        long start = System.nanoTime();
        while (currentIndex < vectorEntries.size()) {
            DataVectorDocument value = vectorEntries.getDocument(currentIndex);
            storage.set(vectorEntries.getKey(currentIndex), value.getValue(), value.getVectors());
            currentIndex++;
            if (System.nanoTime() - start >= service.getMaxOffloadedRunNanos()) {
                break;
            }
        }
    }

    private boolean isDone() {
        return currentIndex >= vectorEntries.size();
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean returnsResponse() {
        // offloaded execution will send response on its own
        // and if offloadable execution is not offloaded, the response will be sent even though returnsResponse() returns false
        //
        // if this method is invoked outside actual execution (without beforeRun) assume that it will be not offloaded.
        // this matters mostly for some asserts.
        return service == null || !isOffloadable();
    }

    @Override
    public boolean shouldBackup() {
        return storage.getConfig().getTotalBackupCount() > 0;
    }

    @Override
    public int getSyncBackupCount() {
        return storage.getConfig().getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return storage.getConfig().getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new PutAllBackupOperation(getName(), vectorEntries);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(vectorEntries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        vectorEntries = in.readObject();
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.PUT_ALL;
    }

    private class PutAllOffload extends Offload implements PartitionSpecificRunnable {

        private final OperationRunnerImpl partitionOperationRunner;

        PutAllOffload() {
            super(PutAllOperation.this);
            partitionOperationRunner = getPartitionOperationRunner();
        }

        @Override
        public void start() {
            // first batch was just executed on partition thread, schedule next one
            schedule();
        }

        private void schedule() {
            this.operationService.execute(this);
        }

        @Override
        public void run() {
            try {
                // check if it is still valid to run. We are running in PartitionSpecificRunnable,
                // these checks are not performed for them.
                partitionOperationRunner.metWithPreconditions(PutAllOperation.this);

                putBatch();

                if (isDone()) {
                    sendResponse(true);
                } else {
                    // schedule next batch of entries
                    schedule();
                }
            } catch (Throwable t) {
                // use standard operation exception handling
                partitionOperationRunner.handleOperationError(PutAllOperation.this, t);
            }
        }

        @Override
        public int getPartitionId() {
            return PutAllOperation.this.getPartitionId();
        }
    }

    private OperationRunnerImpl getPartitionOperationRunner() {
        return (OperationRunnerImpl) getNodeEngine().getOperationService().getOperationExecutor()
                .getPartitionOperationRunners()[getPartitionId()];
    }
}
