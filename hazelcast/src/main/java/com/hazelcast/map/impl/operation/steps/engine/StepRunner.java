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

import com.hazelcast.core.Offloadable;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import javax.annotation.Nullable;
import java.util.Set;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static java.lang.Thread.currentThread;

/**
 * <lu>
 * <li>Created per partition</li>
 * <li>Runs {@link Step}s which are supplied by {@link StepSupplier}</li>
 * <li>Must be thread safe since can be run by multiple threads</li>
 * </lu>
 */
public class StepRunner extends Offload
        implements PartitionSpecificRunnable {

    /**
     * Specific to partition threads.
     * <p>
     * This thread local flag is used to by-pass map-store
     * offloading mechanism in case of a remote call from
     * a running HazelcastInstanceAware entry processor.
     * <p>
     * Although it is not good practice and calling a
     * remote operation can result with deadlock for
     * writes, it is ok to call it for local reads.
     */
    public static final ThreadLocal<Boolean> CURRENTLY_EXECUTING_ON_PARTITION_THREAD
            = ThreadLocal.withInitial(() -> false);

    private final int partitionId;
    private final long maxRunNanos;
    private final Set<MapOperation> offloadedOperations;
    private final OperationExecutor operationExecutor;
    private final ExecutionService executionService;

    private volatile StepSupplier stepSupplier;

    // Acts as a local variable.
    private String currentExecutorName;

    public StepRunner(MapOperation mapOperation) {
        super(mapOperation);
        this.offloadedOperations = getOffloadedOperations(mapOperation);
        this.partitionId = mapOperation.getPartitionId();
        NodeEngine nodeEngine = mapOperation.getNodeEngine();
        this.operationExecutor = ((OperationServiceImpl) nodeEngine
                .getOperationService()).getOperationExecutor();
        this.executionService = nodeEngine.getExecutionService();
        this.maxRunNanos = mapOperation.getMapContainer()
                .getMapServiceContext().getMaxSuccessiveOffloadedOpRunNanos();
    }

    @Override
    public void start() throws Exception {
        Operation thisOp = offloadedOperation();
        addOpToOffloadedOps(((MapOperation) thisOp));

        if (isHeadOp()) {
            run();
        }
    }

    /**
     * @return true if current partition thread is
     * executing this step runner now, false otherwise.
     */
    public static boolean isStepRunnerCurrentlyExecutingOnPartitionThread() {
        return CURRENTLY_EXECUTING_ON_PARTITION_THREAD.get();
    }

    private boolean addOpToOffloadedOps(MapOperation op) {
        if (offloadedOperations.add(op)) {
            op.getRecordStore().incMapStoreOffloadedOperationsCount();
            return true;
        }

        return false;
    }

    private void setSteppedOpResponseHandler() {
        MapOperation op = stepSupplier.getOperation();
        OperationResponseHandler current = op.getOperationResponseHandler();

        if (current instanceof SteppedOpResponseHandler) {
            return;
        }

        op.setOperationResponseHandler(new SteppedOpResponseHandler(current));
    }

    private boolean isHeadOp() {
        return offloadedOperations.size() == 1;
    }

    /**
     * Runs all queued operations one by one.
     * <p>
     * For fair usage of partition thread, it
     * has a {@link #maxRunNanos} upper limit.
     */
    @Override
    @SuppressWarnings({"checkstyle:innerassignment",
            "checkstyle:CyclomaticComplexity"})
    public void run() {
        final boolean runningOnPartitionThread = isRunningOnPartitionThread();
        final long start = System.nanoTime();

        Runnable step;
        do {
            try {
                // set stepSupplier if it is not set yet
                // or get next step from step supplier
                if (stepSupplier == null || (step = stepSupplier.get()) == null) {
                    // set stepSupplier only on partition threads
                    if (runningOnPartitionThread) {
                        stepSupplier = getNextStepSupplierOrNull();
                        if (stepSupplier == null) {
                            return;
                        }
                        continue;
                    } else {
                        // if we are not on partition threads, submit
                        // this runnable to operation executor.
                        operationExecutor.execute(this);
                        return;
                    }
                }

                // If an already offloaded operation is retried on
                // member left, response handler is re-set by retry
                // mechanism. But this new response handler is not the
                // same response handler expected by offload mechanism.
                // As a consequence of this, offloaded operation cannot
                // be removed from offload-queue. To prevent this issue
                // we set response handler before running a step.
                if (runningOnPartitionThread) {
                    setSteppedOpResponseHandler();
                }

                // Try to run this step in this thread, otherwise
                // offload the step to relevant executor(it
                // is operation or general-purpose executor)
                if (!runDirect(step)) {
                    offloadRun(step, this);
                    return;
                }

                // Independent of the number of queued offloadedOperations,
                // this step-runner tries to run all queued operation in
                // one go. This may cause biased usage of partition thread
                // for the favour of operating map. To prevent this, one
                // can put max execution time-limit with `maxRunNanos`
                // setting, so partition operations of other maps don't
                // wait longer but if there is a few maps, this setting
                // can cause increased latencies as a side effect.
                // Default value of `maxRunNanos` is zero.
                if (maxRunNanos > 0 && runningOnPartitionThread
                        && System.nanoTime() - start >= maxRunNanos) {
                    step = stepSupplier.get();
                    if (step != null) {
                        offloadRun(step, this);
                        return;
                    }
                }
            } catch (Throwable throwable) {
                stepSupplier.handleOperationError(throwable);
            }
        } while (!currentThread().isInterrupted());
    }

    /**
     * @return null if no offloaded operations to execute, otherwise
     * create next step supplier for the next offloaded operation
     */
    @Nullable
    private StepSupplier getNextStepSupplierOrNull() {
        for (MapOperation operation : offloadedOperations) {
            return new StepSupplier(operation);
        }

        return null;
    }

    private boolean runDirect(Runnable step) {
        if (step instanceof PartitionSpecificRunnable) {
            if (isRunningOnPartitionThread()) {
                try {
                    CURRENTLY_EXECUTING_ON_PARTITION_THREAD.set(true);
                    step.run();
                } finally {
                    CURRENTLY_EXECUTING_ON_PARTITION_THREAD.set(false);
                }
                return true;
            }
        } else {
            // currentExecutorName can be null, if first step is offload step.
            if (!isRunningOnPartitionThread()
                    && (currentExecutorName == null
                    || ((Offloadable) step).getExecutorName().equals(currentExecutorName))) {
                step.run();
                return true;
            }
        }

        return false;
    }

    private void offloadRun(Runnable step,
                            PartitionSpecificRunnable offload) {
        if (step instanceof PartitionSpecificRunnable) {
            operationExecutor.execute(offload);
        } else {
            Offloadable offloadableStep = (Offloadable) step;
            currentExecutorName = offloadableStep.getExecutorName();
            executionService.getExecutor(currentExecutorName)
                    .execute(offload);
        }
    }

    private Set<MapOperation> getOffloadedOperations(MapOperation mapOperation) {
        return mapOperation.getRecordStore().getOffloadedOperations();
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Response handler of an operation which
     * is modeled as a chain of {@link Step}
     * <p>
     * This callback removes the stepped operation
     * from {@link #offloadedOperations} registry then
     * calls delegate operation response handler.
     */
    private class SteppedOpResponseHandler implements OperationResponseHandler {

        private OperationResponseHandler delegate;

        SteppedOpResponseHandler(OperationResponseHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void sendResponse(Operation op, Object response) {
            assertRunningOnPartitionThread();

            if (offloadedOperations.remove(op)) {
                ((MapOperation) op).getRecordStore().decMapStoreOffloadedOperationsCount();
                delegate.sendResponse(op, response);
            }
        }
    }
}
