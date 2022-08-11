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

package com.hazelcast.map.impl.operation.steps.engine;

import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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

    private static final long DEFAULT_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS
            = TimeUnit.MILLISECONDS.toNanos(5);
    private static final String PROP_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS
            = "hazelcast.internal.map.mapstore.max.successive.offloaded.operation.run.nanos";
    private static final HazelcastProperty MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS
            = new HazelcastProperty(PROP_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS,
            DEFAULT_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS, NANOSECONDS);

    private final int partitionId;
    private final long maxRunNanos;
    private final Set<MapOperation> offloadedOperations;
    private final OperationExecutor operationExecutor;
    private final ManagedExecutorService executor;

    private volatile StepSupplier stepSupplier;

    public StepRunner(MapOperation mapOperation) {
        super(mapOperation);
        this.offloadedOperations = getOffloadedOperations(mapOperation);
        this.partitionId = mapOperation.getPartitionId();
        NodeEngine nodeEngine = mapOperation.getNodeEngine();
        this.operationExecutor = ((OperationServiceImpl) nodeEngine.getOperationService()).getOperationExecutor();
        this.executor = nodeEngine.getExecutionService().getExecutor(MAP_STORE_OFFLOADABLE_EXECUTOR);
        this.maxRunNanos = nodeEngine.getProperties().getNanos(MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS);
    }

    @Override
    public void start() throws Exception {
        Operation op = offloadedOperation();
        addOpToOffloadedOps(((MapOperation) op));

        if (isCurrentOffloadedOpCountOne()) {
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

    private void addOpToOffloadedOps(MapOperation op) {
        op.setOperationResponseHandler(new OffloadedStepResponseHandler(op.getOperationResponseHandler()));
        offloadedOperations.add(op);
        op.getRecordStore().incMapStoreOffloadedOperationsCount();
    }

    private boolean isCurrentOffloadedOpCountOne() {
        return offloadedOperations.size() == 1;
    }

    @Override
    public void run() {
        boolean runningOnPartitionThread = isRunningOnPartitionThread();
        run0(runningOnPartitionThread);
    }

    /**
     * Runs all queued operations one by one.
     * <p>
     * For fair usage of partition thread, it
     * has a {@link #maxRunNanos} upper limit.
     */
    @SuppressWarnings("checkstyle:innerassignment")
    private void run0(boolean runningOnPartitionThread) {
        long start = System.nanoTime();
        Runnable step;
        do {
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

            // Try to run this step in this thread, otherwise
            // offload the step to relevant executor(it
            // is operation or general-purpose executor)
            if (!runDirect(step)) {
                offloadRun(step, this);
                return;
            }

            // if there are many offloadedOperations belonging for
            // a specific map, this step-runner tries to run all in
            // one go. Let's assume all steps are partition-runnable,
            // and can run on partition thread, in this case, to
            // prevent unfair usage of partition thread, we put
            // an upper time-limit with maxRunNanos, so a map
            // cannot make a partition thread busy indefinitely
            // and other operations can have a chance to run.
            if (maxRunNanos > 0 && runningOnPartitionThread
                    && System.nanoTime() - start >= maxRunNanos) {
                step = stepSupplier.get();
                if (step != null) {
                    offloadRun(step, this);
                    return;
                }
            }
        } while (true);
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
            if (!isRunningOnPartitionThread()) {
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
            executor.execute(offload);
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
     * After response is sent for an offloaded operation,
     * this callback removes offloaded operation from
     * the {@link #offloadedOperations} registry.
     */
    private class OffloadedStepResponseHandler implements OperationResponseHandler {

        private OperationResponseHandler delegate;

        OffloadedStepResponseHandler(OperationResponseHandler delegate) {
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
