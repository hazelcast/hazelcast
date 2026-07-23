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

package com.hazelcast.map.impl.operation.steps.engine;

import com.hazelcast.core.Offloadable;
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.recordstore.OffloadedOperations;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.Offload;

import javax.annotation.Nullable;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static java.lang.Thread.currentThread;

/**
 * Drives the execution of step-based map operations.
 * <p>
 * All step-based operations of a record store are queued in that
 * record store's {@link OffloadedOperations}. At most one
 * <em>step-chain</em> is active per record store at any time; a chain
 * executes queued operations one after another, hopping between the
 * partition thread and offloaded executors as steps require.
 * <p>
 * A {@code StepRunner} is created per operation submission, but only
 * the submission that acquires chain-ownership (see
 * {@link OffloadedOperations#tryAcquire()}) actually drives execution;
 * all other submissions merely enqueue their operation. Chain-ownership
 * is taken in {@link #start} and released only when the queue is
 * drained &mdash; never while an operation is parked in an offloaded
 * step &mdash; so operations can never interleave on the same record
 * store.
 * <p>
 * Must be thread-safe: {@link #run} is invoked from partition threads
 * and offloaded executor threads, though never concurrently for the
 * same chain.
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
    private final OffloadedOperations offloadedOperations;
    private final OperationExecutor operationExecutor;
    private final @Nullable String namespace;

    /**
     * Supplies the steps of the operation this chain is currently
     * executing. {@code null} before the first operation is polled;
     * replaced whenever the chain advances to the next queued
     * operation.
     */
    private volatile StepSupplier stepSupplier;

    // Acts as a local variable.
    private String currentExecutorName;

    public StepRunner(MapOperation mapOperation) {
        super(mapOperation);
        this.offloadedOperations = mapOperation.getRecordStore().getOffloadedOperations();
        this.partitionId = mapOperation.getPartitionId();
        NodeEngine nodeEngine = mapOperation.getNodeEngine();
        this.operationExecutor = nodeEngine
                .getOperationService().getOperationExecutor();
        MapContainer mapContainer = mapOperation.getMapContainer();
        this.maxRunNanos = mapContainer
                .getMapServiceContext().getMaxSuccessiveOffloadedOpRunNanos();
        this.namespace = mapContainer.getMapConfig().getUserCodeNamespace();
    }

    @Override
    public void start() throws Exception {
        MapOperation op = ((MapOperation) offloadedOperation());
        if (!offloadedOperations.offer(op)) {
            // This exact operation instance is already queued or
            // running, e.g. it was re-submitted by invocation retry
            // while parked in an offloaded step. Starting a second
            // step-chain for it would execute the operation twice,
            // concurrently, over the same record-store state. The
            // in-flight incarnation will send the response.
            logIgnoredDuplicateSubmission(op);
            return;
        }

        if (offloadedOperations.tryAcquire()) {
            run();
        }
    }

    private void logIgnoredDuplicateSubmission(MapOperation op) {
        ILogger logger = nodeEngine.getLogger(StepRunner.class);
        if (logger.isFineEnabled()) {
            logger.fine("Ignoring duplicate submission of an already queued or running operation"
                            + " [partitionId=%d, queuedOrRunningOpCount=%d, operation=%s]",
                    partitionId, offloadedOperations.size(), op);
        }
    }

    /**
     * @return true if current partition thread is
     * executing this step runner now, false otherwise.
     */
    public static boolean isStepRunnerCurrentlyExecutingOnPartitionThread() {
        return CURRENTLY_EXECUTING_ON_PARTITION_THREAD.get();
    }

    @Override
    public void run() {
        try {
            runQueuedOperations();
        } catch (Throwable t) {
            // The step engine itself failed; step errors are handled
            // inside the loop, so this is unexpected. Do not leave
            // chain-ownership dangling, otherwise all queued and
            // future operations of this record store would stall.
            cleanUpAfterUnexpectedFailure();
            throw rethrow(t);
        }
    }

    /**
     * Executes the steps of the queued operations, one operation at a
     * time, until one of the following happens on the calling thread:
     * <ul>
     *     <li>the queue is drained &mdash; chain-ownership is
     *     released,</li>
     *     <li>the next step requires another executor &mdash; this
     *     runner is re-submitted there and chain-ownership is
     *     kept,</li>
     *     <li>this thread is a partition thread and ran longer than
     *     {@code maxRunNanos} &mdash; see
     *     {@link #postponeRemainingSteps()}, chain-ownership is
     *     kept, or</li>
     *     <li>the thread is interrupted &mdash; shutdown, ownership
     *     is released.</li>
     * </ul>
     */
    private void runQueuedOperations() {
        final boolean onPartitionThread = isRunningOnPartitionThread();
        final long startTime = System.nanoTime();

        do {
            try {
                Runnable step = stepSupplier == null ? null : stepSupplier.get();

                if (step == null) {
                    // Current operation - if any - ran its last step.
                    if (!advanceToNextOperation(onPartitionThread)) {
                        return;
                    }
                    continue;
                }

                if (!tryRunInCallingThread(step)) {
                    // Step requires a different executor.
                    // Step chain-ownership is intentionally
                    // kept while this runner travels there.
                    submitToRequiredExecutor(step);
                    return;
                }

                if (exceededMaxRunTime(onPartitionThread, startTime)) {
                    postponeRemainingSteps();
                    return;
                }
            } catch (Throwable throwable) {
                stepSupplier.handleOperationError(throwable);
            }
        } while (!currentThread().isInterrupted());

        // Interrupt-driven exit (executor is shutting down): this
        // step chain cannot make progress anymore. Release ownership
        // so the record store's queue is not stalled forever.
        offloadedOperations.release();
    }

    /**
     * Completes the current operation (if any) and polls the next
     * queued one. These decisions are only made on the partition
     * thread; when called from another thread, this runner re-submits
     * itself to the partition thread instead.
     *
     * @return {@code true} if a next operation was polled and its
     * steps should now be executed, {@code false} if there is nothing
     * more to do on this thread: either the queue was drained and
     * chain-ownership released, or the decision was handed over to
     * the partition thread
     */
    private boolean advanceToNextOperation(boolean onPartitionThread) {
        if (!onPartitionThread) {
            // Non-partitioned thread cannot make next op decision,
            // send this runner to partition thread
            operationExecutor.execute(this);
            return false;
        }

        // Partitioned thread can make next op decision.
        if (stepSupplier != null) {
            // The operation's response has been sent by its last
            // step; unregister it so a future re-submission of the
            // same instance can be queued again.
            offloadedOperations.complete(stepSupplier.getOperation());
        }

        stepSupplier = getNextStepSupplierOrNull();
        if (stepSupplier == null) {
            // No op left in the queue, all are drained:
            // releasing operation-chain ownership, here and only
            // here. Both offer() and this drain run on the
            // partition thread, so no operation can be offered
            // between the failed peek and this release.
            offloadedOperations.release();
            return false;
        }
        return true;
    }

    /**
     * @return null if no offloaded operations to execute, otherwise
     * create next step supplier for the next offloaded operation.
     * The operation stays registered in {@link OffloadedOperations}
     * while it runs; it is unregistered by {@code complete()} once
     * its last step has been executed.
     */
    @Nullable
    private StepSupplier getNextStepSupplierOrNull() {
        assert isRunningOnPartitionThread();

        MapOperation operation = offloadedOperations.peekNext();
        if (operation == null) {
            return null;
        }

        return new StepSupplier(operation);
    }

    /**
     * Runs the given step in the calling thread, if the calling
     * thread is of the kind the step requires.
     *
     * @return {@code true} if the step was run, {@code false} if it
     * has to be submitted to a different executor
     */
    private boolean tryRunInCallingThread(Runnable step) {
        boolean onPartitionThread = isRunningOnPartitionThread();

        if (step instanceof PartitionSpecificRunnable) {
            if (!onPartitionThread) {
                return false;
            }

            try {
                CURRENTLY_EXECUTING_ON_PARTITION_THREAD.set(true);
                NamespaceUtil.runWithNamespace(nodeEngine, namespace, step);
            } finally {
                CURRENTLY_EXECUTING_ON_PARTITION_THREAD.set(false);
            }
            return true;
        }

        // An offloaded step: run it only if we are already
        // on a thread of the executor the step asks for.
        if (onPartitionThread) {
            return false;
        }
        // currentExecutorName can be null, if first step is offload step.
        if (currentExecutorName != null
                && !((Offloadable) step).getExecutorName().equals(currentExecutorName)) {
            return false;
        }

        NamespaceUtil.runWithNamespace(nodeEngine, namespace, step);
        return true;
    }

    /**
     * Submits this runner to the executor required by the given step:
     * the partition operation executor for partition-specific steps,
     * or the step's offload executor otherwise.
     */
    private void submitToRequiredExecutor(Runnable step) {
        if (step instanceof PartitionSpecificRunnable) {
            operationExecutor.execute(this);
        } else {
            currentExecutorName = ((Offloadable) step).getExecutorName();
            executionService.getExecutor(currentExecutorName)
                    .execute(this);
        }
    }

    /**
     * Independent of the number of queued offloadedOperations, this
     * step-runner tries to run all queued operations in one go. This
     * may cause biased usage of the partition thread in favor of the
     * operating map. To prevent this, one can put a max execution
     * time-limit with the `maxRunNanos` setting, so partition
     * operations of other maps don't wait longer. But if there are
     * only a few maps, this setting can cause increased latencies as
     * a side effect. Default value of `maxRunNanos` is zero.
     *
     * @return {@code true} if this call of {@link
     * #runQueuedOperations()} has kept the partition thread busy
     * longer than {@code maxRunNanos}
     */
    private boolean exceededMaxRunTime(boolean onPartitionThread, long startTime) {
        return onPartitionThread && maxRunNanos > 0
                && System.nanoTime() - startTime >= maxRunNanos;
    }

    /**
     * Stops running steps now and re-submits this runner to the back
     * of the required executor's queue, so other work waiting for the
     * partition thread gets its turn first. Chain-ownership is kept:
     * this gives way to other maps' and partitions' work, never to
     * other step-operations of this record store.
     *
     * If the current operation has no next step, this runner is
     * re-submitted to the partition executor so completing it and
     * advancing to the next queued operation happens in a new
     * partition-thread turn.
     */
    private void postponeRemainingSteps() {
        Runnable nextStep = stepSupplier.get();
        if (nextStep == null) {
            operationExecutor.execute(this);
            return;
        }

        submitToRequiredExecutor(nextStep);
    }

    /**
     * Safety valve for unexpected step-engine failures: unregisters
     * the current operation (when possible) and releases
     * chain-ownership so this record store's queue does not stall
     * forever.
     */
    private void cleanUpAfterUnexpectedFailure() {
        if (isRunningOnPartitionThread() && stepSupplier != null) {
            offloadedOperations.complete(stepSupplier.getOperation());
        }
        offloadedOperations.release();
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }
}
