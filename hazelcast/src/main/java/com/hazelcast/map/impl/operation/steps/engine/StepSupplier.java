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
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.steps.UtilSteps;
import com.hazelcast.map.impl.recordstore.CustomStepAwareStorage;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationRunnerImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static com.hazelcast.map.impl.operation.ForcedEviction.runStepWithForcedEvictionStrategies;
import static com.hazelcast.map.impl.operation.steps.engine.LinkerStep.linkSteps;

/**
 * <ul>
 * <li>Supplies steps for a single operation.</li>
 * <li>Determines the next step after the executed one.</li>
 * <li>Must be thread-safe.</li>
 * </ul>
 */
public class StepSupplier implements Supplier<Runnable>, Consumer<Step> {

    private final State state;
    private final OperationRunnerImpl operationRunner;
    /**
     * Only here to disable the thread check for testing purposes.
     */
    private final boolean checkCurrentThread;

    private volatile Runnable currentRunnable;
    private volatile Step currentStep;
    private volatile boolean firstStep = true;

    public StepSupplier(MapOperation operation) {
        this(operation, true);
    }

    // package-private for testing purposes
    StepSupplier(MapOperation operation,
                 boolean checkCurrentThread) {
        assert operation != null;

        this.state = operation.createState();
        this.currentStep = operation.getStartingStep();
        this.operationRunner = UtilSteps.getPartitionOperationRunner(state);
        this.checkCurrentThread = checkCurrentThread;

        collectCustomSteps(operation, this);

        assert this.currentStep != null;
    }

    @Override
    public void accept(Step headStep) {
        if (headStep != null) {
            this.currentStep = linkSteps(headStep, currentStep);
        }
    }

    public static void collectCustomSteps(MapOperation operation, Consumer<Step> consumer) {
        RecordStore recordStore = operation.getRecordStore();
        if (recordStore == null) {
            return;
        }

        Storage storage = recordStore.getStorage();
        if (storage instanceof CustomStepAwareStorage awareStorage) {
            awareStorage.collectCustomSteps(consumer);
        }
    }

    public static Step injectCustomStepsToOperation(MapOperation operation,
                                                    Step targetStep) {
        List<Step> customSteps = new ArrayList<>();
        collectCustomSteps(operation, step -> {
            if (step != null) {
                customSteps.add(step);
            }
        });

        Step result = targetStep;
        for (int i = 0; i < customSteps.size(); i++) {
            result = linkSteps(customSteps.get(i), result);
        }
        return result;
    }

    // used only for testing
    Step getCurrentStep() {
        return currentStep;
    }

    @Override
    public Runnable get() {
        if (currentRunnable == null && currentStep != null) {
            currentRunnable = createRunnable(currentStep, state);
        }
        return currentRunnable;
    }

    private Runnable createRunnable(Step step, State state) {
        // 0. If null step return null
        if (step == null) {
            return null;
        }

        // 1. If step needs to be offloaded,
        // return step wrapped as a runnable.
        if (step.isOffloadStep(state)) {
            return new ExecutorNameAwareRunnable() {
                @Override
                public String getExecutorName() {
                    return step.getExecutorName(state);
                }

                @Override
                public void run() {
                    assert !checkCurrentThread || !isRunningOnPartitionThread();
                    runStepWithState(step, state);
                }

                @Override
                public String toString() {
                    return step.toString();
                }
            };
        }

        // 2. If step needs to be run on partition thread,
        // return step wrapped as a partition specific runnable.
        return new PartitionSpecificRunnable() {
            @Override
            public void run() {
                assert !checkCurrentThread || isRunningOnPartitionThread();
                runStepWithState(step, state);
            }

            @Override
            public int getPartitionId() {
                return state.getPartitionId();
            }

            @Override
            public String toString() {
                return step.toString();
            }
        };
    }

    /**
     * Runs the supplied step with the supplied state and schedules the next step.
     */
    private void runStepWithState(Step step, State state) {
        boolean runningOnPartitionThread = isRunningOnPartitionThread();
        boolean shouldAdvance = true;

        try {
            refreshState(state);
            shouldAdvance = executeStep(step, state, runningOnPartitionThread);
        } catch (Throwable throwable) {
            handleStepFailure(state, runningOnPartitionThread, throwable);
        } finally {
            advanceStep(step, state, shouldAdvance);
        }
    }

    private boolean executeStep(Step step, State state, boolean runningOnPartitionThread) {
        final boolean errorStep = step == UtilSteps.HANDLE_ERROR;
        final RecordStore recordStore = state.getRecordStore();
        final int threadIndex = beforeOperation(recordStore, errorStep);
        try {
            if (runningOnPartitionThread
                    && state.getThrowable() == null
                    && !checkPreconditions()) {
                return false;
            }

            runStep(step, state, runningOnPartitionThread);
            return true;
        } finally {
            afterOperation(recordStore, errorStep, threadIndex);
        }
    }

    private int beforeOperation(RecordStore recordStore, boolean errorStep) {
        if (errorStep || recordStore == null) {
            return -1;
        }
        return recordStore.beforeOperation();
    }

    private void afterOperation(RecordStore recordStore, boolean errorStep, int threadIndex) {
        if (!errorStep && recordStore != null) {
            recordStore.afterOperation(threadIndex);
        }
    }

    private void runStep(Step step, State state, boolean runningOnPartitionThread) {
        try {
            step.runStep(state);
        } catch (NativeOutOfMemoryError e) {
            if (runningOnPartitionThread) {
                rerunWithForcedEviction(() -> step.runStep(state));
                return;
            }
            throw e;
        }
    }

    private void handleStepFailure(State state, boolean runningOnPartitionThread, Throwable throwable) {
        if (runningOnPartitionThread) {
            state.getOperation().disposeDeferredBlocks();
        }
        state.setThrowable(throwable);
    }

    private void advanceStep(Step step, State state, boolean shouldAdvance) {
        if (shouldAdvance) {
            currentStep = nextStep(step);
            currentRunnable = createRunnable(currentStep, state);
        } else {
            currentStep = null;
            currentRunnable = null;
        }
    }

    /**
     * Refreshes this {@code StepSupplier} {@link State} by resetting its
     * record-store and operation objects.
     * <p>
     * This is needed because while an offloaded operation is waiting in a queue,
     * a previously queued {@code map#destroy} operation can remove the current
     * IMap state. In that case, later queued operations may hold stale state.
     */
    private void refreshState(State state) {
        MapOperation operation = state.getOperation();
        if (!operation.checkMapExists()) {
            return;
        }

        state.init(operation.getRecordStore(), operation);
    }

    private boolean checkPreconditions() {
        assert isRunningOnPartitionThread();

        // check node and cluster health before running each step
        operationRunner.ensureNodeAndClusterHealth(state.getOperation());

        // check timeout for only first step,
        // as in regular operation-runner
        if (firstStep) {
            assert firstStep;
            firstStep = false;
            return !operationRunner.timeout(state.getOperation());
        }
        return true;
    }

    /**
     * In case of exception, sets the next step as {@link UtilSteps#HANDLE_ERROR};
     * otherwise finds the next step by calling {@link Step#nextStep}.
     */
    private Step nextStep(Step step) {
        if (state.getThrowable() != null
                && currentStep != UtilSteps.HANDLE_ERROR) {
            return UtilSteps.HANDLE_ERROR;
        }
        return step.nextStep(state);
    }

    private void rerunWithForcedEviction(Runnable step) {
        runStepWithForcedEvictionStrategies(state.getOperation(), step);
    }

    public void handleOperationError(Throwable throwable) {
        state.setThrowable(throwable);
        currentRunnable = null;
        currentStep = UtilSteps.HANDLE_ERROR;
    }

    public MapOperation getOperation() {
        return state.getOperation();
    }

    private interface ExecutorNameAwareRunnable extends Runnable, Offloadable {
    }
}
