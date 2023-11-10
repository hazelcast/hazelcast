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
import com.hazelcast.map.impl.operation.steps.UtilSteps;
import com.hazelcast.map.impl.recordstore.StepAwareStorage;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationRunnerImpl;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static com.hazelcast.map.impl.operation.ForcedEviction.runStepWithForcedEvictionStrategies;
import static com.hazelcast.map.impl.operation.steps.engine.AppendAsNewHeadStep.appendAsNewHeadStep;

/**
 * <lu>
 * <li>This is a single operation's step supplier</li>
 * <li>Supplies steps and decides next step after executed one.</li>
 * <li>Must be thread safe</li>
 * </lu>
 */
public class StepSupplier implements Supplier<Runnable>, Consumer<Step> {

    private final State state;
    private final OperationRunnerImpl operationRunner;

    private volatile Runnable currentRunnable;
    private volatile Step currentStep;
    private volatile boolean firstStep = true;

    /**
     * Only here to disable check for testing purposes.
     */
    private final boolean checkCurrentThread;

    public StepSupplier(MapOperation operation) {
        this(operation, true);
    }

    // package-private for testing purposes
    StepSupplier(MapOperation operation,
                 boolean checkCurrentThread) {
        assert operation != null;

        this.state = operation.createState();
        this.currentStep = operation.getStartingStep();
        collectAndUpdateHeadSteps(operation);
        this.operationRunner = UtilSteps.getPartitionOperationRunner(state);
        this.checkCurrentThread = checkCurrentThread;

        assert this.currentStep != null;
    }

    @Override
    public void accept(Step headStep) {
        if (headStep == null) {
            return;
        }

        this.currentStep = appendAsNewHeadStep(currentStep, headStep);
    }

    private void collectAndUpdateHeadSteps(MapOperation operation) {
        Storage storage = operation.getRecordStore().getStorage();
        if (storage instanceof StepAwareStorage) {
            ((StepAwareStorage) storage).addAsHeadStep(this);
        }
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
                if (checkCurrentThread) {
                    assert isRunningOnPartitionThread();
                }
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
     * Responsibilities of this method:
     * <lu>
     * <li>Runs passed step with passed state</li>
     * <li>Sets next step to run</li>
     * </lu>
     */
    private void runStepWithState(Step step, State state) {
        boolean runningOnPartitionThread = isRunningOnPartitionThread();
        boolean metWithPreconditions = true;
        try {
            state.getRecordStore().beforeOperation();
            try {
                if (runningOnPartitionThread && state.getThrowable() == null) {
                    metWithPreconditions = metWithPreconditions();
                }

                if (metWithPreconditions) {
                    step.runStep(state);
                }
            } catch (NativeOutOfMemoryError e) {
                if (runningOnPartitionThread) {
                    rerunWithForcedEviction(() -> {
                        step.runStep(state);
                    });
                } else {
                    throw e;
                }
            } finally {
                state.getRecordStore().afterOperation();
            }
        } catch (Throwable throwable) {
            if (runningOnPartitionThread) {
                state.getOperation().disposeDeferredBlocks();
            }
            state.setThrowable(throwable);
        } finally {
            if (metWithPreconditions) {
                currentStep = nextStep(step);
                currentRunnable = createRunnable(currentStep, state);
            } else {
                currentStep = null;
                currentRunnable = null;
            }
        }
    }

    private boolean metWithPreconditions() {
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
     * In case of exception, sets next step as {@link UtilSteps#HANDLE_ERROR},
     * otherwise finds next step by calling {@link Step#nextStep}
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
