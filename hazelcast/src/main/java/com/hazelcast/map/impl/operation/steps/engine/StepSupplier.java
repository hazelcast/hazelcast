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

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.steps.UtilSteps;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationRunnerImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.function.Supplier;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static com.hazelcast.map.impl.operation.ForcedEviction.runStepWithForcedEvictionStrategies;

/**
 * <lu>
 * <li>This is a single operation's step supplier</li>
 * <li>Supplies steps and decides next step after executed one.</li>
 * <li>Must be thread safe</li>
 * </lu>
 */
public class StepSupplier implements Supplier<Runnable> {

    private final State state;
    private final OperationRunnerImpl operationRunner;

    private volatile Runnable currentRunnable;
    private volatile Step currentStep;

    /**
     * Only here to disable check for testing purposes.
     */
    private final boolean checkCurrentThread;

    public StepSupplier(MapOperation operation) {
        this(operation, true);
    }

    public StepSupplier(MapOperation operation, boolean checkCurrentThread) {
        assert operation != null;

        this.state = operation.createState();
        this.currentStep = operation.getStartingStep();
        this.operationRunner = getPartitionOperationRunner(operation);
        this.checkCurrentThread = checkCurrentThread;

        assert state != null;
        assert currentStep != null;
    }

    private OperationRunnerImpl getPartitionOperationRunner(MapOperation operation) {
        return (OperationRunnerImpl) ((OperationServiceImpl) operation.getNodeEngine()
                .getOperationService()).getOperationExecutor().getPartitionOperationRunners()[state.getPartitionId()];
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
        if (step.isOffloadStep()) {
            return new Runnable() {
                @Override
                public void run() {
                    if (checkCurrentThread) {
                        assert !isRunningOnPartitionThread();
                    }

                    runStepWith(step, state);
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
                runStepWith(step, state);
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
     *     <li>Runs this step</li>
     *     <li>Sets next step to run</li>
     * </lu>
     */
    private void runStepWith(Step step, State state) {
        boolean runningOnPartitionThread = isRunningOnPartitionThread();
        try {
            try {
                log(step, state);

                if (runningOnPartitionThread && state.getThrowable() == null) {
                    operationRunner.metWithPreconditions(state.getOperation());
                }
                step.runStep(state);
            } catch (NativeOutOfMemoryError e) {
                assertRunningOnPartitionThread();

                rerunWithForcedEviction(() -> {
                    step.runStep(state);
                });
            }
        } catch (Throwable throwable) {
            if (runningOnPartitionThread) {
                state.getOperation().disposeDeferredBlocks();
            }
            state.setThrowable(throwable);
        } finally {
            currentStep = nextStep(step);
            currentRunnable = createRunnable(currentStep, state);
        }
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

    private static void log(Step currentStep, State state) {
        MapOperation operation = state.getOperation();
        ILogger logger = operation.getNodeEngine().getLogger(operation.getClass());
        if (logger.isFinestEnabled()) {
            logger.finest(currentStep.toString() + " ==> " + operation.hashCode());
        }
    }
}
