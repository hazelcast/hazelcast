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

import javax.annotation.Nullable;

/**
 * This delegator step does 2 things:
 * <lu>
 * <li>
 * Delegates calls to append step.
 * </li>
 * <li>
 * Puts append step to the head of current step chain
 * </li>
 * </lu>
 * <p>
 * So with this delegator Step, append becomes 1st step in step
 * chain and current becomes 2nd.
 */
class AppendAsNewHeadStep<S> implements Step<S> {

    private final Step<S> append;
    private final Step<S> current;

    AppendAsNewHeadStep(Step<S> current, Step<S> append) {
        this.append = append;
        this.current = current;
    }

    @Override
    public boolean isOffloadStep(S state) {
        return append.isOffloadStep(state);
    }

    @Override
    public void runStep(S state) {
        append.runStep(state);
    }

    @Override
    public String getExecutorName(S state) {
        return append.getExecutorName(state);
    }

    @Nullable
    @Override
    public Step<S> nextStep(S state) {
        return current;
    }

    // only used for testing
    Step<S> getAppendedStep() {
        return append;
    }


    /**
     * Appends newStep before current head step.
     * <p>
     * After append, new step order:
     * <p>1st step will be newStep,
     * <p>2nd step will be second to execute,
     * <p>3rd step will be 3rd to execute...
     * <p>then other existing steps will be executed in order.
     *
     * @param currentHead current head
     * @param newStep    step to append before current head.
     * @return current head step.
     */
    static Step appendAsNewHeadStep(Step currentHead, Step newStep) {
        return new AppendAsNewHeadStep(currentHead, newStep);
    }
}
