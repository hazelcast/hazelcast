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
 * So with this delegator Step, stepToAppend becomes 1st step in step
 * chain and current step becomes 2nd.
 */
public class AppendAsHeadStep<S> implements Step<S> {

    private final Step appended;
    private final Step current;

    public AppendAsHeadStep(Step appended, Step current) {
        this.appended = appended;
        this.current = current;
    }

    @Override
    public boolean isOffloadStep(S state) {
        return appended.isOffloadStep(state);
    }

    @Override
    public void runStep(S state) {
        appended.runStep(state);
    }

    @Override
    public String getExecutorName(S state) {
        return appended.getExecutorName(state);
    }

    @Nullable
    @Override
    public Step nextStep(S state) {
        return current;
    }

    // only used for testing
    Step getAppendedStep() {
        return appended;
    }
}
