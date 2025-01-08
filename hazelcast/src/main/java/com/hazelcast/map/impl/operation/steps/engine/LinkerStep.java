/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
 * This linker step links first step with second step, this will
 * mean, after execution of 1st step, 2nd step will be executed.
 */
public class LinkerStep<S> implements Step<S> {

    private final Step<S> first;
    private final Step<S> second;

    LinkerStep(Step<S> first, Step<S> second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean isOffloadStep(S state) {
        return first.isOffloadStep(state);
    }

    @Override
    public void runStep(S state) {
        first.runStep(state);
    }

    @Override
    public String getExecutorName(S state) {
        return first.getExecutorName(state);
    }

    @Nullable
    @Override
    public Step<S> nextStep(S state) {
        return second;
    }

    // only used for testing
    Step<S> getFirstStep() {
        return first;
    }


    /**
     * Appends first before second step.
     * <p>
     * After append, new step execution order:
     * <p>first step will be executed first,
     * <p>second step will be executed secondly
     * <p>then next step of second will be executed..
     *
     * @param first  step to append before second head.
     * @param second second step
     * @return first step.
     */
    public static Step linkSteps(Step first, Step second) {
        return new LinkerStep(first, second);
    }
}
