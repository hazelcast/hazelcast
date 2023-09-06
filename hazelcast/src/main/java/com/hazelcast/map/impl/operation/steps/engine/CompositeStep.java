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

import com.hazelcast.map.impl.operation.steps.IMapOpStep;

import javax.annotation.Nullable;
import java.util.List;

/**
 * The composite step for IMap operations. Composes multiple Steps into one unit.
 * For instance, it is used to combine record store and local indexes compactors as one unit.
 * <p>
 * The primary step defines whether the composite step is offloadable, its executor name
 * and next step.
 */
public class CompositeStep implements IMapOpStep {

    private final List<Step> steps;
    private final Step primaryStep;

    public CompositeStep(List<Step> steps, Step primaryStep) {
        assert steps.size() > 1;
        this.steps = steps;
        this.primaryStep = primaryStep;
    }

    @Override
    public void runStep(State state) {
        for (Step step : steps) {
            step.runStep(state);
        }
    }

    @Override
    public boolean isOffloadStep(State state) {
        return primaryStep.isOffloadStep(state);
    }

    @Nullable
    @Override
    public Step nextStep(State state) {
        return primaryStep.nextStep(state);
    }

    @Override
    public String getExecutorName(State state) {
        // All steps should be performed by the same executor
        return primaryStep.getExecutorName(state);
    }
}
