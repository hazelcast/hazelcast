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
 * Represents an isolated step of an operation. e.g.
 * {@link  com.hazelcast.map.impl.operation.PutOperation}
 * <p>
 * By using this interface, an operation can be modeled as
 * a collection of {@link Step}. With this model, offloadable steps
 * can be run in other threads than partitioned ones.
 *
 * @param <S> state object passed from one step to another
 */
public interface Step<S> {

    /**
     * Code to run at this step by using the provided state
     */
    void runStep(S state);

    /**
     * Idempotent next step finder.
     *
     * @return next step or null if there is no next step.
     * @implSpec This implementation always throws
     * {@code UnsupportedOperationException}.
     */
    @Nullable
    default Step nextStep(S state) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return {@code true} if this step must be
     * offloaded to a thread other than {@link
     * com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread}
     * otherwise {@code false}
     */
    default boolean isOffloadStep(S state) {
        return false;
    }

    /**
     * Used when this step is an
     * offload-step, otherwise not used.
     *
     * @param state the state object
     * @return name of the executor to run
     * this step(valid if this step is an offload-step)
     * @implSpec This implementation always throws
     * {@code UnsupportedOperationException}.
     */
    default String getExecutorName(S state) {
        throw new UnsupportedOperationException();
    }
}
