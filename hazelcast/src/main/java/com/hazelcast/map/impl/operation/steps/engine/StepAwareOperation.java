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

import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;

import java.util.function.Consumer;

/**
 * Contract to create a chain of steps from an operation
 *
 * @see Step
 */
public interface StepAwareOperation<S> {

    /**
     * @return create initial state of this operation
     */
    S createState();

    /**
     * @param state state to be applied to
     * operation after execution of all step chain
     */
    default void applyState(S state) {
        // No need to implement, if there
        // is no state to apply after run.
    }

    /**
     * @return starting {@link Step} for this operation
     */
    default Step getStartingStep() {
        return null;
    }

    /**
     * This method is used to inject {@link Backup#afterRun()} to {@link Step} engine.
     * <p>
     * Its goal is to call it on completion of offloaded backup operation.
     *
     * @param backupOpAfterRun {@link Backup#afterRun()}
     */
    default void setBackupOpAfterRun(Consumer backupOpAfterRun) {

    }
}
