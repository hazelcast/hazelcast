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

package com.hazelcast.map.impl.operation.steps;

import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.operation.steps.engine.StepAwareOperation;
import com.hazelcast.spi.impl.operationservice.BackupOperation;


/**
 * {@link StepAwareOperation} specialized for {@link
 * com.hazelcast.map.IMap} operations.
 */
public interface IMapStepAwareOperation extends StepAwareOperation<State> {

    @Override
    default Step getStartingStep() {
        assert this instanceof MapOperation;

        // Here only backup-operations of some MapOperations which has
        // tieredStoreAndPartitionCompactorEnabled field is set true
        // are created as a Step automatically, otherwise you have
        // to make your MapOperation as a Step operation yourself.
        if (((MapOperation) this).supportsSteppedRun()
                && this instanceof BackupOperation) {
            return UtilSteps.DIRECT_RUN_STEP;
        }

        return UtilSteps.DIRECT_RUN_STEP;
    }
}
