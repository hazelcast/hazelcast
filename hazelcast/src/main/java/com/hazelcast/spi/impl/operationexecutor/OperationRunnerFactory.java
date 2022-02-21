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

package com.hazelcast.spi.impl.operationexecutor;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

/**
 * A Factory for creating {@link OperationRunner} instances.
 */
public interface OperationRunnerFactory {

    /**
     * Creates an OperationRunner for a given partition. This OperationRunner
     * will only execute generic operations and operations for that given
     * partition.
     *
     * @param partitionId the ID of the partition.
     * @return the created OperationRunner.
     */
    OperationRunner createPartitionRunner(int partitionId);

    /**
     * Creates an OperationRunner to execute generic Operations.
     *
     * @return the created OperationRunner
     */
    OperationRunner createGenericRunner();

    /**
     * Creates an ad hoc generic OperationRunner.
     *
     * Why do we need ad-hoc operation runners; why not use the generic ones?
     * The problem is that within Operations can be executed directly on the
     * calling thread and totally bypassing the generic threads. The problem
     * is that or these kinds of 'ad hoc' executions, you need to have an
     * OperationRunner.
     *
     * The ad hoc OperationRunner can be used for these ad hoc executions. It
     * is immutable and won't expose the {@link OperationRunner#currentTask()}.
     * Therefor it is save to be shared between threads without running into
     * problems.
     *
     * @return the created ad hoc OperationRunner.
     * @see OperationService#run(Operation)
     */
    OperationRunner createAdHocRunner();
}
