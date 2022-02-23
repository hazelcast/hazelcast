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

package com.hazelcast.spi.impl.operationservice;

/**
 * An factory for creating partition specific tasks.
 *
 * A task can be:
 * <ol>
 * <li>Operation</li>
 * <li>Runnable</li>
 * </ol>
 *
 * See {@link OperationService#executeOnPartitions} for more details.
 */
public interface PartitionTaskFactory<T> {

    /**
     * Creates the task.
     *
     * @param partitionId the partitionId of the
     *                    partition this task is going to run on
     * @return the created task. The returned task should not be null.
     */
    T create(int partitionId);
}
