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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl;

import java.util.UUID;

/**
 * Resource handler pointing to a {@link IScheduledFuture}. The handler is used to interact with the <code>ScheduledFuture</code>
 * in a {@link IScheduledExecutorService}.
 *
 * <p>To access the handler, see {@link IScheduledFuture#getHandler()}. To re-acquire access to a previously scheduled task,
 * having only the handler at hand, see {@link IScheduledExecutorService#getScheduledFuture(ScheduledTaskHandler)}
 */
public abstract class ScheduledTaskHandler
        implements IdentifiedDataSerializable {

    /**
     * Returns the uuid of the member the task is associated with.
     *
     * <p>The uuid will be {@code null}, if the task was scheduled to particular partition.
     *
     * @return The uuid of the member
     */
    public abstract UUID getUuid();

    /**
     * Returns the partition ID the task is associated with.
     *
     * <p>If the task was scheduled to a particular member, then the partition ID will have the value of -1.
     *
     * @return The partition ID
     */
    public abstract int getPartitionId();

    /**
     * Return the name of the ScheduledExecutor this tasks is running on.
     *
     * @return the name of the scheduler
     */
    public abstract String getSchedulerName();

    /**
     * Returns the name of the task.
     *
     * @return the task name
     */
    public abstract String getTaskName();

    /**
     * @return {@code true} when the associated task was scheduled on a specific partition
     */
    public abstract boolean isAssignedToPartition();

    /**
     * @return {@code true} when the associated task was scheduled on a specific member in the cluster
     */
    public abstract boolean isAssignedToMember();

    /**
     * Returns the String representation of the handler.
     *
     * <p>Useful for persisting and/or communicating this info. A {@link ScheduledTaskHandler} can be constructed again
     * from the Urn String using {@link #of(String)}
     *
     * @return URN representing the handler in a String format
     */
    public abstract String toUrn();

    /**
     * Reconstructs a {@link ScheduledTaskHandler} from a URN String.
     *
     * @param urn The URN of the handler as retrieved from {@link #toUrn()}
     * @return A {@link ScheduledTaskHandler} instance that can be used to access the scheduled task
     */
    public static ScheduledTaskHandler of(String urn) {
        return ScheduledTaskHandlerImpl.of(urn);
    }
}
