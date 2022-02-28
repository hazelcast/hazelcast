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

import java.util.concurrent.TimeUnit;

/**
 * Statistics and timing info for a {@link IScheduledFuture} accessible through {@link IScheduledFuture#getStats()}
 */
public interface ScheduledTaskStatistics
        extends IdentifiedDataSerializable {

    /**
     * Returns how many times the task was ran/called.
     *
     * @return The numbers of runs
     */
    long getTotalRuns();

    /**
     * Returns the duration of the task's last execution.
     *
     * @param unit The time unit to return the duration in.
     * @return The total duration of the task's last execution.
     */
    long getLastRunDuration(TimeUnit unit);

    /**
     * Returns the last period of time the task was idle, waiting to get scheduled
     *
     * @param unit The time unit to return the duration in.
     * @return The last idle period of time of the task.
     */
    long getLastIdleTime(TimeUnit unit);

    /**
     * Returns the total amount of time the task spent while scheduled in.
     *
     * @param unit The time unit to return the duration in.
     * @return The total amount of time the task spent while scheduled in.
     */
    long getTotalRunTime(TimeUnit unit);

    /**
     * Returns the total amount of time the task was idle, waiting to get scheduled in.
     *
     * @param unit The time unit to return the duration in.
     * @return The total amount of time the task was idle, waiting to get scheduled in.
     */
    long getTotalIdleTime(TimeUnit unit);

}
