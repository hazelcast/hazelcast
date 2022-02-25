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

package com.hazelcast.jet.core;

/**
 * Represents current status of the job from the perspective of the job coordinator.
 *
 * @since Jet 3.0
 */
public enum JobStatus {

    /**
     * The job is submitted but hasn't started yet. A job also enters this
     * state when its execution was interrupted (e.g., due to a cluster member
     * failing), before it is started again.
     */
    NOT_RUNNING,

    /**
     * The job is in the initialization phase on a new coordinator.
     */
    STARTING,

    /**
     * The job is currently running.
     */
    RUNNING,

    /**
     * The job is suspended and it can be manually resumed.
     */
    SUSPENDED,

    /**
     * The job is suspended and is exporting the snapshot. It cannot be resumed
     * until the export is finished and status is {@link #SUSPENDED} again.
     */
    SUSPENDED_EXPORTING_SNAPSHOT,

    /**
     * The job is currently being completed.
     */
    COMPLETING,

    /**
     * The job has failed with an exception.
     */
    FAILED,

    /**
     * The job has completed successfully.
     */
    COMPLETED;

    /**
     * Returns {@code true} if this state is terminal - a job in this state
     * will never have any other state and will never execute again. It's
     * {@link #COMPLETED} or {@link #FAILED}.
     */
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED;
    }
}
