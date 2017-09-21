/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 */
public enum JobStatus {

    /**
     * The job is submitted but not started yet.
     */
    NOT_STARTED,

    /**
     * The job is in the initialization phase on new coordinator, in which it
     * starts the execution.
     */
    STARTING,

    /**
     * The job is currently running.
     */
    RUNNING,

    /**
     * The job is performing a restart by the same coordinator, because a job
     * participant has left while the job was running.
     */
    RESTARTING,

    /**
     * The job is failed with an exception.
     */
    FAILED,

    /**
     * The job is completed successfully.
     */
    COMPLETED

}
