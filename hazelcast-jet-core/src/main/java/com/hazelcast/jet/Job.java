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

package com.hazelcast.jet;

import com.hazelcast.jet.impl.util.Util;

import javax.annotation.Nonnull;
import java.util.concurrent.Future;

/**
 * A Jet computation job created from a {@link DAG}. Jet will start executing
 * it automatically and its associated {@link #getFuture() future} can be used
 * to wait for it to complete or cancel it.
 */
public interface Job {

    /**
     * Returns the ID of this job.
     *
     * @throws IllegalStateException If the job was not started yet, and thus
     *                               has no job id.
     */
    long getJobId();

    /**
     * Gets the future associated with the job, used to control the job.
     *
     * @throws IllegalStateException If the job was not started yet.
     */
    @Nonnull
    Future<Void> getFuture();
    /**
     * Returns the status of this job.
     */
    @Nonnull
    JobStatus getJobStatus();

    /**
     * Attempts to cancel execution of this job.
     *
     * Shorthand for <code>job.getFuture().cancel()</code>
     */
    default boolean cancel() {
        return getFuture().cancel(true);
    }

    /**
     * Waits for the job to complete and throws exception if job is completed with an error.
     *
     * Shorthand for <code>job.getFuture().get()</code>
     */
    default void join() {
        Util.uncheckRun(() -> getFuture().get());
    }

    /**
     * Gets the future associated with the job.
     *
     * @return a future that can be inspected for job completion status and cancelled to prematurely end the job.
     *
     * @deprecated Use {@link #getFuture()} instead. This method will be removed in the next release.
     */
    @Deprecated
    default Future<Void> execute() {
        return getFuture();
    }

}
