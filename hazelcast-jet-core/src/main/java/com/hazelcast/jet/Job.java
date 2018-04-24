/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * A Jet computation job created from a {@link DAG} or {@link Pipeline}.
 * Once submitted, Jet starts executing the job automatically.
 */
public interface Job {

    /**
     * Returns the ID of this job.
     *
     * @throws IllegalStateException if the job has not started yet, and thus has no id.
     */
    long getId();

    /**
     * Returns the configuration this job was submitted with. Changes made to the
     * returned config object will not have any affect.
     */
    @Nonnull
    JobConfig getConfig();

    /**
     * Returns the name of this job or {@code null} if no name was supplied.
     * <p>
     * Jobs can be named through {@link JobConfig#setName(String)} prior to submission.
     */
    @Nullable
    default String getName() {
        return getConfig().getName();
    }

    /**
     * Returns the time when the job was submitted to the cluster.
     */
    long getSubmissionTime();

    /**
     * Returns the current status of this job.
     */
    @Nonnull
    JobStatus getStatus();

    /**
     * Gets the future associated with the job. The returned future is
     * not cancellable. To cancel the job, the {@link #cancel()} method
     * should be used.
     *
     * @throws IllegalStateException if the job has not started yet.
     */
    @Nonnull
    CompletableFuture<Void> getFuture();

    /**
     * Attempts to cancel execution of this job. The job will be completed
     * after the job has been stopped on all the nodes.
     *
     * Starting from version 0.6, <code>job.getFuture().cancel()</code> fails
     * with an exception.
     */
    boolean cancel();

    /**
     * Waits for the job to complete and throws exception if job is completed
     * with an error.
     *
     * Shorthand for <code>job.getFuture().get()</code>
     */
    default void join() {
        Util.uncheckRun(() -> getFuture().get());
    }

    /**
     * Cancels the current execution if the job is currently running and
     * schedules a new execution with the current member list of the Jet cluster
     *
     * @throws IllegalStateException if the job has been already completed
     *
     * @return true if the current execution of the job is cancelled
     */
    boolean restart();

}
