/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.pipeline.Pipeline;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/**
 * A handle to a submitted job. See {@link
 * JetService#newLightJob(Pipeline)}.
 */
public interface LightJob {

    /**
     * Returns the ID of this job.
     */
    long getId();

    /**
     * Returns the string representation of this job's ID.
     */
    @Nonnull
    default String getIdString() {
        return Util.idToString(getId());
    }

    /**
     * Waits for the job to complete and throws an exception if the job
     * completes with an error. Never returns for streaming (unbounded) jobs,
     * unless they fail or are cancelled. In rare cases it can happen that
     * after this method returns, the job is not fully cleaned up.
     * <p>
     * Shorthand for <code>job.getFuture().join()</code>.
     *
     * @throws CancellationException if the job was cancelled
     */
    default void join() {
        getFuture().join();
    }

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
     * Makes a request to cancel this job and returns. The job will complete
     * after its execution has stopped on all the nodes.
     * <p>
     * After cancellation, {@link #join()} will throw a {@link
     * CancellationException}.
     */
    void cancel();
}
