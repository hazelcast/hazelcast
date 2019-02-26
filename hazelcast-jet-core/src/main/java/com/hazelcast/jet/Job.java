/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/**
 * A Jet computation job created by submitting a {@link DAG} or {@link
 * Pipeline}. Once submitted, Jet starts executing the job automatically.
 */
public interface Job {

    /**
     * Returns the ID of this job.
     *
     * @throws IllegalStateException if the job has not started yet, and thus has no ID.
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
     * <p>
     * The time is assigned by reading {@code System.currentTimeMillis()} of
     * the master member that executes the job for the first time. It doesn't
     * change on restart.
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
     * Waits for the job to complete and throws an exception if the job
     * completes with an error. Does not return if the job gets suspended.
     * Never returns for streaming (unbounded) jobs, unless they fail or are
     * cancelled.
     * <p>
     * Shorthand for <code>job.getFuture().join()</code>.
     *
     * @throws CancellationException if the job was cancelled
     */
    default void join() {
        getFuture().join();
    }

    /**
     * Gracefully stops the current execution and schedules a new execution
     * with the current member list of the Jet cluster. Can be called to
     * manually make use of added members, if {@linkplain
     * JobConfig#setAutoScaling auto scaling} is disabled. Only a running job
     * can be restarted; a suspended job must be {@linkplain #resume() resumed}.
     * <p>
     * Conceptually this call is equivalent to {@link #suspend()} & {@link
     * #resume()}.
     *
     * @throws IllegalStateException if the job is not running, for example it
     * has already completed, is not yet running, is already restarting,
     * suspended etc.
     */
    void restart();

    /**
     * Gracefully suspends the current execution of the job. The job's status
     * will become {@link JobStatus#SUSPENDED}. To resume the job, call {@link
     * #resume()}.
     * <p>
     * You can suspend a job even if it's not configured for {@linkplain
     * JobConfig#setProcessingGuarantee snapshotting}. Such a job will resume
     * with empty state, as if it has just been started.
     * <p>
     * This call just initiates the suspension process and doesn't wait for it
     * to complete. Suspension starts with creating a terminal state snapshot.
     * Should the terminal snapshot fail, the job will suspend anyway, but the
     * previous snapshot (if there was one) won't be deleted. When the job
     * resumes, its processing starts from the point of the last snapshot.
     * <p>
     * <strong>NOTE:</strong> if the cluster becomes unstable (a member leaves or
     * similar) while the job is in the process of being suspended, it may end up
     * getting immediately restarted. Call {@link #getStatus()} to find out and
     * possibly try to suspend again.
     *
     * @throws IllegalStateException if the job is not running
     */
    void suspend();

    /**
     * Resumes a {@linkplain #suspend suspended} job. The job will resume from
     * the last known successful snapshot, if there is one.
     *
     * <p>If the job is not suspended, it does nothing.
     */
    void resume();

    /**
     * Makes a request to cancel this job and returns. The job will complete
     * after its execution has stopped on all the nodes. If the job is
     * already suspended, Jet will delete its runtime resources and snapshots
     * and it won't be able to resume again.
     * <p>
     * <strong>NOTE:</strong> if the cluster becomes unstable (a member leaves
     * or similar) while the job is in the process of cancellation, it may end
     * up getting restarted after the cluster has stabilized and won't be
     * cancelled. Call {@link #getStatus()} to find out and possibly try to
     * cancel again.
     * <p>
     * The job status will be {@link JobStatus#FAILED} after cancellation,
     * {@link Job#join()} will throw a {@link CancellationException}.
     * <p>
     * See {@link #cancelAndExportSnapshot(String)} to cancel with a terminal
     * snapshot.
     *
     * @throws IllegalStateException if the cluster is not in a state to
     * restart the job, for example when coordinator member left and new
     * coordinator did not yet load job's metadata.
     */
    void cancel();

    /**
     * Exports and saves a state snapshot with the given name,
     * and then cancels the job without processing any more data after the
     * barrier (graceful cancellation). It's similar to {@link #suspend()}
     * followed by a {@link #cancel()}, except that it won't process any more
     * data data after the snapshot.
     * <p>
     * You can use the exported snapshot as a starting point for a new job. The
     * job doesn't need to execute the same Pipeline as the job that created it,
     * it must just be compatible with its state data. To achieve this, use
     * {@link JobConfig#setInitialSnapshotName(String)}.
     * <p>
     * If the terminal snapshot fails, Jet will suspend this job instead of
     * cancelling it.
     * <p>
     * You can call this method for a suspended job, too: in that case it will
     * export the last successful snapshot and cancel the job.
     * <p>
     * The method call will block until it has fully exported the snapshot, but
     * may return before the job has stopped executing.
     * <p>
     * For more information about "exported state" see {@link
     * #exportSnapshot(String)}.
     * <p>
     * The job status will be {@link JobStatus#FAILED} after cancellation,
     * {@link Job#join()} will throw a {@link CancellationException}.
     *
     * @param name name of the snapshot. If name is already used, it will be
     *            overwritten
     */
    JobStateSnapshot cancelAndExportSnapshot(String name);

    /**
     * Exports a state snapshot and saves it under the given
     * name. You can start a new job using the exported state using {@link
     * JobConfig#setInitialSnapshotName(String)}.
     * <p>
     * The snapshot will be independent from the job that created it. Jet
     * won't automatically delete the IMap it is exported into. You must
     * manually call {@linkplain JobStateSnapshot#destroy() snapshot.destroy()}
     * to delete it. If your state is large, make sure you have enough memory
     * to store it.
     * <p>
     * If a snapshot with the same name already exists, it will be
     * overwritten. If a snapshot is already in progress for this job (either
     * automatic or user-requested), the requested one will wait and start
     * immediately after the previous one completes. If a snapshot with the
     * same name is requested for two jobs at the same time, their data will
     * likely be damaged (similar to two processes writing to the same file).
     * <p>
     * You can call this method on a suspended job: in that case it will export
     * the last successful snapshot. You can also export the state of
     * non-snapshotted jobs (those with {@link ProcessingGuarantee#NONE}).
     * <p>
     * If you issue any graceful job-control actions such as a graceful member
     * shutdown or suspending a snapshotted job while Jet is exporting a
     * snapshot, they will wait in a queue for this snapshot to complete.
     * Forceful job-control actions will interrupt the export procedure.
     * <p>
     * You can access the exported state map using {@link
     * JetInstance#getJobStateSnapshot(String)}.
     *
     * @param name name of the snapshot. If name is already used, it will be
     *            overwritten
     * @throws JetException if the job is in an incorrect state: completed,
     *            cancelled, or also in the process of restarting or
     *            suspending.
     */
    JobStateSnapshot exportSnapshot(String name);
}
