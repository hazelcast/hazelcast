/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LocalMemberResetException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStatusListener;
import com.hazelcast.jet.config.DeltaJobConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.exception.CancellationByUserException;
import com.hazelcast.jet.impl.operation.AddJobStatusListenerOperation;
import com.hazelcast.jet.impl.operation.UpdateJobConfigOperation;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.eventservice.impl.Registration;
import com.hazelcast.spi.impl.eventservice.impl.operations.RegistrationOperation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.util.ExceptionUtil.ignoreException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.memoizeConcurrent;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Base {@link Job} implementation for both client and member proxy.
 *
 * @param <C> the type of container (the client instance or the node engine)
 * @param <M> the type of member ID (UUID or Address)
 */
public abstract class AbstractJobProxy<C, M> implements Job {
    private static final long TERMINATE_RETRY_DELAY_NS = MILLISECONDS.toNanos(100);

    // We intentionally do a `new String` to not have an interned copy of the string.
    @SuppressWarnings("StringOperationCanBeSimplified")
    private static final String NOT_LOADED = new String("NOT_LOADED");

    /** Null for normal jobs, non-null for light jobs  */
    protected final M lightJobCoordinator;

    private final long jobId;
    private volatile String name = NOT_LOADED;
    private final ILogger logger;
    private final C container;

    /**
     * Future that will be completed when we learn that the coordinator
     * completed the job, but only if {@link #joinedJob} is true.
     */
    private final NonCompletableFuture future;

    /** Indicates whether this proxy has sent a request to join the job result or not. */
    private final AtomicBoolean joinedJob = new AtomicBoolean();
    private final BiConsumer<Void, Throwable> joinJobCallback;
    private final Supplier<Long> submissionTimeSup = memoizeConcurrent(this::doGetJobSubmissionTime);

    AbstractJobProxy(C container, long jobId, M lightJobCoordinator) {
        this.jobId = jobId;
        this.container = container;
        this.lightJobCoordinator = lightJobCoordinator;

        logger = loggingService().getLogger(AbstractJobProxy.class);
        future = new NonCompletableFuture();
        joinJobCallback = new JoinJobCallback();
    }

    AbstractJobProxy(C container, long jobId, boolean isLightJob, @Nonnull Object jobDefinition, @Nonnull JobConfig config) {
        this.jobId = jobId;
        this.container = container;
        this.lightJobCoordinator = isLightJob ? findLightJobCoordinator() : null;
        this.logger = loggingService().getLogger(Job.class);

        try {
            NonCompletableFuture submitFuture = doSubmitJob(jobDefinition, config);
            joinedJob.set(true);
            // For light jobs, the future of the submit operation is also the job future.
            // For normal jobs, we invoke the join operation separately.
            if (isLightJob) {
                future = submitFuture;
                joinJobCallback = null;
            } else {
                submitFuture.join();
                future = new NonCompletableFuture();
                joinJobCallback = new JoinJobCallback();
                doInvokeJoinJob();
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public long getId() {
        return jobId;
    }

    @Nullable @Override
    @SuppressWarnings({"StringEquality", "java:S4973"})
    public String getName() {
        if (isLightJob()) {
            return null;
        }
        if (name == NOT_LOADED) {
            return getConfig().getName();
        }
        return name;
    }

    @Nonnull @Override
    public JobConfig getConfig() {
        synchronized (this) {
            JobConfig config = doGetJobConfig();
            if (config == null) {
                throw new NullPointerException("Supplier returned null");
            }
            name = config.getName();
            return config;
        }
    }

    @Override
    public JobConfig updateConfig(@Nonnull DeltaJobConfig deltaConfig) {
        checkNotLightJob("updateConfig");
        synchronized (this) {
            JobConfig config = doUpdateJobConfig(deltaConfig);
            name = config.getName();
            return config;
        }
    }

    /**
     * Returns the string {@code <jobId> (name <jobName>)}, where {@code <jobName>} is
     * {@code ??} if it is not loaded yet via {@link #getName()} or {@link #getConfig()}.
     * Otherwise, it is single-quoted and also empty for unnamed jobs.
     */
    @SuppressWarnings("StringEquality")
    private String idAndName() {
        return getIdString() + " (name " +
                (name == NOT_LOADED ? "??" : "'" + (name != null ? name : "") + "'") + ")";
    }

    @Override
    public void join() {
        joinFuture(false);
        future.join();
    }

    @Nonnull @Override
    public CompletableFuture<Void> getFuture() {
        joinFuture(true);
        return future;
    }

    @Nonnull @Override
    public final JobStatus getStatus() {
        if (!future.isDone()) {
            JobStatus status = isLightJob() && joinedJob.get() ? RUNNING : getStatus0();
            if (status != null) {
                return status;
            }
        }
        Throwable t = getResult();
        if (t == null) {
            return COMPLETED;
        } else if (isFailureOrCancellation(t)) {
            return FAILED;
        }
        throw rethrow(t);
    }

    /**
     * Gets the job status and waits until {@link #future} is completed if the status is terminal.
     * If the job is not found, returns null. In this case, the job status should be obtained from
     * the result of {@code future} since {@link JobNotFoundException} causes waiting for the
     * completion of {@code future} in {@link #joinAndInvoke}.
     */
    private JobStatus getStatus0() {
        try {
            JobStatus status = getStatus1();
            if (status.isTerminal()) {
                ignoreException(future::join);
            }
            return status;
        } catch (JobNotFoundException ignored) {
            return null;
        }
    }

    protected abstract JobStatus getStatus1();

    @Override
    public final boolean isUserCancelled() {
        joinFuture(true);
        if (!future.isDone()) {
            throw new IllegalStateException("Job not finished");
        }
        Throwable t = getResult();
        if (t == null) {
            return false;
        } else if (isFailureOrCancellation(t)) {
            return t instanceof CancellationByUserException;
        }
        throw rethrow(t);
    }

    @Override
    public long getSubmissionTime() {
        return submissionTimeSup.get();
    }

    @Override
    public void cancel() {
        terminate(TerminationMode.CANCEL_FORCEFUL);
    }

    @Override
    public void restart() {
        terminate(TerminationMode.RESTART_GRACEFUL);
    }

    public void restart(boolean graceful) {
        terminate(graceful ? TerminationMode.RESTART_GRACEFUL : TerminationMode.RESTART_FORCEFUL);
    }

    @Override
    public void suspend() {
        terminate(TerminationMode.SUSPEND_GRACEFUL);
    }

    private void terminate(TerminationMode mode) {
        if (mode != TerminationMode.CANCEL_FORCEFUL) {
            checkNotLightJob(mode.toString());
        }
        logger.fine("Sending " + mode + " request for job " + idAndName());
        while (true) {
            try {
                joinAndInvoke(() -> invokeTerminateJob(mode).get());
                break;
            } catch (Exception e) {
                if (e instanceof JobNotFoundException) {
                    if (!(getResult() instanceof JobNotFoundException)) {
                        // Job completed/failed/cancelled just before we send termination request.
                        break;
                    }
                    throw e;
                } else if (!(isRestartable(e) || e instanceof IllegalStateException)) {
                    throw e;
                }
            }
            LockSupport.parkNanos(TERMINATE_RETRY_DELAY_NS);
            logger.fine("Re-sending " + mode + " request for job " + idAndName());
        }
    }

    /**
     * Joins {@link #future} if not already joined, optionally in a synchronous manner.
     * @implNote Joining {@link #future} is an asynchronous operation, so {@code future.isDone()}
     * is not expected to give sensible results just after join request is sent. When we can get
     * the first reading depends on member load and network speed, which are nondeterministic.
     * The first reading may be improved by sending another operation, which is synchronous. <ol>
     * <li> If the job is completed and removed from the coordinator before invoking this method,
     *      future will be waited for completion, which will complete with JobNotFoundException.
     * <li> If the job exists on the coordinator when this method is invoked, but removed before
     *      sending the second operation, future will be waited for completion, which may complete
     *      normally or exceptionally.
     * <li> If the job exists on the coordinator throughout the invocation of this method, future
     *      may or may not be completed on return.
     * @see #joinAndInvoke(SupplierEx) joinAndInvoke(operation)
     */
    private void joinFuture(boolean synchronous) {
        if (joinedJob.compareAndSet(false, true)) {
            doInvokeJoinJob();
            if (synchronous) {
                getStatus0();
            }
        }
        if (synchronous && future.isDone()) {
            Throwable t = getResult();
            if (t instanceof JobNotFoundException) {
                throw rethrow(t);
            }
        }
    }

    /**
     * Joins {@link #future} if not already joined and returns the result of the operation.
     * If the operation throws {@link JobNotFoundException}, {@link #future} will be waited
     * for completion and the exception will be rethrown. In this case, the caller will check
     * the result of {@link #future} to decide to retry operation, throw exception, etc.
     */
    protected <T> T joinAndInvoke(SupplierEx<T> operation) {
        if (joinedJob.compareAndSet(false, true)) {
            doInvokeJoinJob();
        }
        try {
            return operation.get();
        } catch (Exception e) {
            Throwable t = peel(e);
            if (t instanceof JobNotFoundException) {
                ignoreException(future::join);
                throw jobAlreadyRemoved();
            } else {
                throw rethrow(t);
            }
        }
    }

    private Throwable getResult() {
        try {
            future.getNow(null);
            return null;
        } catch (Exception e) {
            return peel(e);
        }
    }

    @Override
    public UUID addStatusListener(@Nonnull JobStatusListener listener) {
        try {
            return doAddStatusListener(listener);
        } catch (JobNotFoundException ignored) {
            throw cannotAddStatusListener(
                    future.isDone() ? future.isCompletedExceptionally() ? FAILED : COMPLETED : "completed/failed");
        }
    }

    @SuppressWarnings("StringEquality")
    @Override
    public String toString() {
        return "Job{id=" + getIdString()
                + ", name=" + (name == NOT_LOADED ? "??" : name)
                // Don't include these, they do remote calls and wreak havoc when the debugger tries to display
                // the string value. They can also fail at runtime.
                //+ ", submissionTime=" + toLocalDateTime(getSubmissionTime())
                //+ ", status=" + getStatus()
                + "}";
    }

    @Override
    public boolean isLightJob() {
        return lightJobCoordinator != null;
    }

    protected abstract M findLightJobCoordinator();

    /**
     * Submit and join job with a given DAG and config
     */
    protected abstract CompletableFuture<Void> invokeSubmitJob(Object jobDefinition, JobConfig config);

    /**
     * Join already existing job
     */
    protected abstract CompletableFuture<Void> invokeJoinJob();

    protected abstract CompletableFuture<Void> invokeTerminateJob(TerminationMode mode);

    protected abstract long doGetJobSubmissionTime();

    protected abstract JobConfig doGetJobConfig();

    /**
     * Applies the specified delta configuration to this job and returns the updated
     * configuration. Synchronization with {@link #getConfig()} is handled by {@link
     * #updateConfig}.
     * @implNote
     * Sends an {@link UpdateJobConfigOperation} to the master member. On the master
     * member, if the job is SUSPENDED, the job record is updated both locally and
     * {@linkplain JobRepository#JOB_RECORDS_MAP_NAME globally} (in order for {@link
     * #getConfig()} to reflect the changes); otherwise, the operation fails.
     */
    protected abstract JobConfig doUpdateJobConfig(@Nonnull DeltaJobConfig deltaConfig);

    /**
     * Associates the specified listener to this job.
     * @throws JobNotFoundException if the job's master context is cleaned up after job
     *         completion/failure. This is translated to {@link IllegalStateException} by
     *         {@link #addStatusListener}.
     * @implNote
     * Listeners added to a job after it completes will not be removed automatically since
     * the job has already produced a terminal event. In order to make auto-deregistration
     * race-free, it is not allowed to add listeners to completed jobs. Checking the job
     * status before the listener registration will not work since they are not atomic. The
     * registration should be delegated to the job coordinator, but the {@code listener}
     * is local. To overcome this, the following algorithm is used: <ol>
     * <li> A {@link Registration} object is created with a unique registration id. The
     *      {@code listener} is cached locally by the registration id.
     * <li> The {@link Registration} object is delivered to the job coordinator via an
     *      {@link AddJobStatusListenerOperation}. If the job is not completed/failed, the
     *      coordinator invokes a {@link RegistrationOperation} on the subscriber member
     *      —or all members if the registration is global. The registration operation is
     *      guaranteed to be executed earlier than a possible terminal event since the
     *      operation is executed as an event callback with the same {@code orderKey} as
     *      job events.
     * <li> When the subscriber member receives the {@link RegistrationOperation}, the
     *      {@link Registration}'s {@code listener} is restored from the cache and the
     *      registration is completed. </ol>
     */
    protected abstract UUID doAddStatusListener(@Nonnull JobStatusListener listener);

    /**
     * Return the ID of the coordinator - the master member for normal jobs and
     * the {@link #lightJobCoordinator} for light jobs.
     */
    protected M coordinatorId() {
        return lightJobCoordinator != null ? lightJobCoordinator : masterId();
    }

    /**
     * Get the current master ID.
     *
     * @throws IllegalStateException if the master isn't known
     */
    @Nonnull
    protected abstract M masterId();

    protected abstract SerializationService serializationService();

    protected abstract LoggingService loggingService();

    protected abstract boolean isContainerRunning();

    protected C container() {
        return container;
    }

    private NonCompletableFuture doSubmitJob(Object jobDefinition, JobConfig config) {
        NonCompletableFuture submitFuture = new NonCompletableFuture();
        SubmitJobCallback callback = new SubmitJobCallback(submitFuture, jobDefinition, config);
        invokeSubmitJob(jobDefinition, config).whenCompleteAsync(callback);
        return submitFuture;
    }

    private boolean isFailureOrCancellation(Throwable t) {
        return t instanceof CancellationException
                || (t instanceof JetException && !(t instanceof JobNotFoundException));
    }

    /** @see ExceptionUtil#isRestartableException */
    private boolean isRestartable(Throwable t) {
        if (isLightJob()) {
            return false;
        }
        // These exceptions are restartable only for non-light jobs. If the light job coordinator
        // leaves or disconnects, the job fails. For normal jobs, the new master will take over.
        return isContainerRunning()
                && (t instanceof MemberLeftException
                || t instanceof TargetDisconnectedException
                || t instanceof TargetNotMemberException
                || t instanceof HazelcastInstanceNotActiveException);
    }

    private void doInvokeJoinJob() {
        invokeJoinJob()
                .whenComplete((r, t) -> {
                    if (peel(t) instanceof JobNotFoundException) {
                        throw jobAlreadyRemoved();
                    }
                })
                .whenCompleteAsync(withTryCatch(logger, joinJobCallback));
    }

    protected void checkNotLightJob(String msg) {
        if (isLightJob()) {
            throw new UnsupportedOperationException("not supported for light jobs: " + msg);
        }
    }

    public static IllegalStateException cannotAddStatusListener(Object status) {
        return new IllegalStateException("Cannot add status listener to a " + status + " job");
    }

    private JobNotFoundException jobAlreadyRemoved() {
        return new JobNotFoundException("Job " + idAndName() + " is already removed. Call getFuture()"
                + " after obtaining a job reference to be able to know later how it completed/failed.");
    }

    private abstract class CallbackBase implements BiConsumer<Void, Throwable> {
        private final NonCompletableFuture future;

        protected CallbackBase(NonCompletableFuture future) {
            this.future = future;
        }

        @Override
        public final void accept(Void aVoid, Throwable t) {
            if (t != null) {
                Throwable ex = peel(t);
                if (ex instanceof LocalMemberResetException) {
                    String msg = operationName() + " failed for job " + idAndName()
                            + " because the cluster is performing split-brain merge";
                    logger.warning(msg, ex);
                    future.internalCompleteExceptionally(new CancellationException(msg));
                } else if (!isRestartable(ex)) {
                    future.internalCompleteExceptionally(ex);
                } else {
                    try {
                        retryAction(ex);
                    } catch (Exception e) {
                        future.internalCompleteExceptionally(peel(e));
                    }
                }
            } else {
                future.internalComplete();
            }
        }

        private void retryAction(Throwable t) {
            try {
                // Calling for the side effect of throwing IllegalStateException if master is not known
                masterId();
            } catch (IllegalStateException e) {
                // Job data will be cleaned up eventually by the coordinator
                String msg = operationName() + " failed for job " + idAndName() + " because the cluster " +
                        "is performing split-brain merge and the coordinator is not known";
                logger.warning(msg, t);
                future.internalCompleteExceptionally(new CancellationException(msg));
                return;
            }
            retryActionInt(t);
        }

        protected abstract void retryActionInt(Throwable t);

        protected abstract String operationName();
    }

    private class SubmitJobCallback extends CallbackBase {
        private final Object jobDefinition;
        private final JobConfig config;

        SubmitJobCallback(NonCompletableFuture future, Object jobDefinition, JobConfig config) {
            super(future);
            this.jobDefinition = jobDefinition;
            this.config = config;
        }

        @Override
        protected void retryActionInt(Throwable t) {
            logger.fine("Resubmitting job " + idAndName() + " after " + t.getClass().getSimpleName());
            invokeSubmitJob(jobDefinition, config).whenCompleteAsync(this);
        }

        @Override
        protected String operationName() {
            return "Submit";
        }
    }

    private class JoinJobCallback extends CallbackBase {

        JoinJobCallback() {
            super(AbstractJobProxy.this.future);
        }

        @Override
        protected void retryActionInt(Throwable t) {
            logger.fine("Rejoining to job " + idAndName() + " after " + t.getClass().getSimpleName(), t);
            doInvokeJoinJob();
        }

        @Override
        protected String operationName() {
            return "Join";
        }
    }
}
