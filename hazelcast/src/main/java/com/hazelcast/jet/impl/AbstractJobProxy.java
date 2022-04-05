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

package com.hazelcast.jet.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LocalMemberResetException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

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

    /** Null for normal jobs, non-null for light jobs  */
    protected final M lightJobCoordinator;

    private final long jobId;
    private final ILogger logger;
    private final C container;

    /**
     * Future that will be completed when we learn that the coordinator
     * completed the job, but only if {@link #joinedJob} is true.
     */
    private final NonCompletableFuture future;

    // Flag which indicates if this proxy has sent a request to join the job result or not
    private final AtomicBoolean joinedJob = new AtomicBoolean();
    private final BiConsumer<Void, Throwable> joinJobCallback;

    private volatile JobConfig jobConfig;
    private final Supplier<Long> submissionTimeSup = memoizeConcurrent(this::doGetJobSubmissionTime);

    /**
     * True if this instance submitted the job. False if it was created later
     * to track existing job.
     */
    private final boolean submittingInstance;

    AbstractJobProxy(C container, long jobId, M lightJobCoordinator) {
        this.jobId = jobId;
        this.container = container;
        this.lightJobCoordinator = lightJobCoordinator;

        logger = loggingService().getLogger(AbstractJobProxy.class);
        future = new NonCompletableFuture();
        joinJobCallback = new JoinJobCallback();
        submittingInstance = false;
    }

    AbstractJobProxy(C container, long jobId, boolean isLightJob, @Nonnull Object jobDefinition, @Nonnull JobConfig config) {
        this.jobId = jobId;
        this.container = container;
        this.lightJobCoordinator = isLightJob ? findLightJobCoordinator() : null;
        this.logger = loggingService().getLogger(Job.class);
        submittingInstance = true;

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

    @Nonnull @Override
    public JobConfig getConfig() {
        // The common path will use a single volatile load
        JobConfig loadResult = jobConfig;
        if (loadResult != null) {
            return loadResult;
        }
        synchronized (this) {
            // The uncommon path can use simpler code with multiple volatile loads
            if (jobConfig != null) {
                return jobConfig;
            }
            jobConfig = doGetJobConfig();
            if (jobConfig == null) {
                throw new NullPointerException("Supplier returned null");
            }
            return jobConfig;
        }
    }

    /**
     * Returns the string {@code <jobId> (name <jobName>)} without risking
     * triggering of lazy-loading of JobConfig: if we don't have it, it will
     * say {@code name ??}. If we have it and it is null, it will say {@code
     * name ''}.
     */
    private String idAndName() {
        JobConfig config = jobConfig;
        return getIdString() + " (name "
                + (config != null ? "'" + (config.getName() != null ? config.getName() : "") + "'" : "??")
                + ')';
    }

    @Nonnull @Override
    public CompletableFuture<Void> getFuture() {
        if (joinedJob.compareAndSet(false, true)) {
            doInvokeJoinJob();
        }
        return future;
    }

    @Nonnull @Override
    public final JobStatus getStatus() {
        if (isLightJob()) {
            CompletableFuture<Void> f = getFuture();
            if (!f.isDone()) {
                return JobStatus.RUNNING;
            }
            return f.isCompletedExceptionally()
                    ? JobStatus.FAILED : JobStatus.COMPLETED;
        } else {
            return getStatus0();
        }
    }

    protected abstract JobStatus getStatus0();

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
                try {
                    invokeTerminateJob(mode).get();
                    break;
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof JobNotFoundException) || !isLightJob()) {
                        throw e;
                    }
                    if (submittingInstance) {
                        // it can happen that we enqueued the submit operation, but the master handled
                        // the terminate op before the submit op and doesn't yet know about the job. But
                        // it can be that the job already completed, we don't know. We'll look at the submit
                        // future, if it's done, the job is done. Otherwise we'll retry - the job will eventually
                        // start or complete.
                        // This scenario is possible only on the client or lite member. On normal member,
                        // the submit op is executed directly.
                        assert joinedJob.get() : "not joined";
                        if (getFuture().isDone()) {
                            return;
                        }
                    } else {
                        // This instance is an output of one of the JetService.getJob() or getJobs() methods.
                        // That means that the job was already known to some member and since it's not
                        // known anymore, it's safe to assume it already completed.
                        return;
                    }
                }
                LockSupport.parkNanos(TERMINATE_RETRY_DELAY_NS);
            } catch (Exception e) {
                if (!isRestartable(e)) {
                    throw rethrow(e);
                }
                logger.fine("Re-sending " + mode + " request for job " + idAndName());
            }
        }
    }

    @Override
    public String toString() {
        return "Job{id=" + getIdString()
                + ", name=" + getName()
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

    protected abstract boolean isRunning();

    protected C container() {
        return container;
    }

    private NonCompletableFuture doSubmitJob(Object jobDefinition, JobConfig config) {
        NonCompletableFuture submitFuture = new NonCompletableFuture();
        SubmitJobCallback callback = new SubmitJobCallback(submitFuture, jobDefinition, config);
        invokeSubmitJob(jobDefinition, config).whenCompleteAsync(callback);
        return submitFuture;
    }

    private boolean isRestartable(Throwable t) {
        if (isLightJob()) {
            return false;
        }
        // these exceptions are restartable only for non-light jobs. If the light job coordinator leaves
        // or disconnects, the job fails. For normal jobs, the new master will take over.
        return t instanceof MemberLeftException
                || t instanceof TargetDisconnectedException
                || t instanceof TargetNotMemberException
                || t instanceof HazelcastInstanceNotActiveException && isRunning();
    }

    private void doInvokeJoinJob() {
        invokeJoinJob()
                .whenComplete(withTryCatch(logger, (r, t) -> {
                    if (isLightJob() && t instanceof JobNotFoundException) {
                        throw new IllegalStateException("job already completed");
                    }
                }))
                .whenCompleteAsync(withTryCatch(logger, joinJobCallback));
    }

    protected void checkNotLightJob(String msg) {
        if (isLightJob()) {
            throw new UnsupportedOperationException("not supported for light jobs: " + msg);
        }
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
                // calling for the side-effect of throwing ISE if master not known
                masterId();
            } catch (IllegalStateException e) {
                // job data will be cleaned up eventually by the coordinator
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
