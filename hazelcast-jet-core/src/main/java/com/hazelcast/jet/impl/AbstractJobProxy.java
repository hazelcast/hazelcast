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

package com.hazelcast.jet.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.LocalMemberResetException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.completeVoidFuture;
import static com.hazelcast.jet.impl.util.Util.idToString;

/**
 * Base {@link Job} implementation for both client and member proxy.
 */
public abstract class AbstractJobProxy<T> implements Job {

    private final long jobId;
    private final ILogger logger;
    private final T container;

    /**
     * Future that will be completed when we learn that the coordinator
     * completed the job, but only if {@link #joinedJob} is true.
     */
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    /**
     * Flag to indicate if job status should be retried. This avoids the
     * race between submitting a job and getting the status for it when
     * the proxy is created with a new job.
     */
    private final boolean shouldRetryJobStatus;

    /** Flag which indicates if this proxy has sent a request to join the job result or not */
    private final AtomicBoolean joinedJob = new AtomicBoolean();

    AbstractJobProxy(T container, long jobId) {
        this.jobId = jobId;
        this.container = container;
        this.logger = createLogger();
        this.shouldRetryJobStatus = false;
    }

    AbstractJobProxy(T container, long jobId, DAG dag, JobConfig config) {
        this.jobId = jobId;
        this.container = container;
        this.logger = createLogger();
        this.shouldRetryJobStatus = true;

        joinedJob.set(true);
        handleInvocation(invokeSubmitJob(serializationService().toData(dag), config));
    }

    @Override
    public long getJobId() {
        return jobId;
    }

    @Nonnull @Override
    public CompletableFuture<Void> getFuture() {
        if (joinedJob.compareAndSet(false, true)) {
            handleInvocation(invokeJoinJob());
        }
        return future;
    }

    @Override
    public String toString() {
        return "Job{id=" + idToString(jobId) + "}";
    }

    /**
     * Submit and join job with a given DAG and config
     */
    protected abstract ICompletableFuture<Void> invokeSubmitJob(Data dag, JobConfig config);

    /**
     * Join already existing job
     */
    protected abstract ICompletableFuture<Void> invokeJoinJob();

    protected abstract Address masterAddress();

    protected abstract SerializationService serializationService();

    protected abstract LoggingService loggingService();

    protected boolean shouldRetryJobStatus() {
        return shouldRetryJobStatus;
    }

    protected ILogger getLogger() {
        return logger;
    }

    protected T container() {
        return container;
    }

    private void handleInvocation(ICompletableFuture<Void> invocationFuture) {
        invocationFuture.andThen(new JobCallback());
        future.whenComplete(withTryCatch(logger, (r, t) -> {
            if (t instanceof CancellationException) {
                invocationFuture.cancel(true);
            }
        }));
    }

    private ILogger createLogger() {
        return loggingService().getLogger(Job.class + "." + idToString(jobId));
    }

    private class JobCallback implements ExecutionCallback<Void> {

        @Override
        public void onResponse(Void response) {
            // job completed successfully
            completeVoidFuture(future);
        }

        @Override
        public synchronized void onFailure(Throwable t) {
            Throwable ex = peel(t);
            if ((ex instanceof LocalMemberResetException)) {
                String msg = "Job " + idToString(jobId) + " failed because the cluster is performing split-brain merge.";
                logger.fine(msg, ex);
                future.completeExceptionally(new CancellationException(msg));
            } else if (!isRestartable(ex)) {
                future.completeExceptionally(ex);
            } else {
                try {
                    rejoinJob(t);
                } catch (Exception e) {
                    future.completeExceptionally(peel(e));
                }
            }
        }

        private void rejoinJob(Throwable t) {
            if (masterAddress() != null) {
                logger.fine("Rejoining to job " + idToString(jobId) + " after " + t.getClass().getSimpleName());
                handleInvocation(invokeJoinJob());
                return;
            }
            // job data will be cleaned up eventually by coordinator
            String msg = "Job " + idToString(jobId) + " failed because the cluster is performing "
                    + " split-brain merge and coordinator is not known";
            logger.fine(msg, t);
            future.completeExceptionally(new CancellationException(msg));
        }

        private boolean isRestartable(Throwable t) {
            return t instanceof MemberLeftException
                    || t instanceof TargetDisconnectedException
                    || t instanceof TargetNotMemberException;
        }

    }

}
