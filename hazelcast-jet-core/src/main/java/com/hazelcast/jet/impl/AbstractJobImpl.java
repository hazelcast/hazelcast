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
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.idToString;

public abstract class AbstractJobImpl implements Job {

    private final ILogger logger;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    AbstractJobImpl(ILogger logger) {
        this.logger = logger;
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> getFuture() {
        return future;
    }

    protected abstract Address getMasterAddress();

    protected abstract ICompletableFuture<Void> sendJoinRequest(Address masterAddress);

    protected abstract JobStatus sendJobStatusRequest();

    /**
     * Sends a JoinOp to ensure that the job is started as soon as possible
     */
    void init() {
        Address masterAddress = getMasterAddress();
        if (masterAddress == null) {
            throw new IllegalStateException("Master address is null");
        }

        ICompletableFuture<Void> invocationFuture = sendJoinRequest(masterAddress);
        JobCallback callback = new JobCallback(invocationFuture);
        invocationFuture.andThen(callback);
        future.whenComplete(withTryCatch(logger, (aVoid, throwable) -> {
            if (throwable instanceof CancellationException) {
                callback.cancel();
            }
        }));
    }

    @Nonnull
    @Override
    public final JobStatus getJobStatus() {
        if (!future.isCancelled()) {
            // only check for the completion of the future is the job is cancelled
            // - in which case the future doesn't indicate if the job is still running
            // on the cluster or not
            if (future.isCompletedExceptionally()) {
                return JobStatus.FAILED;
            } else if (future.isDone()) {
                return JobStatus.COMPLETED;
            }
        }
        return sendJobStatusRequest();
    }

    private class JobCallback implements ExecutionCallback<Void> {

        private volatile ICompletableFuture<Void> invocationFuture;

        JobCallback(ICompletableFuture<Void> invocationFuture) {
            this.invocationFuture = invocationFuture;
        }

        @Override
        public void onResponse(Void response) {
            future.complete(response);
        }

        @Override
        public synchronized void onFailure(Throwable t) {
            long jobId = getJobId();
            if (isSplitBrainMerge(t)) {
                String msg = "Job " + idToString(jobId) + " failed because the cluster is performing split-brain merge";
                logger.fine(msg, t);
                future.completeExceptionally(new CancellationException(msg));
            } else if (isRestartable(t)) {
                try {
                    Address masterAddress = getMasterAddress();
                    if (masterAddress == null) {
                        // job data will be cleaned up eventually by coordinator
                        String msg = "Job " + idToString(jobId) + " failed because the cluster is performing "
                                + " split-brain merge and coordinator is not known";
                        logger.fine(msg, t);
                        future.completeExceptionally(new CancellationException(msg));
                        return;
                    }

                    logger.fine("Re-joining to Job " + idToString(jobId) + " after " + t.getClass().getSimpleName());

                    ICompletableFuture<Void> invocationFuture = sendJoinRequest(masterAddress);
                    this.invocationFuture = invocationFuture;
                    invocationFuture.andThen(this);

                    if (future.isCancelled()) {
                        invocationFuture.cancel(true);
                    }
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            } else {
                future.completeExceptionally(peel(t));
            }
        }

        private boolean isRestartable(Throwable t) {
            Throwable cause = peel(t);
            return cause  instanceof MemberLeftException
                    || cause instanceof TargetDisconnectedException
                    || cause instanceof TargetNotMemberException;
        }

        private boolean isSplitBrainMerge(Throwable t) {
            Throwable cause = peel(t);
            return (cause instanceof LocalMemberResetException);
        }

        public synchronized void cancel() {
            invocationFuture.cancel(true);
        }

    }
}
