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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.AbstractInvocationFuture;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.VOID;

/**
 * BaseInvocation for client and core.
 * Keep track of received response and number of backups.
 */
public abstract class BaseInvocation {

    private static final AtomicIntegerFieldUpdater<BaseInvocation> BACKUP_ACKS_RECEIVED =
            AtomicIntegerFieldUpdater.newUpdater(BaseInvocation.class, "backupsAcksReceived");

    protected final ILogger logger;

    /**
     * Number of backups acks received.
     */
    protected volatile int backupsAcksReceived;

    /**
     * Number of expected backups. It is set correctly as soon as the pending response is set.
     */
    volatile int backupsAcksExpected = -1;

    /**
     * The time in millis when the response of the primary has been received.
     */
    volatile long pendingResponseReceivedMillis = -1;

    /**
     * Contains the pending response from the primary. It is pending because it could be that backups need to complete.
     */
    volatile Object pendingResponse = VOID;

    protected BaseInvocation(ILogger logger) {
        this.logger = logger;
    }

    public void notifyBackupComplete() {
        int newBackupAcksCompleted = BACKUP_ACKS_RECEIVED.incrementAndGet(this);

        Object pendingResponse = this.pendingResponse;
        if (pendingResponse == VOID) {
            // no pendingResponse has been set, so we are done since the invocation on the primary needs to complete first
            return;
        }

        // if a pendingResponse is set, then the backupsAcksExpected has been set (so we can now safely read backupsAcksExpected)
        int backupAcksExpected = this.backupsAcksExpected;

        if (backupAcksExpected != newBackupAcksCompleted) {
            // we managed to complete a backup, but we were not the one completing the last backup, so we are done
            return;
        }

        // we are the lucky one since we just managed to complete the last backup for this invocation and since the
        // pendingResponse is set, we can set it on the future
        completeWithPendingResponse();
    }

    /**
     * @param response return true if invocation completed
     */
    protected void notifyResponse(Object response, int expectedBackups) {
        // if a regular response comes and there are backups, we need to wait for the backups
        // when the backups complete, the response will be send by the last backup or backup-timeout-handle mechanism kicks on

        if (expectedBackups > backupsAcksReceived) {
            // so the invocation has backups and since not all backups have completed, we need to wait
            // (it could be that backups arrive earlier than the response)

            this.pendingResponseReceivedMillis = Clock.currentTimeMillis();

            this.backupsAcksExpected = expectedBackups;

            // it is very important that the response is set after the backupsAcksExpected is set, else the system
            // can assume the invocation is complete because there is a response and no backups need to respond
            this.pendingResponse = response;

            if (backupsAcksReceived != expectedBackups) {
                // we are done since not all backups have completed. Therefore we should not notify the future
                return;
            }
        }

        // we are going to notify the future that a response is available; this can happen when:
        // - we had a regular operation (so no backups we need to wait for) that completed
        // - we had a backup-aware operation that has completed, but also all its backups have completed
        complete(response);
    }

    /**
     * @param timeoutMillis timeout value to wait for backups after  the response received
     * @return true if invocation is completed
     */
    public boolean detectAndHandleBackupTimeout(long timeoutMillis) {
        // if the backups have completed, we are done; this also filters out all non backup-aware operations
        // since the backupsAcksExpected will always be equal to the backupsAcksReceived
        if (backupsAcksExpected == backupsAcksReceived) {
            return false;
        }

        // if no response has yet been received, we we are done; we are only going to re-invoke an operation
        // if the response of the primary has been received, but the backups have not replied
        if (pendingResponse == VOID) {
            return false;
        }

        // if this has not yet expired (so has not been in the system for a too long period) we ignore it
        long expirationTime = pendingResponseReceivedMillis + timeoutMillis;
        boolean timeoutReached = expirationTime > 0 && expirationTime < Clock.currentTimeMillis();
        if (!timeoutReached) {
            return false;
        }

        if (shouldFailOnIndeterminateOperationState()) {
            completeExceptionally(new IndeterminateOperationStateException(this + " failed because backup acks missed."));
            return true;
        }

        if (shouldCompleteWithoutBackups()) {
            if (logger.isFineEnabled()) {
                logger.fine("Invocation " + this + " will be completed without backup acks.");
            }
            // the backups have not yet completed, but we are going to release the future anyway if a pendingResponse has been set
            completeWithPendingResponse();
            return true;
        }

        return false;
    }

    private void completeWithPendingResponse() {
        if (pendingResponse instanceof AbstractInvocationFuture.ExceptionalResult) {
            completeExceptionally(((AbstractInvocationFuture.ExceptionalResult) pendingResponse).getCause());
        } else {
            complete(pendingResponse);
        }
    }

    protected abstract boolean shouldCompleteWithoutBackups();

    protected abstract void complete(Object value);

    protected abstract void completeExceptionally(Throwable t);

    protected abstract boolean shouldFailOnIndeterminateOperationState();
}
