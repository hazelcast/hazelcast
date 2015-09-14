/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.waitnotifyservice.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;

import java.util.Queue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.util.Clock.currentTimeMillis;
import static com.hazelcast.util.EmptyStatement.ignore;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class WaitingOperation extends AbstractOperation implements Delayed, PartitionAwareOperation {
    final Queue<WaitingOperation> queue;
    final Operation op;
    final WaitSupport waitSupport;
    final long expirationTime;
    volatile boolean valid = true;
    volatile Object cancelResponse;
    volatile boolean interrupted;

    WaitingOperation(Queue<WaitingOperation> queue, WaitSupport waitSupport) {
        this.op = (Operation) waitSupport;
        this.waitSupport = waitSupport;
        this.queue = queue;
        this.expirationTime = getExpirationTime(waitSupport);
        this.setPartitionId(op.getPartitionId());
    }

    private long getExpirationTime(WaitSupport waitSupport) {
        long waitTimeout = waitSupport.getWaitTimeout();
        if (waitTimeout < 0) {
            return -1;
        }
        long expirationTime = currentTimeMillis() + waitTimeout;
        if (expirationTime < 0) {
            return -1;
        }
        return expirationTime;
    }

    public Operation getOperation() {
        return op;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public void setInterrupted() {
        interrupted = true;
    }

    public boolean isValid() {
        return valid;
    }

    public boolean needsInvalidation() {
        return isExpired() || isCancelled() || isCallTimedOut();
    }

    public boolean isExpired() {
        return expirationTime > 0 && currentTimeMillis() >= expirationTime;
    }

    public boolean isCancelled() {
        return cancelResponse != null;
    }

    public boolean isCallTimedOut() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalOperationService operationService = nodeEngine.getOperationService();
        if (operationService.isCallTimedOut(op)) {
            cancel(new CallTimeoutResponse(op.getCallId(), op.isUrgent()));
            return true;
        }
        return false;
    }

    public boolean shouldWait() {
        return waitSupport.shouldWait();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(expirationTime - currentTimeMillis(), MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        // compare zero ONLY if same object
        if (other == this) {
            return 0;
        }

        long delay = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
        if (delay == 0) {
            return 0;
        } else {
            return (delay < 0) ? -1 : 1;
        }
    }

    @Override
    public void run() throws Exception {
        System.out.println(Thread.currentThread().getName());

        if (!valid) {
            return;
        }

        boolean expired = isExpired();
        boolean cancelled = isCancelled();
        if (!expired && !cancelled) {

            //todo: we forget the remove the operation from the pending queue
            if (interrupted) {
                valid = false;
                op.onInterrupt();
            }
            return;
        }

        if (!queue.remove(this)) {
            return;
        }

        valid = false;
        if (expired) {
            waitSupport.onWaitExpire();
        } else {
            op.sendResponse(cancelResponse);
        }
    }

    //If you don't think instances of this class will ever be inserted into a HashMap/HashTable,
    // the recommended hashCode implementation to use is:
    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        // any arbitrary constant will do
        return 42;
    }

    @Override
    // use object.equals
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public void logError(Throwable e) {
        final ILogger logger = getLogger();
        if (e instanceof RetryableException) {
            logger.warning("Op: " + op + ", " + e.getClass().getName() + ": " + e.getMessage());
        } else if (e instanceof OutOfMemoryError) {
            try {
                logger.log(Level.SEVERE, e.getMessage(), e);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        } else {
            logger.severe("Op: " + op + ", Error: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return op.getServiceName();
    }

    public void onExpire() {
        waitSupport.onWaitExpire();
    }

    public void cancel(Object error) {
        this.cancelResponse = error;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", op=").append(op);
        sb.append(", expirationTime=").append(expirationTime);
        sb.append(", valid=").append(valid);
    }
}
