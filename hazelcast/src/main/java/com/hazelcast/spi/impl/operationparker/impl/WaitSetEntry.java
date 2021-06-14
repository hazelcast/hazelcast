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

package com.hazelcast.spi.impl.operationparker.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.internal.util.Clock;

import java.util.Queue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * A simple container for a {@link BlockingOperation} that is added to the {@link WaitSet}.
 *
 * This operation is only executed when the inner BlockingOperation requires invalidation.
 *
 * WaitSetEntry may be put in a delay queue (part of the {@link OperationParkerImpl}) based on its wait timeout.
 *
 * @see Operation#getWaitTimeout()
 * @see #needsInvalidation()
 *
 *
 */
class WaitSetEntry extends AbstractLocalOperation implements Delayed, PartitionAwareOperation,
        IdentifiedDataSerializable, OperationResponseHandler<WaitSetEntry> {
    final Queue<WaitSetEntry> queue;
    final Operation op;
    final BlockingOperation blockingOperation;
    final long expirationTimeMs;
    volatile boolean valid = true;
    volatile Object cancelResponse;

    WaitSetEntry(Queue<WaitSetEntry> queue, BlockingOperation blockingOperation) {
        this.op = (Operation) blockingOperation;
        this.blockingOperation = blockingOperation;
        this.queue = queue;
        this.expirationTimeMs = getExpirationTimeMs(blockingOperation);

        setPartitionId(op.getPartitionId());
    }

    private long getExpirationTimeMs(BlockingOperation blockingOperation) {
        long waitTimeout = blockingOperation.getWaitTimeout();
        if (waitTimeout < 0) {
            return -1;
        }
        long expirationTime = Clock.currentTimeMillis() + waitTimeout;
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

    public boolean isValid() {
        return valid;
    }

    public boolean needsInvalidation() {
        return isExpired() || isCancelled() || isCallTimedOut();
    }

    public boolean isExpired() {
        return expirationTimeMs > 0 && Clock.currentTimeMillis() >= expirationTimeMs;
    }

    public boolean isCancelled() {
        return cancelResponse != null;
    }

    public boolean isCallTimedOut() {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        if (operationService.isCallTimedOut(op)) {
            cancel(new CallTimeoutResponse(op.getCallId(), op.isUrgent()));
            return true;
        }
        return false;
    }

    public boolean shouldWait() {
        return blockingOperation.shouldWait();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(expirationTimeMs - Clock.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        // compare zero ONLY if same object
        if (other == this) {
            return 0;
        }
        long d = (getDelay(TimeUnit.NANOSECONDS)
                - other.getDelay(TimeUnit.NANOSECONDS));
        return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
    }

    @Override
    public void run() throws Exception {
        if (!valid) {
            return;
        }

        boolean expired = isExpired();
        boolean cancelled = isCancelled();
        if (!expired && !cancelled) {
            return;
        }

        if (!queue.remove(this)) {
            return;
        }

        valid = false;
        if (expired) {
            onExpire();
        } else {
            onCancel();
        }
    }

    // if you don't think instances of this class will ever be inserted into a HashMap/HashTable,
    // the recommended hashCode implementation to use is:
    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        return 42;
        // any arbitrary constant will do
    }

    @Override
    // use object.equals
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public void logError(Throwable e) {
        ILogger logger = getLogger();
        if (e instanceof RetryableException) {
            // When WaitSetEntry is still valid then it means the error happened before WaitSetEntry was executed.
            // This can happen for example during partition migration. We log it into the FINE level only, because
            // the operation still remains in the WaitSet and OperationParker will re-execute it eventually.

            // We log into WARNING when the operation is no longer valid. This means WaitSetEntry was already executed
            // and removed from the WaitSet -> It won't be re-executed.
            Level level = isValid() ? Level.FINE : Level.WARNING;
            logger.log(level, "Op: " + op + ", " + e.getClass().getName() + ": " + e.getMessage());
        } else if (e instanceof OutOfMemoryError) {
            try {
                logger.severe(e.getMessage(), e);
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
        blockingOperation.onWaitExpire();
    }

    public void onCancel() {
        OperationResponseHandler responseHandler = op.getOperationResponseHandler();
        responseHandler.sendResponse(op, cancelResponse);
    }

    public void cancel(Object error) {
        this.cancelResponse = error;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", op=").append(op);
        sb.append(", expirationTimeMs=").append(expirationTimeMs);
        sb.append(", valid=").append(valid);
    }

    @Override
    public void sendResponse(WaitSetEntry op, Object response) {
        // WaitSetEntry operation should never send a normal (non-exceptional) response.
        // It's because WaitSetEntry operation is only executed to notify the inner BlockingOperation about
        // expiration/timeout/cancellation. Upon the notification the inner BlockingOperation will use its
        // own logic and its own response handler to react on the event.

        // WaitSetEntry operation may still receive an exceptional response. For example when there is a partition
        // migration in progress. This is not a big deal as the OperationParker will eventually retry it anyway.
        if (response instanceof Throwable) {
            // we log on finest only, as it's meant for debugging only. See #logError for regular
            // user-facing logging on errors.
            getLogger().finest("Error while executing " + this, (Throwable) response);
        } else {
            getLogger().warning("Error while executing " + this + " and the response is NOT Throwable. This is "
                    + "unexpected, please open an issue for this. Response: " + response);
        }
    }
}
