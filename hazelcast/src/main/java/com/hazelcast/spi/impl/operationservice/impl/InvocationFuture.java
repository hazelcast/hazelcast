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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.AbstractInvocationFuture;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.CALL_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.HEARTBEAT_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.INTERRUPTED;
import static com.hazelcast.util.Clock.currentTimeMillis;
import static com.hazelcast.util.ExceptionUtil.fixAsyncStackTrace;
import static com.hazelcast.util.StringUtil.timeToString;

/**
 * The InvocationFuture is the {@link com.hazelcast.spi.InternalCompletableFuture} that waits on the completion
 * of an {@link Invocation}. The Invocation executes an operation.
 * <p>
 * In the past the InvocationFuture.get logic was also responsible for detecting the heartbeat for blocking operations
 * using the CONTINUE_WAIT and detecting if an operation is still running using the IsStillRunning functionality. This
 * has been removed from the future and moved into the {@link InvocationMonitor}.
 *
 * @param <E>
 */
final class InvocationFuture<E> extends AbstractInvocationFuture<E> {

    volatile boolean interrupted;
    final Invocation invocation;
    private final boolean deserialize;

    InvocationFuture(Invocation invocation, boolean deserialize) {
        super(invocation.context.asyncExecutor, invocation.context.logger);
        this.invocation = invocation;
        this.deserialize = deserialize;
    }

    @Override
    protected String invocationToString() {
        return invocation.toString();
    }

    @Override
    protected TimeoutException newTimeoutException(long timeout, TimeUnit unit) {
        return new TimeoutException(String.format("%s failed to complete within %d %s. %s",
                invocation.op.getClass().getSimpleName(), timeout, unit, invocation));
    }

    @Override
    protected void onInterruptDetected() {
        interrupted = true;
    }

    @Override
    protected E resolveAndThrowIfException(Object unresolved) throws ExecutionException, InterruptedException {
        Object value = resolve(unresolved);

        if (value == null || !(value instanceof Throwable)) {
            return (E) value;
        } else if (value instanceof CancellationException) {
            throw (CancellationException) value;
        } else if (value instanceof ExecutionException) {
            throw (ExecutionException) value;
        } else if (value instanceof InterruptedException) {
            throw (InterruptedException) value;
        } else if (value instanceof Error) {
            throw (Error) value;
        } else {
            throw new ExecutionException((Throwable) value);
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    @Override
    protected Object resolve(Object unresolved) {
        if (unresolved == null) {
            return null;
        } else if (unresolved == INTERRUPTED) {
            return new InterruptedException(invocation.op.getClass().getSimpleName() + " was interrupted. " + invocation);
        } else if (unresolved == CALL_TIMEOUT) {
            return newOperationTimeoutException(false);
        } else if (unresolved == HEARTBEAT_TIMEOUT) {
            return newOperationTimeoutException(true);
        } else if (unresolved.getClass() == Packet.class) {
            NormalResponse response = invocation.context.serializationService.toObject(unresolved);
            unresolved = response.getValue();
        }

        Object value = unresolved;
        if (deserialize && value instanceof Data) {
            value = invocation.context.serializationService.toObject(value);
            if (value == null) {
                return null;
            }
        }

        if (invocation.shouldFailOnIndeterminateOperationState() && (value instanceof MemberLeftException)) {
            String message = invocation + " failed because the target has left the cluster before response is received";
            value = new IndeterminateOperationStateException(message, (MemberLeftException) value);
        }

        if (value instanceof Throwable) {
            Throwable throwable = ((Throwable) value);
            fixAsyncStackTrace((Throwable) value, Thread.currentThread().getStackTrace());
            return throwable;
        }

        return value;
    }

    private Object newOperationTimeoutException(boolean heartbeatTimeout) {
        StringBuilder sb = new StringBuilder();
        if (heartbeatTimeout) {
            sb.append(invocation.op.getClass().getSimpleName())
                    .append(" invocation failed to complete due to operation-heartbeat-timeout. ");
            sb.append("Current time: ").append(timeToString(currentTimeMillis())).append(". ");
            sb.append("Start time: ").append(timeToString(invocation.firstInvocationTimeMillis)).append(". ");
            sb.append("Total elapsed time: ")
                    .append(currentTimeMillis() - invocation.firstInvocationTimeMillis).append(" ms. ");

            long lastHeartbeatMillis = invocation.lastHeartbeatMillis;
            sb.append("Last operation heartbeat: ");
            appendHeartbeat(sb, lastHeartbeatMillis);

            long lastHeartbeatFromMemberMillis = invocation.context.invocationMonitor
                    .getLastMemberHeartbeatMillis(invocation.invTarget);
            sb.append("Last operation heartbeat from member: ");
            appendHeartbeat(sb, lastHeartbeatFromMemberMillis);
        } else {
            sb.append(invocation.op.getClass().getSimpleName())
                    .append(" got rejected before execution due to not starting within the operation-call-timeout of: ")
                    .append(invocation.callTimeoutMillis).append(" ms. ");
            sb.append("Current time: ").append(timeToString(currentTimeMillis())).append(". ");
            sb.append("Start time: ").append(timeToString(invocation.firstInvocationTimeMillis)).append(". ");
            sb.append("Total elapsed time: ")
                    .append(currentTimeMillis() - invocation.firstInvocationTimeMillis).append(" ms. ");
        }

        sb.append(invocation);
        String msg = sb.toString();
        return new ExecutionException(msg, new OperationTimeoutException(msg));
    }

    private static void appendHeartbeat(StringBuilder sb, long lastHeartbeatMillis) {
        if (lastHeartbeatMillis == 0) {
            sb.append("never. ");
        } else {
            sb.append(timeToString(lastHeartbeatMillis)).append(". ");
        }
    }
}
