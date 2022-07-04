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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IndeterminateOperationState;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.AbstractInvocationFuture;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.WrappableException;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.internal.util.Clock.currentTimeMillis;
import static com.hazelcast.internal.util.ExceptionUtil.cloneExceptionWithFixedAsyncStackTrace;
import static com.hazelcast.internal.util.StringUtil.timeToString;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.CALL_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.HEARTBEAT_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.INTERRUPTED;

/**
 * The InvocationFuture is the {@link InternalCompletableFuture} that waits on the completion
 * of an {@link Invocation}. The Invocation executes an operation.
 * <p>
 * In the past the InvocationFuture.get logic was also responsible for detecting the heartbeat for blocking operations
 * using the CONTINUE_WAIT and detecting if an operation is still running using the IsStillRunning functionality. This
 * has been removed from the future and moved into the {@link InvocationMonitor}.
 *
 * @param <E>
 */
public final class InvocationFuture<E> extends AbstractInvocationFuture<E> {

    final Invocation invocation;
    volatile boolean interrupted;
    private final boolean deserialize;

    InvocationFuture(Invocation invocation, boolean deserialize) {
        super(invocation.context.logger);
        this.invocation = invocation;
        this.deserialize = deserialize;
    }

    @Override
    protected void onInterruptDetected() {
        interrupted = true;
    }

    @Override
    public boolean isCompletedExceptionally() {
        return (state instanceof ExceptionalResult
                || state == CALL_TIMEOUT
                || state == HEARTBEAT_TIMEOUT
                || state == INTERRUPTED);
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
    protected Exception wrapToInstanceNotActiveException(RejectedExecutionException e) {
        if (!invocation.context.nodeEngine.isRunning()) {
            return new HazelcastInstanceNotActiveException(e.getMessage());
        }
        return e;
    }

    @Override
    protected E resolveAndThrowIfException(Object unresolved) throws ExecutionException, InterruptedException {
        Object value = resolve(unresolved);
        return returnOrThrowWithGetConventions(value);
    }

    // public for tests
    public static <T> T returnOrThrowWithGetConventions(Object response) throws ExecutionException, InterruptedException {
        if (!(response instanceof ExceptionalResult)) {
            return (T) response;
        }
        response = ((ExceptionalResult) response).getCause();
        if (response instanceof WrappableException) {
            response = ((WrappableException) response).wrap();
        } else if (response instanceof RuntimeException || response instanceof Error) {
            response = cloneExceptionWithFixedAsyncStackTrace((Throwable) response);
        }
        if (response instanceof CancellationException) {
            throw (CancellationException) response;
        } else if (response instanceof ExecutionException) {
            throw (ExecutionException) response;
        } else if (response instanceof InterruptedException) {
            throw (InterruptedException) response;
        } else {
            throw new ExecutionException((Throwable) response);
        }
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    @Override
    protected Object resolve(Object unresolved) {
        if (unresolved == null) {
            return null;
        } else if (unresolved == INTERRUPTED) {
            return new ExceptionalResult(
                    new InterruptedException(invocation.op.getClass().getSimpleName() + " was interrupted. " + invocation));
        } else if (unresolved == CALL_TIMEOUT) {
            return new ExceptionalResult(newOperationTimeoutException(false));
        } else if (unresolved == HEARTBEAT_TIMEOUT) {
            return new ExceptionalResult(newOperationTimeoutException(true));
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

        Throwable cause = (value instanceof ExceptionalResult)
                ? ((ExceptionalResult) value).getCause()
                : null;

        if (invocation.shouldFailOnIndeterminateOperationState()
                && (value instanceof IndeterminateOperationState
                || cause instanceof IndeterminateOperationState)) {
            value = wrapThrowable(new IndeterminateOperationStateException("indeterminate operation state",
                    cause == null ? (Throwable) value : cause));
        }

        return value;
    }

    private OperationTimeoutException newOperationTimeoutException(boolean heartbeatTimeout) {
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
                    .getLastMemberHeartbeatMillis(invocation.getTargetAddress());
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
        return new OperationTimeoutException(msg);
    }

    private static void appendHeartbeat(StringBuilder sb, long lastHeartbeatMillis) {
        if (lastHeartbeatMillis == 0) {
            sb.append("never. ");
        } else {
            sb.append(timeToString(lastHeartbeatMillis)).append(". ");
        }
    }

}
