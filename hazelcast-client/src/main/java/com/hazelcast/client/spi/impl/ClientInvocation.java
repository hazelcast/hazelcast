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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.sequence.CallIdSequence;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.Clock.currentTimeMillis;
import static com.hazelcast.util.StringUtil.timeToString;

/**
 * Handles the routing of a request from a Hazelcast client.
 * <p>
 * 1) Where should request be sent?<br>
 * 2) Should it be retried?<br>
 * 3) How many times is it retried?
 */
public class ClientInvocation implements Runnable {

    private static final int MAX_FAST_INVOCATION_COUNT = 5;
    private static final int UNASSIGNED_PARTITION = -1;
    private static final AtomicLongFieldUpdater<ClientInvocation> INVOKE_COUNT
            = AtomicLongFieldUpdater.newUpdater(ClientInvocation.class, "invokeCount");

    private final ClientInvocationFuture clientInvocationFuture;
    private final ILogger logger;
    private final LifecycleService lifecycleService;
    private final ClientClusterService clientClusterService;
    private final AbstractClientInvocationService invocationService;
    private final ClientExecutionService executionService;
    private final ClientMessage clientMessage;
    private final CallIdSequence callIdSequence;
    private final Address address;
    private final int partitionId;
    private final Connection connection;
    private final long startTimeMillis;
    private final long retryPauseMillis;
    private final String objectName;
    private volatile ClientConnection sendConnection;
    private boolean bypassHeartbeatCheck;
    private EventHandler handler;
    private volatile long invokeCount;

    protected ClientInvocation(HazelcastClientInstanceImpl client,
                               ClientMessage clientMessage,
                               String objectName,
                               int partitionId,
                               Address address,
                               Connection connection) {
        this.clientClusterService = client.getClientClusterService();
        this.lifecycleService = client.getLifecycleService();
        this.invocationService = (AbstractClientInvocationService) client.getInvocationService();
        this.executionService = client.getClientExecutionService();
        this.objectName = objectName;
        this.clientMessage = clientMessage;
        this.partitionId = partitionId;
        this.address = address;
        this.connection = connection;
        this.startTimeMillis = System.currentTimeMillis();
        this.retryPauseMillis = invocationService.getInvocationRetryPauseMillis();
        this.logger = invocationService.invocationLogger;
        this.callIdSequence = client.getCallIdSequence();
        this.clientInvocationFuture = new ClientInvocationFuture(this, executionService,
                clientMessage, logger, callIdSequence);
    }

    /**
     * Create an invocation that will be executed on random member.
     */
    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage, String objectName) {
        this(client, clientMessage, objectName, UNASSIGNED_PARTITION, null, null);
    }

    /**
     * Create an invocation that will be executed on owner of {@code partitionId}.
     */
    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage, String objectName,
                            int partitionId) {
        this(client, clientMessage, objectName, partitionId, null, null);
    }

    /**
     * Create an invocation that will be executed on member with given {@code address}.
     */
    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage, String objectName,
                            Address address) {
        this(client, clientMessage, objectName, UNASSIGNED_PARTITION, address, null);
    }

    /**
     * Create an invocation that will be executed on given {@code connection}.
     */
    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage, String objectName,
                            Connection connection) {
        this(client, clientMessage, objectName, UNASSIGNED_PARTITION, null, connection);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public ClientMessage getClientMessage() {
        return clientMessage;
    }

    public ClientInvocationFuture invoke() {
        assert (clientMessage != null);
        clientMessage.setCorrelationId(callIdSequence.next());
        invokeOnSelection();
        return clientInvocationFuture;
    }

    public ClientInvocationFuture invokeUrgent() {
        assert (clientMessage != null);
        clientMessage.setCorrelationId(callIdSequence.forceNext());
        invokeOnSelection();
        return clientInvocationFuture;
    }

    private void invokeOnSelection() {
        INVOKE_COUNT.incrementAndGet(this);
        try {
            if (isBindToSingleConnection()) {
                invocationService.invokeOnConnection(this, (ClientConnection) connection);
            } else if (partitionId != -1) {
                invocationService.invokeOnPartitionOwner(this, partitionId);
            } else if (address != null) {
                invocationService.invokeOnTarget(this, address);
            } else {
                invocationService.invokeOnRandomTarget(this);
            }
        } catch (Exception e) {
            if (e instanceof HazelcastOverloadException) {
                throw (HazelcastOverloadException) e;
            }
            notifyException(e);
        }
    }

    @Override
    public void run() {
        retry();
    }

    private void retry() {
        // first we force a new invocation slot because we are going to return our old invocation slot immediately after
        // It is important that we first 'force' taking a new slot; otherwise it could be that a sneaky invocation gets
        // through that takes our slot!
        clientMessage.setCorrelationId(callIdSequence.forceNext());
        //we release the old slot
        callIdSequence.complete();

        try {
            invokeOnSelection();
        } catch (Throwable e) {
            clientInvocationFuture.complete(e);
        }
    }

    public void notify(ClientMessage clientMessage) {
        if (clientMessage == null) {
            throw new IllegalArgumentException("response can't be null");
        }
        clientInvocationFuture.complete(clientMessage);
    }

    public void notifyException(Throwable exception) {
        logException(exception);

        if (!lifecycleService.isRunning()) {
            clientInvocationFuture.complete(new HazelcastClientNotActiveException(exception.getMessage(), exception));
            return;
        }

        if (isNotAllowedToRetryOnSelection(exception)) {
            clientInvocationFuture.complete(exception);
            return;
        }

        boolean retry = isRetrySafeException(exception)
                || invocationService.isRedoOperation()
                || (exception instanceof TargetDisconnectedException && clientMessage.isRetryable());
        if (!retry) {
            clientInvocationFuture.complete(exception);
            return;
        }

        long timePassed = System.currentTimeMillis() - startTimeMillis;
        if (timePassed > invocationService.getInvocationTimeoutMillis()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Exception will not be retried because invocation timed out", exception);
            }

            clientInvocationFuture.complete(newOperationTimeoutException(exception));
            return;
        }

        try {
            execute();
        } catch (RejectedExecutionException e) {
            clientInvocationFuture.complete(exception);
        }

    }

    private void logException(Throwable exception) {
        if (logger.isFinestEnabled()) {
            logger.finest("Invocation got an exception " + this
                    + ", invoke count : " + invokeCount
                    + ", exception : " + exception.getClass()
                    + ", message : " + exception.getMessage()
                    + (exception.getCause() != null ? (", cause :" + exception.getCause()) : ""));
        }
    }

    private void execute() {
        if (invokeCount < MAX_FAST_INVOCATION_COUNT) {
            // fast retry for the first few invocations
            executionService.execute(this);
        } else {
            // progressive retry delay
            long delayMillis = Math.min(1 << (invokeCount - MAX_FAST_INVOCATION_COUNT), retryPauseMillis);
            executionService.schedule(this, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    private boolean isNotAllowedToRetryOnSelection(Throwable exception) {
        if (isBindToSingleConnection() && exception instanceof IOException) {
            return true;
        }

        if (address != null
                && exception instanceof TargetNotMemberException
                && clientClusterService.getMember(address) == null) {
            //when invocation send over address
            //if exception is target not member and
            //address is not available in member list , don't retry
            return true;
        }
        return false;
    }

    private boolean isBindToSingleConnection() {
        return connection != null;
    }

    public EventHandler getEventHandler() {
        return handler;
    }

    public void setEventHandler(EventHandler handler) {
        this.handler = handler;
    }

    public boolean shouldBypassHeartbeatCheck() {
        return bypassHeartbeatCheck;
    }

    public void setBypassHeartbeatCheck(boolean bypassHeartbeatCheck) {
        this.bypassHeartbeatCheck = bypassHeartbeatCheck;
    }

    public void setSendConnection(ClientConnection connection) {
        this.sendConnection = connection;
    }

    public ClientConnection getSendConnectionOrWait() throws InterruptedException {
        while (sendConnection == null && !clientInvocationFuture.isDone()) {
            Thread.sleep(retryPauseMillis);
        }
        return sendConnection;
    }

    public ClientConnection getSendConnection() {
        return sendConnection;
    }

    public static boolean isRetrySafeException(Throwable t) {
        return t instanceof IOException
                || t instanceof HazelcastInstanceNotActiveException
                || t instanceof RetryableException;
    }

    public Executor getUserExecutor() {
        return executionService.getUserExecutor();
    }

    private Object newOperationTimeoutException(Throwable e) {
        StringBuilder sb = new StringBuilder();
        sb.append(this);
        sb.append(" timed out because exception occurred after client invocation timeout ");
        sb.append(invocationService.getInvocationTimeoutMillis()).append(" ms. ");
        sb.append("Current time: ").append(timeToString(currentTimeMillis())).append(". ");
        sb.append("Start time: ").append(timeToString(startTimeMillis)).append(". ");
        sb.append("Total elapsed time: ").append(currentTimeMillis() - startTimeMillis).append(" ms. ");
        String msg = sb.toString();
        return new OperationTimeoutException(msg, e);
    }


    @Override
    public String toString() {
        String target;
        if (isBindToSingleConnection()) {
            target = "connection " + connection;
        } else if (partitionId != -1) {
            target = "partition " + partitionId;
        } else if (address != null) {
            target = "address " + address;
        } else {
            target = "random";
        }
        return "ClientInvocation{"
                + "clientMessage = " + clientMessage
                + ", objectName = " + objectName
                + ", target = " + target
                + ", sendConnection = " + sendConnection + '}';
    }
}
