/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.connection.nio.ClusterConnectorService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.ClientExecutionService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.impl.BaseInvocation;
import com.hazelcast.spi.impl.sequence.CallIdSequence;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.Clock.currentTimeMillis;
import static com.hazelcast.internal.util.StringUtil.timeToString;

/**
 * Handles the routing of a request from a Hazelcast client.
 * <p>
 * 1) Where should request be sent?<br>
 * 2) Should it be retried?<br>
 * 3) How many times is it retried?
 */
public class ClientInvocation extends BaseInvocation implements Runnable {
    private static final int MAX_FAST_INVOCATION_COUNT = 5;
    private static final int UNASSIGNED_PARTITION = -1;
    private static final AtomicLongFieldUpdater<ClientInvocation> INVOKE_COUNT
            = AtomicLongFieldUpdater.newUpdater(ClientInvocation.class, "invokeCount");
    final LifecycleService lifecycleService;
    private final ClientInvocationFuture clientInvocationFuture;
    private final ILogger logger;
    private final ClientClusterService clientClusterService;
    private final AbstractClientInvocationService invocationService;
    private final ClientExecutionService executionService;
    private final ClusterConnectorService clusterConnectorService;
    private volatile ClientMessage clientMessage;
    private final CallIdSequence callIdSequence;
    private final Address address;
    private final int partitionId;
    private final Connection connection;
    private final long startTimeMillis;
    private final long retryPauseMillis;
    private final Object objectName;
    private volatile ClientConnection sendConnection;
    private EventHandler handler;
    private volatile long invokeCount;
    private volatile long invocationTimeoutMillis;

    protected ClientInvocation(HazelcastClientInstanceImpl client,
                               ClientMessage clientMessage,
                               Object objectName,
                               int partitionId,
                               Address address,
                               Connection connection) {
        this.clusterConnectorService = client.getClusterConnectorService();
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
        this.callIdSequence = invocationService.getCallIdSequence();
        this.clientInvocationFuture = new ClientInvocationFuture(this, clientMessage, logger, callIdSequence);
        this.invocationTimeoutMillis = invocationService.getInvocationTimeoutMillis();
    }

    /**
     * Create an invocation that will be executed on random member.
     */
    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage, Object objectName) {
        this(client, clientMessage, objectName, UNASSIGNED_PARTITION, null, null);
    }

    /**
     * Create an invocation that will be executed on owner of {@code partitionId}.
     */
    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage, Object objectName,
                            int partitionId) {
        this(client, clientMessage, objectName, partitionId, null, null);
    }

    /**
     * Create an invocation that will be executed on member with given {@code address}.
     */
    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage, Object objectName,
                            Address address) {
        this(client, clientMessage, objectName, UNASSIGNED_PARTITION, address, null);
    }

    /**
     * Create an invocation that will be executed on given {@code connection}.
     */
    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage, Object objectName,
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
        long correlationId = callIdSequence.forceNext();
        // retry modifies the client message and should not reuse the client message.
        // It could be the case that it is in write queue of the connection.
        // through that takes our slot!
        clientMessage = clientMessage.copyWithNewCorrelationId(correlationId);
        //we release the old slot
        callIdSequence.complete();

        try {
            invokeOnSelection();
        } catch (Throwable e) {
            completeExceptionally(e);
        }
    }

    public void setInvocationTimeoutMillis(long invocationTimeoutMillis) {
        this.invocationTimeoutMillis = invocationTimeoutMillis;
    }


    /**
     * @param clientMessage return true if invocation completed
     */
    void notify(ClientMessage clientMessage) {
        assert clientMessage != null : "response can't be null";
        int expectedBackups = clientMessage.getNumberOfBackupAcks();

        notifyResponse(clientMessage, expectedBackups);
    }

    @Override
    protected boolean shouldCompleteWithoutBackups() {
        return true;
    }

    @Override
    protected void complete(Object response) {
        clientInvocationFuture.complete(response);
        invocationService.deRegisterInvocation(clientMessage.getCorrelationId());
    }

    @Override
    protected void completeExceptionally(Throwable t) {
        clientInvocationFuture.completeExceptionally(t);
        invocationService.deRegisterInvocation(clientMessage.getCorrelationId());
    }

    protected boolean shouldFailOnIndeterminateOperationState() {
        return invocationService.shouldFailOnIndeterminateOperationState();
    }

    void notifyException(Throwable exception) {
        logException(exception);

        if (!lifecycleService.isRunning()) {
            completeExceptionally(new HazelcastClientNotActiveException("Client is shutting down", exception));
            return;
        }

        if (shouldThrowOfflineException(exception)) {
            clientInvocationFuture.complete(new HazelcastClientOfflineException("Client is offline"));
            return;
        }

        if (isNotAllowedToRetryOnSelection(exception)) {
            completeExceptionally(exception);
            return;
        }

        if (!shouldRetry(exception)) {
            completeExceptionally(exception);
            return;
        }

        long timePassed = System.currentTimeMillis() - startTimeMillis;
        if (timePassed > invocationTimeoutMillis) {
            if (logger.isFinestEnabled()) {
                logger.finest("Exception will not be retried because invocation timed out", exception);
            }

            StringBuilder sb = new StringBuilder();
            sb.append(this);
            sb.append(" timed out because exception occurred after client invocation timeout ");
            sb.append(invocationService.getInvocationTimeoutMillis()).append(" ms. ");
            sb.append("Current time: ").append(timeToString(currentTimeMillis())).append(". ");
            sb.append("Start time: ").append(timeToString(startTimeMillis)).append(". ");
            sb.append("Total elapsed time: ").append(currentTimeMillis() - startTimeMillis).append(" ms. ");
            String msg = sb.toString();
            completeExceptionally(new OperationTimeoutException(msg, exception));
            return;
        }

        try {
            execute();
        } catch (RejectedExecutionException e) {
            completeExceptionally(new HazelcastClientNotActiveException("Client is shutting down", exception));
        }

    }

    private boolean shouldThrowOfflineException(Throwable exception) {
        return ClientConnectionStrategyConfig.ReconnectMode.ASYNC.equals(invocationService.getReconnectMode())
                && exception instanceof IOException
                && !clusterConnectorService.mainConnectionExists();
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
        invocationService.deRegisterInvocation(clientMessage.getCorrelationId());
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
        if (isBindToSingleConnection()
                && (exception instanceof IOException || exception instanceof TargetDisconnectedException)) {
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

    EventHandler getEventHandler() {
        return handler;
    }

    public void setEventHandler(EventHandler handler) {
        this.handler = handler;
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

    private boolean shouldRetry(Throwable t) {
        if (isRetrySafeException(t)) {
            return true;
        }
        if (t instanceof TargetDisconnectedException) {
            return clientMessage.isRetryable() || invocationService.isRedoOperation();
        }
        return false;
    }

    public static boolean isRetrySafeException(Throwable t) {
        return t instanceof IOException
                || t instanceof HazelcastInstanceNotActiveException
                || t instanceof RetryableException;
    }

    public Executor getUserExecutor() {
        return executionService.getUserExecutor();
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

    // package private methods for tests
    CallIdSequence getCallIdSequence() {
        return callIdSequence;
    }

    ClientInvocationFuture getClientInvocationFuture() {
        return clientInvocationFuture;
    }
}
