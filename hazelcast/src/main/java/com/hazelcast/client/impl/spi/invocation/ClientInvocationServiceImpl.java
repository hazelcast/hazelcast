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

package com.hazelcast.client.impl.spi.invocation;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.ClientInvocationService;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.impl.sequence.CallIdSequence;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import static com.hazelcast.client.impl.protocol.codec.builtin.ErrorsCodec.EXCEPTION_MESSAGE_TYPE;
import static com.hazelcast.client.properties.ClientProperty.FAIL_ON_INDETERMINATE_OPERATION_STATE;
import static com.hazelcast.client.properties.ClientProperty.INVOCATION_RETRY_PAUSE_MILLIS;
import static com.hazelcast.client.properties.ClientProperty.INVOCATION_TIMEOUT_SECONDS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_METRIC_INVOCATIONS_MAX_CURRENT_INVOCATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_METRIC_INVOCATIONS_PENDING_CALLS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_METRIC_INVOCATIONS_STARTED_INVOCATIONS;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.Clock.currentTimeMillis;
import static com.hazelcast.internal.util.StringUtil.timeToString;

/**
 * Invocation service for Hazelcast clients.
 * <p>
 * Allows remote invocations on different targets like {@link ClientConnection},
 * partition owners, {@link Member} based or random targets.
 */
public final class ClientInvocationServiceImpl implements ClientInvocationService {

    private static final int MAX_FAST_INVOCATION_COUNT = 5;
    final CallIdSequence callIdSequence;
    final boolean shouldFailOnIndeterminateOperationState;
    @Probe(name = CLIENT_METRIC_INVOCATIONS_PENDING_CALLS, level = MANDATORY)
    final ConcurrentMap<Long, ClientInvocation> invocations = new ConcurrentHashMap<>();
    private final Map<Long, ClientInvocation> unmodifiableInvocations;
    private final boolean isBackupAckToClientEnabled;
    private final ILogger logger;
    private final long globalInvocationTimeoutMillis;
    private final long invocationRetryPauseMillis;
    private final boolean isSmartRoutingEnabled;
    private final boolean redoOperation;
    private final ClientConnectionManager connectionManager;
    private final ClientPartitionService partitionService;
    private final BooleanSupplier isInstanceRunning;
    private final TaskScheduler taskScheduler;
    private final ClientExceptionFactory exceptionFactory;

    @SuppressWarnings("checkstyle:parameternumber")
    public ClientInvocationServiceImpl(ILogger logger, HazelcastProperties properties, CallIdSequence callIdSequence,
                                       boolean isSmartRoutingEnabled, boolean isBackupAckToClientEnabled,
                                       boolean redoOperation,
                                       ClientConnectionManager connectionManager,
                                       ClientPartitionService partitionService,
                                       BooleanSupplier isInstanceRunning,
                                       TaskScheduler taskScheduler,
                                       ClientExceptionFactory exceptionFactory) {
        this.logger = logger;
        this.globalInvocationTimeoutMillis = properties.getPositiveMillisOrDefault(INVOCATION_TIMEOUT_SECONDS);
        this.invocationRetryPauseMillis = properties.getPositiveMillisOrDefault(INVOCATION_RETRY_PAUSE_MILLIS);
        this.callIdSequence = callIdSequence;
        this.shouldFailOnIndeterminateOperationState = properties.getBoolean(FAIL_ON_INDETERMINATE_OPERATION_STATE);
        this.isSmartRoutingEnabled = isSmartRoutingEnabled;
        this.isBackupAckToClientEnabled = isBackupAckToClientEnabled;
        this.redoOperation = redoOperation;
        this.connectionManager = connectionManager;
        this.partitionService = partitionService;
        this.isInstanceRunning = isInstanceRunning;
        this.taskScheduler = taskScheduler;
        this.exceptionFactory = exceptionFactory;
        this.unmodifiableInvocations = Collections.unmodifiableMap(invocations);
    }

    @Probe(name = CLIENT_METRIC_INVOCATIONS_STARTED_INVOCATIONS, level = MANDATORY)
    private long startedInvocations() {
        return callIdSequence.getLastCallId();
    }

    @Probe(name = CLIENT_METRIC_INVOCATIONS_MAX_CURRENT_INVOCATIONS, level = MANDATORY)
    private long maxCurrentInvocations() {
        return callIdSequence.getMaxConcurrentInvocations();
    }

    void deRegisterInvocation(long callId) {
        invocations.remove(callId);
    }

    public Map<Long, ClientInvocation> getUnmodifiableInvocations() {
        return unmodifiableInvocations;
    }

    public boolean isBackupAckToClientEnabled() {
        return isBackupAckToClientEnabled;
    }

    /**
     * Urgent messages, unlike normal messages, can go through
     * 1. even if max allowed invocation count is reached
     * 2. and in client disconnected state
     *
     * @return future
     */
    @Override
    public ClientInvocationFuture invokeOnPartition(ClientMessage clientMessage,
                                                    Object objectName, int partitionId,
                                                    boolean urgent,
                                                    boolean allowRetryOnRandom,
                                                    long invocationTimeoutMillis) {
        clientMessage.setPartitionId(partitionId);
        return invokeOnSelection(clientMessage, null, objectName, urgent, allowRetryOnRandom, invocationTimeoutMillis,
                e -> false, partitionId, null, null);
    }

    @Override
    public ClientInvocationFuture invokeOnConnection(ClientMessage clientMessage, EventHandler handler,
                                                     Object objectName, ClientConnection connection,
                                                     boolean urgent, boolean allowRetryOnRandom,
                                                     long invocationTimeoutMillis) {
        return invokeOnSelection(clientMessage, handler, objectName, urgent, allowRetryOnRandom, invocationTimeoutMillis,
                t -> (t instanceof IOException || t instanceof TargetDisconnectedException), -1, null, connection);
    }


    @Override
    public ClientInvocationFuture invokeOnRandom(ClientMessage clientMessage,
                                                 Object objectName,
                                                 boolean urgent,
                                                 boolean allowRetryOnRandom,
                                                 long invocationTimeoutMillis) {
        return invokeOnSelection(clientMessage, null, objectName, urgent, allowRetryOnRandom, invocationTimeoutMillis,
                e -> false, -1, null, null);
    }

    @Override
    public ClientInvocationFuture invokeOnMember(ClientMessage clientMessage,
                                                 Object objectName, UUID memberUUid,
                                                 boolean urgent,
                                                 boolean allowRetryOnRandom,
                                                 long invocationTimeoutMillis) {

        return invokeOnSelection(clientMessage, null, objectName, urgent, allowRetryOnRandom, invocationTimeoutMillis,
                t -> t instanceof TargetNotMemberException,
                -1, memberUUid, null);
    }

    @Override
    public void handleResponseMessage(ClientMessage message) {

        long correlationId = message.getCorrelationId();

        ClientInvocation invocation = invocations.get(correlationId);
        if (invocation == null) {
            logger.warning("No call for callId: " + correlationId + ", response: " + message);
            return;
        }

        if (EXCEPTION_MESSAGE_TYPE == message.getMessageType()) {
            invocation.notifyException(correlationId, exceptionFactory.createException(message));
        } else {
            invocation.notify(message);
        }
    }

    //TODO sancar introduce invocatinConfigs with static predefined configs and pass invocatoinConfig class here
    @SuppressWarnings("checkstyle:parameternumber")
    private ClientInvocationFuture invokeOnSelection(ClientMessage clientMessage,
                                                     EventHandler handler,
                                                     Object objectName,
                                                     boolean urgent,
                                                     boolean allowRetryOnRandom,
                                                     long invocationTimeoutMillis,
                                                     Predicate<Throwable> retryDisallowedFunc,
                                                     int partitionID, UUID uuid, ClientConnection connection) {
        clientMessage.setCorrelationId(callIdSequence.next());
        ClientInvocationFuture future = new ClientInvocationFuture(isInstanceRunning, clientMessage, logger,
                callIdSequence, invocationRetryPauseMillis);
        if (invocationTimeoutMillis == USE_GLOBAL_INVOCATION_TIMEOUT_MILLIS) {
            invocationTimeoutMillis = globalInvocationTimeoutMillis;
        }
        ClientInvocation invocation = new ClientInvocation(future, this, clientMessage, handler, objectName,
                retryDisallowedFunc, invocationTimeoutMillis, urgent, allowRetryOnRandom, partitionID, uuid, connection);
        return invokeOnSelection(invocation, future);
    }

    /**
     * Retry is guaranteed to be run via single thread at a time per invocation.
     * {@link ClientInvocation#getPermissionToNotifyForDeadConnection}
     * {@link ClientInvocation#getPermissionToNotify(long)}}
     */
    void retryOnSelection(ClientInvocation invocation, ClientInvocationFuture future) {
        // first we force a new invocation slot because we are going to return our old invocation slot immediately after
        // It is important that we first 'force' taking a new slot; otherwise it could be that a sneaky invocation gets
        long correlationId = callIdSequence.forceNext();
        // retry modifies the client message and should not reuse the client message.
        // It could be the case that it is in write queue of the connection.
        // through that takes our slot!
        invocation.clientMessage = invocation.clientMessage.copyWithNewCorrelationId(correlationId);
        //we release the old slot
        callIdSequence.complete();
        invokeOnSelection(invocation, future);
    }

    ClientInvocationFuture invokeOnSelection(ClientInvocation invocation, ClientInvocationFuture future) {
        try {
            invocation.incrementInvokeCount();

            if (!invocation.urgent) {
                connectionManager.checkInvocationAllowed();
            }

            boolean invoked;
            if (isSmartRoutingEnabled) {
                if (invocation.partitionId != -1) {
                    invoked = sendToPartitionOwner(invocation, invocation.partitionId, future);
                } else if (invocation.uuid != null) {
                    invoked = sendToMember(invocation, invocation.uuid, future);
                } else if (invocation.connection != null) {
                    invoked = send(invocation, future, invocation.connection);
                } else {
                    invoked = sendToRandom(invocation, future);
                }
                if (invocation.allowRetryOnRandom && !invoked) {
                    invoked = sendToRandom(invocation, future);
                }
            } else {
                if (invocation.connection != null) {
                    invoked = send(invocation, future, invocation.connection);
                } else {
                    invoked = sendToRandom(invocation, future);
                }
            }
            if (!invoked) {
                handleException(
                        invocation, new IOException("No connection found to invoke"), invocation.clientMessage, future);
            }

        } catch (Throwable e) {
            handleException(invocation, e, invocation.clientMessage, future);
        }
        return future;
    }

    /**
     * We make sure that this method can not be called from multiple threads per invocation at the same time
     * Only ones that can set a null on `sentConnection` can notify the invocation
     */
    void handleException(ClientInvocation invocation,
                         Throwable exception,
                         ClientMessage clientMessage, ClientInvocationFuture future) {
        logException(logger, exception, invocation);

        if (!isInstanceRunning.getAsBoolean()) {
            invocation.completeExceptionally(new HazelcastClientNotActiveException("Client is shutting down", exception));
            return;
        }

        if (!shouldRetry(clientMessage.isRetryable(), redoOperation, exception, invocation.retryDisallowedFunc)) {
            invocation.completeExceptionally(exception);
            return;
        }

        long timePassed = System.currentTimeMillis() - invocation.startTimeMillis;
        if (timePassed > invocation.invocationTimeoutMillis) {
            if (logger.isFinestEnabled()) {
                logger.finest("Exception will not be retried because invocation timed out", exception);
            }

            StringBuilder sb = new StringBuilder();
            sb.append(invocation);
            sb.append(" timed out because exception occurred after client invocation timeout ");
            sb.append(invocation.invocationTimeoutMillis).append(" ms. ");
            sb.append("Current time: ").append(timeToString(currentTimeMillis())).append(". ");
            sb.append("Start time: ").append(timeToString(invocation.startTimeMillis)).append(". ");
            sb.append("Total elapsed time: ").append(currentTimeMillis() - invocation.startTimeMillis).append(" ms. ");
            String msg = sb.toString();
            invocation.completeExceptionally(new OperationTimeoutException(msg, exception));
            return;
        }

        try {
            deRegisterInvocation(clientMessage.getCorrelationId());
            if (invocation.getInvokeCount() < MAX_FAST_INVOCATION_COUNT) {
                // fast retry for the first few invocations
                taskScheduler.execute(() -> retryOnSelection(invocation, future));
            } else {
                // progressive retry delay
                long delayMillis = Math.min(1 << (invocation.getInvokeCount() - MAX_FAST_INVOCATION_COUNT),
                        invocationRetryPauseMillis);
                taskScheduler.schedule(() -> retryOnSelection(invocation, future), delayMillis, TimeUnit.MILLISECONDS);
            }
        } catch (RejectedExecutionException e) {
            invocation.completeExceptionally(new HazelcastClientNotActiveException("Client is shutting down", exception));
        }
    }

    private boolean shouldRetry(boolean retryableMessage, boolean redoOperation,
                                Throwable t, Predicate<Throwable> retryDisallowedFunc) {
        if (retryDisallowedFunc.test(t)) {
            return false;
        }

        if (t instanceof IOException || t instanceof HazelcastInstanceNotActiveException || t instanceof RetryableException) {
            return true;
        }
        if (t instanceof TargetDisconnectedException) {
            return retryableMessage || redoOperation;
        }
        return false;
    }

    private void logException(ILogger logger, Throwable exception, ClientInvocation invocation) {
        if (logger.isFinestEnabled()) {
            logger.finest("Invocation got an exception " + invocation
                    + ", exception : " + exception.getClass()
                    + ", message : " + exception.getMessage()
                    + (exception.getCause() != null ? (", cause :" + exception.getCause()) : ""));
        }
    }

    private boolean sendToPartitionOwner(
            ClientInvocation invocation,
            int partitionId,
            ClientInvocationFuture invocationFuture) {
        UUID partitionOwner = partitionService.getPartitionOwner(partitionId);
        if (partitionOwner == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Partition owner is not assigned yet");
            }
            return false;
        }
        return sendToMember(invocation, partitionOwner, invocationFuture);
    }

    /**
     * Behaviour of this method varies for unisocket and smart client
     * Unisocket invokes on only available connection
     * SmartClient randomly picks a connection to invoke on via {@link LoadBalancer}
     *
     * @param invocation to be invoked
     * @return true if successfully send, false otherwise
     */
    private boolean sendToRandom(ClientInvocation invocation,
                                 ClientInvocationFuture invocationFuture) {
        ClientConnection connection = connectionManager.getRandomConnection();
        if (connection == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("No connection found to invoke");
            }
            return false;
        }
        return send(invocation, invocationFuture, connection);
    }

    /**
     * @return true if successfully send the member with given uuid, false otherwise
     */
    private boolean sendToMember(ClientInvocation invocation, UUID uuid,
                                 ClientInvocationFuture invocationFuture) {
        assert (uuid != null);
        ClientConnection connection = connectionManager.getConnection(uuid);
        if (connection == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Client is not connected to target : " + uuid);
            }
            return false;
        }
        return send(invocation, invocationFuture, connection);
    }

    private boolean send(ClientInvocation invocation,
                         ClientInvocationFuture invocationFuture,
                         ClientConnection connection) {
        ClientMessage clientMessage = invocation.getClientMessage();
        if (isBackupAckToClientEnabled) {
            // Marking message as backup aware
            clientMessage.getStartFrame().flags |= ClientMessage.BACKUP_AWARE_FLAG;
        }

        long correlationId = clientMessage.getCorrelationId();
        invocations.put(correlationId, invocation);
        EventHandler handler = invocation.getEventHandler();
        if (handler != null) {
            connection.addEventHandler(correlationId, handler);
        }

        //After this is set, a second thread can notify this invocation
        //Connection could be closed. From this point on, we need to reacquire the permission to notify if needed.
        invocation.setSentConnection(connection);

        if (!connection.write(clientMessage)) {
            if (invocation.getPermissionToNotifyForDeadConnection(connection)) {
                IOException exception = new IOException("Packet not sent to " + connection.getRemoteAddress() + " "
                        + clientMessage);
                handleException(invocation, exception, clientMessage, invocationFuture);
            }
        } else {
            invocationFuture.invoked();
        }
        return true;
    }
}
