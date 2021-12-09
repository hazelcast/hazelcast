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

import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.impl.operationservice.impl.BaseInvocation;
import com.hazelcast.spi.impl.sequence.CallIdSequence;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;


/**
 * Handles the routing of a request from a Hazelcast client.
 * <p>
 * 1) Where should request be sent?<br>
 * 2) Should it be retried?<br>
 * 3) How many times is it retried?
 */
// TODO sancar convert to invocation builder
public class ClientInvocation extends BaseInvocation {
    private static final AtomicReferenceFieldUpdater<ClientInvocation, ClientConnection> SENT_CONNECTION
            = AtomicReferenceFieldUpdater.newUpdater(ClientInvocation.class, ClientConnection.class, "sentConnection");
    private static final AtomicLongFieldUpdater<ClientInvocation> INVOKE_COUNT
            = AtomicLongFieldUpdater.newUpdater(ClientInvocation.class, "invokeCount");
    final long startTimeMillis;
    final Object objectName;
    final Predicate<Throwable> retryDisallowedFunc;
    final long invocationTimeoutMillis;
    final boolean urgent;
    final boolean allowRetryOnRandom;
    volatile ClientMessage clientMessage;
    // Target fields */
    final UUID uuid;
    final int partitionId;
    final ClientConnection connection;
    //* Target fields
    //TODO sancar remove this dependency
    private final ClientInvocationFuture clientInvocationFuture;
    private final ClientInvocationServiceImpl invocationService;
    private final EventHandler handler;

    /**
     * We achieve synchronization of different threads via this field
     * sentConnection starts as null.
     * This field is set to a non-null when `connection` to be sent is determined.
     * This field is set to null when a response/exception is going to be notified.
     * This field is trying to be compared and set to null on connection close case with dead connection,
     * to prevent invocation to be notified which is already retried on another connection.
     * Only one thread that can set this to null can be actively notify a response/exception.
     * {@link #getPermissionToNotifyForDeadConnection}
     * {@link #getPermissionToNotify(long)}
     */
    private volatile ClientConnection sentConnection;
    private volatile long invokeCount;

    //TODO sancar introduce invocatinConfigs with static predefined configs and pass invocatoinConfig class here
    @SuppressWarnings("checkstyle:parameternumber")
    ClientInvocation(ClientInvocationFuture clientInvocationFuture,
                     ClientInvocationServiceImpl invocationService,
                     ClientMessage clientMessage, EventHandler handler, Object objectName,
                     Predicate<Throwable> retryDisallowedFunc, long invocationTimeoutMillis,
                     boolean urgent, boolean allowRetryOnRandom, int partitionId, UUID uuid, ClientConnection connection) {
        this.clientInvocationFuture = clientInvocationFuture;
        this.invocationService = invocationService;
        this.clientMessage = clientMessage;
        this.handler = handler;
        this.startTimeMillis = System.currentTimeMillis();
        this.objectName = objectName;
        this.retryDisallowedFunc = retryDisallowedFunc;
        this.invocationTimeoutMillis = invocationTimeoutMillis;
        this.urgent = urgent;
        this.allowRetryOnRandom = allowRetryOnRandom;
        this.partitionId = partitionId;
        this.uuid = uuid;
        this.connection = connection;
    }

    public ClientMessage getClientMessage() {
        return clientMessage;
    }

    /**
     * We make sure that `notifyResponse` can not be called by multiple threads concurrently
     * Only ones that can set a null on `sentConnection` can notify the invocation
     *
     * @param clientMessage return true if invocation completed
     */
    void notify(ClientMessage clientMessage) {
        assert clientMessage != null : "response can't be null";

        if (getPermissionToNotify(clientMessage.getCorrelationId())) {
            int expectedBackups = clientMessage.getNumberOfBackupAcks();
            notifyResponse(clientMessage, expectedBackups);
        }
    }

    boolean getPermissionToNotify(long responseCorrelationId) {
        ClientConnection conn = this.sentConnection;
        if (conn == null) {
            //invocation is being handled by a second thread.
            //we don't need to take action
            return false;
        }

        long requestCorrelationId = clientMessage.getCorrelationId();

        if (responseCorrelationId != requestCorrelationId) {
            //invocation is retried with new correlation id
            //we should not notify
            return false;
        }
        //we have the permission to notify if we can compareAndSet
        //otherwise another thread is handling it, we don't need to notify anymore
        return SENT_CONNECTION.compareAndSet(this, conn, null);
    }

    public void notifyForDeadConnection(ClientConnection deadConnection) {
        if (getPermissionToNotifyForDeadConnection(deadConnection)) {
            Exception ex = new TargetDisconnectedException(deadConnection.getCloseReason(),
                    deadConnection.getCloseCause());
            invocationService.handleException(this, ex, clientMessage, clientInvocationFuture);
        }
    }

    boolean getPermissionToNotifyForDeadConnection(ClientConnection deadConnection) {
        return SENT_CONNECTION.compareAndSet(this, deadConnection, null);
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
    public void completeExceptionally(Throwable t) {
        clientInvocationFuture.completeExceptionally(t);
        invocationService.deRegisterInvocation(clientMessage.getCorrelationId());
    }

    protected boolean shouldFailOnIndeterminateOperationState() {
        return invocationService.shouldFailOnIndeterminateOperationState;
    }

    void notifyException(long correlationId, Throwable exception) {
        if (getPermissionToNotify(correlationId)) {
            invocationService.handleException(this, exception, clientMessage, clientInvocationFuture);
        }
    }

    EventHandler getEventHandler() {
        return handler;
    }

    public void setSentConnection(ClientConnection connection) {
        SENT_CONNECTION.set(this, connection);
    }

    public void incrementInvokeCount() {
        INVOKE_COUNT.incrementAndGet(this);
    }

    public long getInvokeCount() {
        return invokeCount;
    }

    @Override
    public String toString() {
        String target;
        if (connection != null) {
            target = "connection " + connection;
        } else if (partitionId != -1) {
            target = "partition " + partitionId;
        } else if (uuid != null) {
            target = "uuid " + uuid;
        } else {
            target = "random";
        }
        return "ClientInvocation{"
                + "clientMessage = " + clientMessage
                + ", objectName = " + objectName
                + ", target = " + target
                + ", invoke count : " + invokeCount
                + ", sentConnection = " + sentConnection + '}';
    }

    // package private methods for tests
    CallIdSequence getCallIdSequence() {
        return invocationService.callIdSequence;
    }
}
