/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.spi.properties.ClientProperty.INVOCATION_TIMEOUT_SECONDS;

/**
 * ClientInvocation handles routing of a request from client
 * <p/>
 * 1) Where should request be send
 * 2) Should it be retried
 * 3) How many times it is retried
 */
public class ClientInvocation implements Runnable {

    public static final long RETRY_WAIT_TIME_IN_SECONDS = 1;
    protected static final int UNASSIGNED_PARTITION = -1;

    protected final ClientInvocationFuture clientInvocationFuture;
    private final ILogger logger;
    private final LifecycleService lifecycleService;
    private final ClientInvocationService invocationService;
    private final ClientExecutionService executionService;
    private final ClientMessage clientMessage;

    private final Address address;
    private final int partitionId;
    private final Connection connection;
    private volatile ClientConnection sendConnection;
    private boolean bypassHeartbeatCheck;
    private boolean urgent;
    private long retryTimeoutPointInMillis;
    private EventHandler handler;


    protected ClientInvocation(HazelcastClientInstanceImpl client,
                               ClientMessage clientMessage, int partitionId, Address address,
                               Connection connection) {
        this.lifecycleService = client.getLifecycleService();
        this.invocationService = client.getInvocationService();
        this.executionService = client.getClientExecutionService();
        this.clientMessage = clientMessage;
        this.partitionId = partitionId;
        this.address = address;
        this.connection = connection;

        HazelcastProperties hazelcastProperties = client.getProperties();
        long waitTime = hazelcastProperties.getMillis(INVOCATION_TIMEOUT_SECONDS);
        long waitTimeResolved = waitTime > 0 ? waitTime : Integer.parseInt(INVOCATION_TIMEOUT_SECONDS.getDefaultValue());
        retryTimeoutPointInMillis = System.currentTimeMillis() + waitTimeResolved;

        logger = ((ClientInvocationServiceSupport) invocationService).invocationLogger;
        clientInvocationFuture = new ClientInvocationFuture(this, client, clientMessage, logger);
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage) {
        this(client, clientMessage, UNASSIGNED_PARTITION, null, null);
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage,
                            int partitionId) {
        this(client, clientMessage, partitionId, null, null);
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage,
                            Address address) {
        this(client, clientMessage, UNASSIGNED_PARTITION, address, null);
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage,
                            Connection connection) {
        this(client, clientMessage, UNASSIGNED_PARTITION, null, connection);
    }


    public int getPartitionId() {
        return partitionId;
    }

    public ClientMessage getClientMessage() {
        return clientMessage;
    }

    public ClientInvocationFuture invoke() {
        assert (clientMessage != null);

        try {
            invokeOnSelection();
        } catch (Exception e) {
            if (e instanceof HazelcastOverloadException) {
                throw (HazelcastOverloadException) e;
            }
            notifyException(e);
        }
        return clientInvocationFuture;

    }

    public ClientInvocationFuture invokeUrgent() {
        urgent = true;
        return invoke();
    }

    private void invokeOnSelection() throws IOException {
        if (isBindToSingleConnection()) {
            invocationService.invokeOnConnection(this, (ClientConnection) connection);
        } else if (partitionId != -1) {
            invocationService.invokeOnPartitionOwner(this, partitionId);
        } else if (address != null) {
            invocationService.invokeOnTarget(this, address);
        } else {
            invocationService.invokeOnRandomTarget(this);
        }
    }

    @Override
    public void run() {
        try {
            invoke();
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

        if (!lifecycleService.isRunning()) {
            clientInvocationFuture.complete(new HazelcastClientNotActiveException(exception.getMessage(), exception));
            return;
        }

        if (isRetryable(exception)) {
            if (handleRetry()) {
                return;
            }
        }
        if (exception instanceof RetryableHazelcastException) {
            if (clientMessage.isRetryable() || invocationService.isRedoOperation()) {
                if (handleRetry()) {
                    return;
                }
            }
        }
        clientInvocationFuture.complete(exception);
    }

    private boolean handleRetry() {
        if (isBindToSingleConnection()) {
            return false;
        }

        if (!shouldRetry()) {
            return false;
        }

        try {
            rescheduleInvocation();
        } catch (RejectedExecutionException e) {
            if (logger.isFinestEnabled()) {
                logger.finest("Retry could not be scheduled ", e);
            }
            notifyException(e);
        }
        return true;
    }

    private void rescheduleInvocation() {
        ClientExecutionServiceImpl executionServiceImpl = (ClientExecutionServiceImpl) this.executionService;
        executionServiceImpl.schedule(this, RETRY_WAIT_TIME_IN_SECONDS, TimeUnit.SECONDS);
    }

    protected boolean shouldRetry() {
        return System.currentTimeMillis() < retryTimeoutPointInMillis;
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

    public boolean isUrgent() {
        return urgent;
    }

    public void setBypassHeartbeatCheck(boolean bypassHeartbeatCheck) {
        this.bypassHeartbeatCheck = bypassHeartbeatCheck;
    }

    public void setSendConnection(ClientConnection connection) {
        this.sendConnection = connection;
    }

    public ClientConnection getSendConnectionOrWait() throws InterruptedException {
        while (sendConnection == null && !clientInvocationFuture.isDone()) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(RETRY_WAIT_TIME_IN_SECONDS));
        }
        return sendConnection;
    }

    public ClientConnection getSendConnection() {
        return sendConnection;
    }

    public static boolean isRetryable(Throwable t) {
        return t instanceof IOException || t instanceof HazelcastInstanceNotActiveException;
    }
}
