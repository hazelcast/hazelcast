package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.util.EmptyStatement;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.config.ClientProperties.PROP_HEARTBEAT_INTERVAL_DEFAULT;

public class ClientInvocation implements Runnable {

    private static final long RETRY_WAIT_TIME_IN_SECONDS = 1;
    private static final int UNASSIGNED_PARTITION = -1;
    private static final ILogger LOGGER = Logger.getLogger(ClientInvocation.class);
    private final LifecycleService lifecycleService;
    private final ClientInvocationService invocationService;
    private final ClientExecutionService executionService;
    private final ClientListenerServiceImpl listenerService;
    private final ClientMessage clientMessage;
    private final EventHandler handler;
    private final long retryCountLimit;
    private final ClientInvocationFuture clientInvocationFuture;

    private final int heartBeatInterval;
    private final AtomicInteger reSendCount = new AtomicInteger();
    private final Address address;
    private final int partitionId;
    private final Connection connection;
    private volatile ClientConnection sendConnection;
    private boolean bypassHeartbeatCheck;


    private ClientInvocation(HazelcastClientInstanceImpl client, EventHandler handler,
                             ClientMessage clientMessage, int partitionId, Address address,
                             Connection connection) {
        this.lifecycleService = client.getLifecycleService();
        this.invocationService = client.getInvocationService();
        this.executionService = client.getClientExecutionService();
        this.listenerService = (ClientListenerServiceImpl) client.getListenerService();
        this.handler = handler;
        this.clientMessage = clientMessage;
        this.partitionId = partitionId;
        this.address = address;
        this.connection = connection;
        final ClientProperties clientProperties = client.getClientProperties();

        int waitTime = clientProperties.getInvocationTimeoutSeconds().getInteger();
        long retryTimeoutInSeconds = waitTime > 0 ? waitTime
                : Integer.parseInt(ClientProperties.PROP_INVOCATION_TIMEOUT_SECONDS_DEFAULT);

        clientInvocationFuture = new ClientInvocationFuture(this, client, clientMessage, handler);
        this.retryCountLimit = retryTimeoutInSeconds / RETRY_WAIT_TIME_IN_SECONDS;

        int interval = clientProperties.getHeartbeatInterval().getInteger();
        this.heartBeatInterval = interval > 0 ? interval : Integer.parseInt(PROP_HEARTBEAT_INTERVAL_DEFAULT);
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, EventHandler handler, ClientMessage clientMessage) {
        this(client, handler, clientMessage, UNASSIGNED_PARTITION, null, null);

    }

    public ClientInvocation(HazelcastClientInstanceImpl client, EventHandler handler,
                            ClientMessage clientMessage, int partitionId) {
        this(client, handler, clientMessage, partitionId, null, null);
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, EventHandler handler,
                            ClientMessage clientMessage, Address address) {
        this(client, handler, clientMessage, UNASSIGNED_PARTITION, address, null);
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, EventHandler handler,
                            ClientMessage clientMessage, Connection connection) {
        this(client, handler, clientMessage, UNASSIGNED_PARTITION, null, connection);

    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage) {
        this(client, null, clientMessage);
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage,
                            int partitionId) {
        this(client, null, clientMessage, partitionId);
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage,
                            Address address) {
        this(client, null, clientMessage, address);
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientMessage clientMessage,
                            Connection connection) {
        this(client, null, clientMessage, connection);
    }


    public int getPartitionId() {
        return partitionId;
    }

    public ClientMessage getClientMessage() {
        return clientMessage;
    }

    public EventHandler getHandler() {
        return handler;
    }

    public ClientInvocationFuture invoke() {
        if (clientMessage == null) {
            throw new IllegalStateException("Request can not be null");
        }

        try {
            invokeOnSelection();
        } catch (Exception e) {
            notifyException(e);
        }
        return clientInvocationFuture;

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
            if (handler != null) {
                listenerService.registerFailedListener(this);
            } else {
                clientInvocationFuture.setResponse(e);
            }
        }
    }


    public void notify(ClientMessage clientMessage) {
        if (clientMessage == null) {
            throw new IllegalArgumentException("response can't be null");
        }
        clientInvocationFuture.setResponse(clientMessage);

    }

    public void notifyException(Throwable exception) {

        if (!lifecycleService.isRunning()) {
            clientInvocationFuture.setResponse(new HazelcastClientNotActiveException(exception.getMessage()));
            return;
        }

        if (exception instanceof IOException || exception instanceof HazelcastInstanceNotActiveException) {
            if (handleRetry()) {
                return;
            }
        }
        if (exception instanceof RetryableHazelcastException) {
            if (clientMessage instanceof RetryableRequest || invocationService.isRedoOperation()) {
                if (handleRetry()) {
                    return;
                }
            }
        }
        clientInvocationFuture.setResponse(exception);
    }


    private boolean handleRetry() {

        if (isBindToSingleConnection()) {
            return false;
        }
        if (handler == null && reSendCount.incrementAndGet() > retryCountLimit) {
            return false;
        }
        if (handler != null) {
            handler.beforeListenerRegister();
        }

        try {
            sleep();
            executionService.execute(this);
        } catch (RejectedExecutionException e) {
            if (LOGGER.isFinestEnabled()) {
                LOGGER.finest("Retry could not be scheduled ", e);
            }
            clientInvocationFuture.setResponse(e);
        }
        return true;
    }

    private void sleep() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(RETRY_WAIT_TIME_IN_SECONDS));
        } catch (InterruptedException ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    private boolean isBindToSingleConnection() {
        return connection != null;
    }

    boolean isConnectionHealthy(long elapsed) {
        if (elapsed >= heartBeatInterval) {
            if (sendConnection != null) {
                return sendConnection.isHeartBeating();
            } else {
                return true;
            }
        }
        return true;
    }

    public int getHeartBeatInterval() {
        return heartBeatInterval;
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

    public ClientConnection getSendConnection() {
        return sendConnection;
    }

    public boolean isInvoked() {
        return sendConnection != null;
    }
}
