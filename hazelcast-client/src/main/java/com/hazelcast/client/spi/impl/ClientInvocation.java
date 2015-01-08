package com.hazelcast.client.spi.impl;

import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.executor.impl.client.RefreshableRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.config.ClientProperties.PROP_HEARTBEAT_INTERVAL_DEFAULT;

public class ClientInvocation implements Runnable {

    protected static final long RETRY_WAIT_TIME_IN_SECONDS = 1;
    private final LifecycleService lifecycleService;
    private final ClientInvocationService invocationService;
    private final ClientExecutionService executionService;
    private final ClientListenerServiceImpl listenerService;
    private final ClientRequest request;
    private final EventHandler handler;
    private final long retryCountLimit;

    private final ClientInvocationFuture clientInvocationFuture;
    private final int heartBeatInterval;
    private final AtomicInteger reSendCount = new AtomicInteger();

    private boolean isBindToSingleConnection;
    private Address address;
    private int partitionId = -1;
    private volatile ClientConnection connection;

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientRequest request, EventHandler handler) {
        this.lifecycleService = client.getLifecycleService();
        this.invocationService = client.getInvocationService();
        this.executionService = client.getClientExecutionService();
        this.listenerService = (ClientListenerServiceImpl) client.getListenerService();
        this.request = request;
        this.handler = handler;

        final ClientProperties clientProperties = client.getClientProperties();

        int waitTime = clientProperties.getInvocationTimeoutSeconds().getInteger();
        long retryTimeoutInSeconds = waitTime > 0 ? waitTime
                : Integer.parseInt(ClientProperties.PROP_INVOCATION_TIMEOUT_SECONDS_DEFAULT);

        clientInvocationFuture = new ClientInvocationFuture(this, client, request, handler);
        this.retryCountLimit = retryTimeoutInSeconds / RETRY_WAIT_TIME_IN_SECONDS;

        int interval = clientProperties.getHeartbeatInterval().getInteger();
        this.heartBeatInterval = interval > 0 ? interval : Integer.parseInt(PROP_HEARTBEAT_INTERVAL_DEFAULT);


    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientRequest request, EventHandler handler,
                            Connection connection) {
        this(client, request, handler);
        isBindToSingleConnection = true;
        this.connection = (ClientConnection) connection;
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientRequest request, EventHandler handler,
                            int partitionId) {
        this(client, request, handler);
        this.partitionId = partitionId;
    }

    public ClientInvocation(HazelcastClientInstanceImpl client, ClientRequest request, EventHandler handler,
                            Address address) {
        this(client, request, handler);
        this.address = address;
    }

    public ClientInvocation(ClientContext clientContext, ClientRequest request, EventHandler handler, Address address) {
        this((HazelcastClientInstanceImpl) clientContext.getHazelcastInstance(), request, handler);
        this.address = address;
    }

    public ClientInvocation(ClientContext clientContext, ClientRequest request, EventHandler handler, int partitionId) {
        this((HazelcastClientInstanceImpl) clientContext.getHazelcastInstance(), request, handler);
        this.partitionId = partitionId;
    }

    public ClientInvocation(ClientContext clientContext, ClientRequest request, EventHandler handler) {
        this((HazelcastClientInstanceImpl) clientContext.getHazelcastInstance(), request, handler);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public ClientRequest getRequest() {
        return request;
    }

    public EventHandler getHandler() {
        return handler;
    }

    public ClientInvocationFuture getFuture() {
        return clientInvocationFuture;
    }

    public ClientInvocationFuture invoke() {
        if (request == null) {
            throw new IllegalStateException("Request can not be null");
        }

        try {
            invokeOnSelection();
        } catch (Exception e) {
            notify(e);
        }
        return clientInvocationFuture;

    }

    private void invokeOnSelection() throws IOException {
        if (isBindToSingleConnection) {
            invocationService.invokeOnConnection(this, connection);
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


    public void notify(Object response) {
        if (response == null) {
            throw new IllegalArgumentException("response can't be null");
        }
        if (!(response instanceof Exception)) {
            clientInvocationFuture.setResponse(response);
            return;
        }

        Exception exception = (Exception) response;
        if (!lifecycleService.isRunning()) {
            clientInvocationFuture.setResponse(new HazelcastClientNotActiveException(exception.getMessage()));
            return;
        }
        notifyException(exception);

    }

    private void notifyException(Exception exception) {
        if (exception instanceof IOException || exception instanceof HazelcastInstanceNotActiveException) {
            if (handleRetry()) {
                return;
            }
        }
        if (exception instanceof RetryableHazelcastException) {
            if (request instanceof RetryableRequest || invocationService.isRedoOperation()) {
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
        if (request instanceof RefreshableRequest) {
            ((RefreshableRequest) request).refresh();
        }

        try {
            executionService.schedule(this, RETRY_WAIT_TIME_IN_SECONDS, TimeUnit.SECONDS);
        } catch (RejectedExecutionException e) {
            return false;
        }
        return true;
    }

    private boolean isBindToSingleConnection() {
        return isBindToSingleConnection;
    }

    boolean isConnectionHealthy(long elapsed) {
        if (elapsed >= heartBeatInterval) {
            if (connection != null) {
                return connection.isHeartBeating();
            } else {
                return false;
            }
        }
        return true;
    }

    public int getHeartBeatInterval() {
        return heartBeatInterval;
    }

    public void setConnection(ClientConnection connection) {
        this.connection = connection;
    }

    public ClientConnection getConnection() {
        return connection;
    }
}
