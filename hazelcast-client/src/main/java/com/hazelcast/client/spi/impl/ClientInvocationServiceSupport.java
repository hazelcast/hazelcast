package com.hazelcast.client.spi.impl;

import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.cluster.client.ClientPingRequest;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;


abstract class ClientInvocationServiceSupport implements ClientInvocationService {

    protected static final long RETRY_WAIT_TIME_IN_SECONDS = 1;
    protected final long retryTimeoutInSeconds;
    protected final HazelcastClientInstanceImpl client;
    protected final ClientConnectionManager connectionManager;
    protected final ClientPartitionService partitionService;
    private final ILogger logger = Logger.getLogger(ClientInvocationService.class);
    private final ResponseThread responseThread;
    private volatile boolean isShutdown;

    public ClientInvocationServiceSupport(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.connectionManager = client.getConnectionManager();
        this.partitionService = client.getClientPartitionService();
        responseThread = new ResponseThread(client.getThreadGroup(), client.getName() + ".response-",
                client.getClientConfig().getClassLoader());
        responseThread.start();

        final ClientProperties clientProperties = client.getClientProperties();
        int waitTime = clientProperties.getInvocationTimeoutSeconds().getInteger();
        this.retryTimeoutInSeconds = waitTime > 0 ? waitTime
                : Integer.parseInt(ClientProperties.PROP_INVOCATION_TIMEOUT_SECONDS_DEFAULT);
    }

    @Override
    public boolean isRedoOperation() {
        return client.getClientConfig().getNetworkConfig().isRedoOperation();
    }

    protected <T> ICompletableFuture<T> send(ClientRequest request, ClientConnection connection,
                                             EventHandler handler) {
        final ClientCallFuture<T> future = new ClientCallFuture<T>(client, request, handler);
        sendInternal(future, connection);
        return future;
    }

    protected void sendInternal(ClientCallFuture future, ClientConnection connection) {
        connection.registerCallId(future);
        future.setConnection(connection);
        final SerializationService ss = client.getSerializationService();
        final ClientRequest request = future.getRequest();
        final Data data = ss.toData(request);
        Packet packet = new Packet(data, request.getPartitionId(), ss.getPortableContext());
        if (!isAllowedToSendRequest(connection, request) || !connection.write(packet)) {
            final int callId = request.getCallId();
            connection.deRegisterCallId(callId);
            connection.deRegisterEventHandler(callId);
            future.notify(new TargetNotMemberException("Address : " + connection.getRemoteEndpoint()));
        }
    }

    private boolean isAllowedToSendRequest(ClientConnection connection, ClientRequest request) {
        if (!connection.isHeartBeating()) {
            if (request instanceof ClientPingRequest) {
                //ping request should be send even though heart is not beating
                return true;
            }

            if (logger.isFinestEnabled()) {
                logger.warning("Connection is not heart-beating, won't write request -> " + request);
            }
            return false;
        }
        return true;
    }

    public void shutdown() {
        isShutdown = true;
        responseThread.interrupt();
    }

    @Override
    public void handlePacket(Packet packet) {
        responseThread.workQueue.add(packet);
    }

    private class ResponseThread extends Thread {
        private final BlockingQueue<Packet> workQueue = new LinkedBlockingQueue<Packet>();

        public ResponseThread(ThreadGroup threadGroup, String name, ClassLoader classLoader) {
            super(threadGroup, name);
            setContextClassLoader(classLoader);
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (OutOfMemoryError e) {
                onOutOfMemory(e);
            } catch (Throwable t) {
                logger.severe(t);
            }
        }

        private void doRun() {
            while (true) {
                Packet task;
                try {
                    task = workQueue.take();
                } catch (InterruptedException e) {
                    if (isShutdown) {
                        return;
                    }
                    continue;
                }

                if (isShutdown) {
                    return;
                }
                process(task);
            }
        }

        private void process(Packet packet) {
            final ClientConnection conn = (ClientConnection) packet.getConn();
            try {
                final ClientResponse clientResponse = client.getSerializationService().toObject(packet.getData());
                final int callId = clientResponse.getCallId();
                final Data response = clientResponse.getResponse();
                handlePacket(response, clientResponse.isError(), callId, conn);
            } catch (Exception e) {
                logger.severe("Failed to process task: " + packet + " on responseThread :" + getName());
            } finally {
                conn.decrementPacketCount();
            }
        }

        private void handlePacket(Object response, boolean isError, int callId, ClientConnection conn) {
            final ClientCallFuture future = conn.deRegisterCallId(callId);
            if (future == null) {
                logger.warning("No call for callId: " + callId + ", response: " + response);
                return;
            }
            if (isError) {
                response = client.getSerializationService().toObject(response);
            }
            future.notify(response);
        }

    }

}
