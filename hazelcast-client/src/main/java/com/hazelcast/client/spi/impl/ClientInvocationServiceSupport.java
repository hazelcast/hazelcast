package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.client.impl.client.RemoveAllListeners;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.cluster.client.ClientPingRequest;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.util.ConstructorFunction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;


abstract class ClientInvocationServiceSupport implements ClientInvocationService,
        ConnectionHeartbeatListener, ConnectionListener {

    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED = 10;
    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED_THRESHOLD = 5000;
    protected final HazelcastClientInstanceImpl client;
    protected final ClientConnectionManager connectionManager;
    protected final ClientPartitionService partitionService;
    protected final ClientExecutionService executionService;
    private final ILogger logger = Logger.getLogger(ClientInvocationService.class);
    private final ResponseThread responseThread;
    private final ConcurrentMap<Integer, ClientInvocation> callIdMap
            = new ConcurrentHashMap<Integer, ClientInvocation>();
    private final ConcurrentMap<Integer, ClientInvocation> eventHandlerMap
            = new ConcurrentHashMap<Integer, ClientInvocation>();
    private final AtomicInteger callIdIncrementer = new AtomicInteger();

    private volatile boolean isShutdown;


    public ClientInvocationServiceSupport(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.connectionManager = client.getConnectionManager();
        this.executionService = client.getClientExecutionService();
        connectionManager.addConnectionListener(this);
        connectionManager.addConnectionHeartbeatListener(this);
        this.partitionService = client.getClientPartitionService();
        responseThread = new ResponseThread(client.getThreadGroup(), client.getName() + ".response-",
                client.getClientConfig().getClassLoader());
        responseThread.start();
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target) throws Exception {
        return new ClientInvocation(client, request, target).invoke();
    }

    @Override
    public boolean isRedoOperation() {
        return client.getClientConfig().getNetworkConfig().isRedoOperation();
    }

    protected void send(ClientInvocation invocation, ClientConnection connection) throws IOException {
        if (isShutdown) {
            throw new HazelcastClientNotActiveException("Client is shut down");
        }
        registerInvocation(invocation);
        invocation.setConnection(connection);
        final SerializationService ss = client.getSerializationService();
        final Data data = ss.toData(invocation.getRequest());
        Packet packet = new Packet(data, invocation.getPartitionId(), ss.getPortableContext());
        if (!isAllowedToSendRequest(connection, invocation.getRequest()) || !connection.write(packet)) {
            final int callId = invocation.getRequest().getCallId();
            deRegisterCallId(callId);
            deRegisterEventHandler(callId);
            throw new IOException("Packet not send to " + connection.getRemoteEndpoint());
        }
    }

    private boolean isAllowedToSendRequest(ClientConnection connection, ClientRequest request) {
        if (!connection.isHeartBeating()) {
            if (request instanceof ClientPingRequest || request instanceof RemoveAllListeners) {
                //ping request and removeAllListeners should be send even though heart is not beating
                return true;
            }

            if (logger.isFinestEnabled()) {
                logger.warning("Connection is not heart-beating, won't write request -> " + request);
            }
            return false;
        }
        return true;
    }

    private void registerInvocation(ClientInvocation clientInvocation) {
        final int callId = newCallId();
        clientInvocation.getRequest().setCallId(callId);
        callIdMap.put(callId, clientInvocation);
        if (clientInvocation.getHandler() != null) {
            eventHandlerMap.put(callId, clientInvocation);
        }
    }

    private ClientInvocation deRegisterCallId(int callId) {
        return callIdMap.remove(callId);
    }

    private ClientInvocation deRegisterEventHandler(int callId) {
        return eventHandlerMap.remove(callId);
    }

    @Override
    public EventHandler getEventHandler(int callId) {
        final ClientInvocation clientInvocation = eventHandlerMap.get(callId);
        if (clientInvocation == null) {
            return null;
        }
        return clientInvocation.getHandler();
    }

    @Override
    public boolean removeEventHandler(Integer callId) {
        if (callId != null) {
            return eventHandlerMap.remove(callId) != null;

        }
        return false;
    }


    public void cleanResources(ConstructorFunction<Object, Throwable> responseCtor, ClientConnection connection) {
        final Iterator<Map.Entry<Integer, ClientInvocation>> iter = callIdMap.entrySet().iterator();
        while (iter.hasNext()) {
            final Map.Entry<Integer, ClientInvocation> entry = iter.next();
            final ClientInvocation invocation = entry.getValue();
            if (invocation.getConnection().equals(connection)) {
                iter.remove();
                invocation.notify(responseCtor.createNew(null));
                eventHandlerMap.remove(entry.getKey());
            }
        }
        final Iterator<ClientInvocation> iterator = eventHandlerMap.values().iterator();
        while (iterator.hasNext()) {
            final ClientInvocation invocation = iterator.next();
            if (invocation.getConnection().equals(connection)) {
                iterator.remove();
                invocation.notify(responseCtor.createNew(null));
            }
        }

    }

    @Override
    public void heartBeatStarted(Connection connection) {

    }

    @Override
    public void heartBeatStopped(Connection connection) {
        final RemoveAllListeners request = new RemoveAllListeners();
        new ClientInvocation(client, request, connection).invoke();

        final Address remoteEndpoint = connection.getEndPoint();
        final Iterator<ClientInvocation> iterator = eventHandlerMap.values().iterator();
        final TargetDisconnectedException response = new TargetDisconnectedException(remoteEndpoint);

        while (iterator.hasNext()) {
            final ClientInvocation clientInvocation = iterator.next();
            if (clientInvocation.getConnection().equals(connection)) {
                iterator.remove();
                clientInvocation.notify(response);
            }
        }
    }

    @Override
    public void connectionAdded(Connection connection) {

    }

    @Override
    public void connectionRemoved(Connection connection) {
        cleanConnectionResources((ClientConnection) connection);
    }

    @Override
    public void cleanConnectionResources(ClientConnection connection) {
        if (connectionManager.isAlive()) {
            try {
                executionService.execute(new CleanResourcesTask(connection));
            } catch (RejectedExecutionException e) {
                logger.warning("Execution rejected ", e);
            }
        } else {
            cleanResources(new ConstructorFunction<Object, Throwable>() {
                @Override
                public Throwable createNew(Object arg) {
                    return new HazelcastClientNotActiveException("Client is shutting down!");
                }
            }, connection);
        }
    }

    public void shutdown() {
        isShutdown = true;
        responseThread.interrupt();
    }

    private class CleanResourcesTask implements Runnable {

        private final ClientConnection connection;

        CleanResourcesTask(ClientConnection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {
            waitForPacketsProcessed();
            cleanResources(new ConstructorFunction<Object, Throwable>() {
                @Override
                public Throwable createNew(Object arg) {
                    return new TargetDisconnectedException(connection.getRemoteEndpoint());
                }
            }, connection);
        }

        private void waitForPacketsProcessed() {
            final long begin = System.currentTimeMillis();
            int count = connection.getPacketCount();
            while (count != 0) {
                try {
                    Thread.sleep(WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED);
                } catch (InterruptedException e) {
                    logger.warning(e);
                    break;
                }
                long elapsed = System.currentTimeMillis() - begin;
                if (elapsed > WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED_THRESHOLD) {
                    logger.warning("There are packets which are not processed " + count);
                    break;
                }
                count = connection.getPacketCount();
            }
        }
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
                handlePacket(response, clientResponse.isError(), callId);
            } catch (Exception e) {
                logger.severe("Failed to process task: " + packet + " on responseThread :" + getName());
            } finally {
                conn.decrementPacketCount();
            }
        }

        private void handlePacket(Object response, boolean isError, int callId) {
            final ClientInvocation future = deRegisterCallId(callId);
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

    private int newCallId() {
        return callIdIncrementer.incrementAndGet();
    }

}
