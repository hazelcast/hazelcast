package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.StripedExecutor;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;

public final class ClientListenerServiceImpl implements ClientListenerService {

    private final ILogger logger = Logger.getLogger(ClientInvocationService.class);
    private final ClientConnectionManager connectionManager;
    private final SerializationService serializationService;
    private final ClientInvocationService invocationService;
    private final ConcurrentMap<String, Integer> registrationMap = new ConcurrentHashMap<String, Integer>();
    private final ConcurrentMap<String, String> registrationAliasMap = new ConcurrentHashMap<String, String>();
    private final StripedExecutor eventExecutor;

    private final Set<ClientCallFuture> failedListeners =
            Collections.newSetFromMap(new ConcurrentHashMap<ClientCallFuture, Boolean>());

    public ClientListenerServiceImpl(HazelcastClient client, int eventThreadCount, int eventQueueCapacity) {
        this.connectionManager = client.getConnectionManager();
        this.serializationService = client.getSerializationService();
        this.invocationService = client.getInvocationService();
        this.eventExecutor = new StripedExecutor(logger, client.getName() + ".event",
                client.getThreadGroup(), eventThreadCount, eventQueueCapacity);
    }

    @Override
    public String listen(ClientRequest request, Object key, EventHandler handler) {
        final Future future;
        try {
            handler.beforeListenerRegister();
            if (key == null) {
                future = invocationService.invokeOnRandomTarget(request, handler);
            } else {
                future = invocationService.invokeOnKeyOwner(request, key, handler);
            }
            String registrationId = serializationService.toObject(future.get());
            registerListener(registrationId, request.getCallId());
            return registrationId;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean stopListening(BaseClientRemoveListenerRequest request, String registrationId) {
        try {
            String realRegistrationId = deRegisterListener(registrationId);
            if (realRegistrationId == null) {
                return false;
            }
            request.setRegistrationId(realRegistrationId);
            final Future<Boolean> future = invocationService.invokeOnRandomTarget(request);
            return (Boolean) serializationService.toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void registerFailedListener(ClientCallFuture future) {
        failedListeners.add(future);
    }

    public void triggerFailedListeners() {
        final Iterator<ClientCallFuture> iterator = failedListeners.iterator();
        while (iterator.hasNext()) {
            final ClientCallFuture failedListener = iterator.next();
            iterator.remove();
            failedListener.resend();
        }
    }

    public void registerListener(String uuid, Integer callId) {
        registrationAliasMap.put(uuid, uuid);
        registrationMap.put(uuid, callId);
    }

    public void reRegisterListener(String uuid, String alias, Integer callId) {
        final String oldAlias = registrationAliasMap.put(uuid, alias);
        if (oldAlias != null) {
            registrationMap.remove(oldAlias);
            registrationMap.put(alias, callId);
        }
    }

    public String deRegisterListener(String alias) {
        final String uuid = registrationAliasMap.remove(alias);
        if (uuid != null) {
            final Integer callId = registrationMap.remove(alias);
            connectionManager.removeEventHandler(callId);
        }
        return uuid;
    }

    public void handleEventPacket(Packet packet) {
        try {
            eventExecutor.execute(new ClientEventProcessor(packet));
        } catch (RejectedExecutionException e) {
            logger.log(Level.WARNING, " event packet could not be handled ", e);
        }
    }

    public void shutdown() {
        eventExecutor.shutdown();
    }

    private final class ClientEventProcessor implements Runnable {
        final Packet packet;

        private ClientEventProcessor(Packet packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            final ClientConnection conn = (ClientConnection) packet.getConn();
            final ClientResponse clientResponse = serializationService.toObject(packet.getData());
            final int callId = clientResponse.getCallId();
            final Data response = clientResponse.getResponse();
            handleEvent(response, callId, conn);
        }

        private void handleEvent(Data event, int callId, ClientConnection conn) {
            final EventHandler eventHandler = conn.getEventHandler(callId);
            final Object eventObject = serializationService.toObject(event);
            if (eventHandler == null) {
                logger.warning("No eventHandler for callId: " + callId + ", event: " + eventObject + ", conn: " + conn);
                return;
            }
            eventHandler.handle(eventObject);
        }
    }
}
