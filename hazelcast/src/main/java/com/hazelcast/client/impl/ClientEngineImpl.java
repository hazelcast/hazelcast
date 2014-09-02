/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEndpointManager;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.impl.client.AuthenticationRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.client.impl.operations.ClientDisconnectionOperation;
import com.hazelcast.client.impl.operations.PostJoinClientOperation;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.impl.InternalOperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.util.executor.ExecutorType;

import javax.security.auth.login.LoginException;
import java.security.Permission;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.spi.impl.ResponseHandlerFactory.createEmptyResponseHandler;

/**
 * Class that requests, listeners from client handled in node side.
 */
public class ClientEngineImpl implements ClientEngine, CoreService, PostJoinAwareService,
        ManagedService, MembershipAwareService, EventPublishingService<ClientEndpointImpl, ClientListener> {

    /**
     * Service Name of clientEngine to be used in requests
     */
    public static final String SERVICE_NAME = "hz:core:clientEngine";
    private static final int ENDPOINT_REMOVE_DELAY_MS = 10;
    private static final int EXECUTOR_QUEUE_CAPACITY_PER_CORE = 100000;
    private static final int THREADS_PER_CORE = 20;

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final Executor executor;

    private final SerializationService serializationService;
    // client uuid -> member uuid
    private final ConcurrentMap<String, String> ownershipMappings = new ConcurrentHashMap<String, String>();

    private final ClientEndpointManagerImpl endpointManager;
    private final ILogger logger;
    private final ConnectionListener connectionListener = new ConnectionListenerImpl();

    public ClientEngineImpl(Node node) {
        this.logger = node.getLogger(ClientEngine.class);
        this.node = node;
        this.serializationService = node.getSerializationService();
        this.nodeEngine = node.nodeEngine;
        this.endpointManager = new ClientEndpointManagerImpl(this, nodeEngine);
        this.executor = newExecutor();

        ClientHeartbeatMonitor heartBeatMonitor = new ClientHeartbeatMonitor(
                endpointManager, this, nodeEngine.getExecutionService(), node.groupProperties);
        heartBeatMonitor.start();
    }

    private Executor newExecutor() {
        final ExecutionService executionService = nodeEngine.getExecutionService();
        int coreSize = Runtime.getRuntime().availableProcessors();

        int threadCount = node.getGroupProperties().CLIENT_ENGINE_THREAD_COUNT.getInteger();
        if (threadCount <= 0) {
            threadCount = coreSize * THREADS_PER_CORE;
        }

        return executionService.register(ExecutionService.CLIENT_EXECUTOR,
                threadCount, coreSize * EXECUTOR_QUEUE_CAPACITY_PER_CORE,
                ExecutorType.CONCRETE);
    }

    //needed for testing purposes
    public ConnectionListener getConnectionListener() {
        return connectionListener;
    }

    @Override
    public int getClientEndpointCount() {
        return endpointManager.size();
    }

    public void handlePacket(Packet packet) {
        int partitionId = packet.getPartitionId();
        if (partitionId < 0) {
            executor.execute(new ClientPacketProcessor(packet));
        } else {
            InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();
            operationService.execute(new ClientPacketProcessor(packet), packet.getPartitionId());
        }
    }

    @Override
    public InternalPartitionService getPartitionService() {
        return nodeEngine.getPartitionService();
    }

    @Override
    public ClusterService getClusterService() {
        return nodeEngine.getClusterService();
    }

    @Override
    public EventService getEventService() {
        return nodeEngine.getEventService();
    }

    @Override
    public ProxyService getProxyService() {
        return nodeEngine.getProxyService();
    }

    public void sendResponse(ClientEndpointImpl endpoint, Object response, int callId, boolean isError, boolean isEvent) {
        Data data = serializationService.toData(response);
        ClientResponse clientResponse = new ClientResponse(data, callId, isError);
        sendResponse(endpoint, clientResponse, isEvent);
    }

    private void sendResponse(ClientEndpointImpl endpoint, ClientResponse response, boolean isEvent) {
        Data resultData = serializationService.toData(response);
        Connection conn = endpoint.getConnection();
        final Packet packet = new Packet(resultData, serializationService.getPortableContext());
        if (isEvent) {
            packet.setHeader(Packet.HEADER_EVENT);
        }
        conn.write(packet);
    }

    @Override
    public Address getMasterAddress() {
        return node.getMasterAddress();
    }

    @Override
    public Address getThisAddress() {
        return node.getThisAddress();
    }

    @Override
    public MemberImpl getLocalMember() {
        return node.getLocalMember();
    }

    @Override
    public Config getConfig() {
        return node.getConfig();
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return node.getLogger(clazz);
    }

    public ClientEndpointManager getEndpointManager() {
        return endpointManager;
    }

    @Override
    public SecurityContext getSecurityContext() {
        return node.securityContext;
    }

    public void bind(final ClientEndpoint endpoint) {
        final Connection conn = endpoint.getConnection();
        if (conn instanceof TcpIpConnection) {
            Address address = new Address(conn.getRemoteSocketAddress());
            TcpIpConnectionManager connectionManager = (TcpIpConnectionManager) node.getConnectionManager();
            connectionManager.bind((TcpIpConnection) conn, address, null, false);
        }
        sendClientEvent(endpoint);
    }

    void sendClientEvent(ClientEndpoint endpoint) {
        if (!endpoint.isFirstConnection()) {
            final EventService eventService = nodeEngine.getEventService();
            final Collection<EventRegistration> regs = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
            eventService.publishEvent(SERVICE_NAME, regs, endpoint, endpoint.getUuid().hashCode());
        }
    }

    @Override
    public void dispatchEvent(ClientEndpointImpl event, ClientListener listener) {
        if (event.isAuthenticated()) {
            listener.clientConnected(event);
        } else {
            listener.clientDisconnected(event);
        }
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        if (event.getMember().localMember()) {
            return;
        }

        final String deadMemberUuid = event.getMember().getUuid();
        try {
            nodeEngine.getExecutionService().schedule(new DestroyEndpointTask(deadMemberUuid),
                    ENDPOINT_REMOVE_DELAY_MS, TimeUnit.SECONDS);

        } catch (RejectedExecutionException e) {
            if (logger.isFinestEnabled()) {
                logger.finest(e);
            }
        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    public Collection<Client> getClients() {
        final HashSet<Client> clients = new HashSet<Client>();
        for (ClientEndpoint endpoint : endpointManager.getEndpoints()) {
            if (!endpoint.isFirstConnection()) {
                clients.add((Client) endpoint);
            }
        }
        return clients;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        node.getConnectionManager().addConnectionListener(connectionListener);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        for (ClientEndpoint ce : endpointManager.getEndpoints()) {
            ClientEndpointImpl endpoint = (ClientEndpointImpl) ce;
            try {
                endpoint.destroy();
            } catch (LoginException e) {
                logger.finest(e.getMessage());
            }
            try {
                final Connection conn = endpoint.getConnection();
                if (conn.live()) {
                    conn.close();
                }
            } catch (Exception e) {
                logger.finest(e);
            }
        }
        endpointManager.clear();
        ownershipMappings.clear();
    }

    public void addOwnershipMapping(String clientUuid, String ownerUuid) {
        ownershipMappings.put(clientUuid, ownerUuid);
    }

    public void removeOwnershipMapping(String clientUuid) {
        ownershipMappings.remove(clientUuid);
    }

    public TransactionManagerService getTransactionManagerService() {
        return node.nodeEngine.getTransactionManagerService();
    }

    private final class ClientPacketProcessor implements Runnable {
        final Packet packet;

        private ClientPacketProcessor(Packet packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            Connection conn = packet.getConn();
            ClientEndpointImpl endpoint = (ClientEndpointImpl) endpointManager.getEndpoint(conn);
            ClientRequest request = null;
            try {
                if (!node.joined()) {
                    throw new HazelcastInstanceNotActiveException("Hazelcast instance is not ready yet!");
                }
                request = loadRequest();
                if (request == null) {
                    handlePacketWithNullRequest();
                } else if (request instanceof AuthenticationRequest) {
                    if (conn.live()) {
                        endpoint = new ClientEndpointImpl(ClientEngineImpl.this, conn);
                        processRequest(endpoint, request);
                    } else {
                        handleEndpointNotCreatedConnectionNotAlive();
                    }
                } else if (endpoint == null) {
                    handleMissingEndpoint(conn);
                } else if (endpoint.isAuthenticated()) {
                    processRequest(endpoint, request);
                } else {
                    handleAuthenticationFailure(endpoint, request);
                }
            } catch (Throwable e) {
                handleProcessingFailure(endpoint, request, e);
            }
        }

        private ClientRequest loadRequest() {
            Data data = packet.getData();
            return serializationService.toObject(data);
        }

        private void handleEndpointNotCreatedConnectionNotAlive() {
            logger.warning("Dropped: " + packet + " -> endpoint not created for AuthenticationRequest, "
                    + "connection not alive");
        }

        private void handlePacketWithNullRequest() {
            logger.warning("Dropped: " + packet + " -> null request");
        }

        private void handleMissingEndpoint(Connection conn) {
            if (conn.live()) {
                logger.severe("Dropping: " + packet + " -> no endpoint found for live connection.");
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("Dropping: " + packet + " -> no endpoint found for dead connection.");
                }
            }
        }

        private void handleProcessingFailure(ClientEndpointImpl endpoint, ClientRequest request, Throwable e) {
            Level level = nodeEngine.isActive() ? Level.SEVERE : Level.FINEST;
            if (logger.isLoggable(level)) {
                if (request == null) {
                    logger.log(level, e.getMessage(), e);
                } else {
                    logger.log(level, "While executing request: " + request + " -> " + e.getMessage(), e);
                }
            }

            if (request != null && endpoint != null) {
                endpoint.sendResponse(e, request.getCallId());
            }
        }

        private void processRequest(ClientEndpointImpl endpoint, ClientRequest request) throws Exception {
            request.setEndpoint(endpoint);
            initService(request);
            request.setClientEngine(ClientEngineImpl.this);
            final Credentials credentials = endpoint.getCredentials();
            request.setSerializationService(serializationService);
            request.setOperationService(nodeEngine.getOperationService());
            interceptBefore(credentials, request);
            checkPermissions(endpoint, request);
            request.process();
            interceptAfter(credentials, request);
        }

        private void interceptBefore(Credentials credentials, ClientRequest request) {
            final SecurityContext securityContext = getSecurityContext();
            final String methodName = request.getMethodName();
            if (securityContext != null && methodName != null) {
                final String objectType = request.getDistributedObjectType();
                final String objectName = request.getDistributedObjectName();
                securityContext.interceptBefore(credentials, objectType, objectName, methodName, request.getParameters());
            }
        }

        private void interceptAfter(Credentials credentials, ClientRequest request) {
            final SecurityContext securityContext = getSecurityContext();
            final String methodName = request.getMethodName();
            if (securityContext != null && methodName != null) {
                final String objectType = request.getDistributedObjectType();
                final String objectName = request.getDistributedObjectName();
                securityContext.interceptAfter(credentials, objectType, objectName, methodName);
            }
        }

        private void checkPermissions(ClientEndpointImpl endpoint, ClientRequest request) {
            SecurityContext securityContext = getSecurityContext();
            if (securityContext != null) {
                Permission permission = request.getRequiredPermission();
                if (permission != null) {
                    securityContext.checkPermission(endpoint.getSubject(), permission);
                }
            }
        }

        private void initService(ClientRequest request) {
            String serviceName = request.getServiceName();
            if (serviceName == null) {
                return;
            }

            Object service = nodeEngine.getService(serviceName);
            if (service == null) {
                if (nodeEngine.isActive()) {
                    throw new IllegalArgumentException("No service registered with name: " + serviceName);
                }
                throw new HazelcastInstanceNotActiveException();
            }
            request.setService(service);
        }

        private void handleAuthenticationFailure(ClientEndpointImpl endpoint, ClientRequest request) {
            Exception exception;
            if (nodeEngine.isActive()) {
                String message = "Client " + endpoint + " must authenticate before any operation.";
                logger.severe(message);
                exception = new AuthenticationException(message);
            } else {
                exception = new HazelcastInstanceNotActiveException();
            }
            endpoint.sendResponse(exception, request.getCallId());
            endpointManager.removeEndpoint(endpoint);
        }
    }

    private final class ConnectionListenerImpl implements ConnectionListener {

        @Override
        public void connectionAdded(Connection conn) {
            //no-op
            //unfortunately we can't do the endpoint creation here, because this event is only called when the
            //connection is bound, but we need to use the endpoint connection before that.
        }

        @Override
        public void connectionRemoved(Connection connection) {
            if (connection.isClient() && connection instanceof TcpIpConnection && nodeEngine.isActive()) {
                ClientEndpointImpl endpoint = (ClientEndpointImpl) endpointManager.getEndpoint(connection);
                if (endpoint == null) {
                    return;
                }

                if (!endpoint.isFirstConnection()) {
                    return;
                }

                String localMemberUuid = node.getLocalMember().getUuid();
                String ownerUuid = endpoint.getPrincipal().getOwnerUuid();
                if (localMemberUuid.equals(ownerUuid)) {
                    callDisconnectionOperation(endpoint);
                }
            }
        }

        private void callDisconnectionOperation(ClientEndpointImpl endpoint) {
            Collection<MemberImpl> memberList = nodeEngine.getClusterService().getMemberList();
            OperationService operationService = nodeEngine.getOperationService();
            ClientDisconnectionOperation op = createClientDisconnectionOperation(endpoint.getUuid());
            operationService.runOperationOnCallingThread(op);

            for (MemberImpl member : memberList) {
                if (!member.localMember()) {
                    op = createClientDisconnectionOperation(endpoint.getUuid());
                    operationService.send(op, member.getAddress());
                }
            }
        }
    }

    private ClientDisconnectionOperation createClientDisconnectionOperation(String clientUuid) {
        ClientDisconnectionOperation op = new ClientDisconnectionOperation(clientUuid);
        op.setNodeEngine(nodeEngine)
                .setServiceName(SERVICE_NAME)
                .setService(this)
                .setResponseHandler(createEmptyResponseHandler());
        return op;
    }

    private class DestroyEndpointTask implements Runnable {
        private final String deadMemberUuid;

        public DestroyEndpointTask(String deadMemberUuid) {
            this.deadMemberUuid = deadMemberUuid;
        }

        @Override
        public void run() {
            endpointManager.removeEndpoints(deadMemberUuid);
            removeMappings();
        }

        void removeMappings() {
            Iterator<Map.Entry<String, String>> iterator = ownershipMappings.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                String clientUuid = entry.getKey();
                String memberUuid = entry.getValue();
                if (deadMemberUuid.equals(memberUuid)) {
                    iterator.remove();
                    ClientDisconnectionOperation op = createClientDisconnectionOperation(clientUuid);
                    nodeEngine.getOperationService().runOperationOnCallingThread(op);
                }
            }
        }
    }

    @Override
    public Operation getPostJoinOperation() {
        return ownershipMappings.isEmpty() ? null : new PostJoinClientOperation(ownershipMappings);
    }
}
