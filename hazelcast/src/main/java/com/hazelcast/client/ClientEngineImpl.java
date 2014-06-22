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

package com.hazelcast.client;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClientPacket;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataAdapter;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.ExecutorType;

import javax.security.auth.login.LoginException;
import java.security.Permission;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.spi.impl.ResponseHandlerFactory.createEmptyResponseHandler;

public class ClientEngineImpl implements ClientEngine, CoreService,
        ManagedService, MembershipAwareService, EventPublishingService<ClientEndpoint, ClientListener> {

    public static final String SERVICE_NAME = "hz:core:clientEngine";
    public static final int DESTROY_ENDPOINT_DELAY_MS = 1111;
    public static final int ENDPOINT_REMOVE_DELAY_MS = 10;
    public static final int THREADS_PER_CORE = 10;
    public static final int RIDICULOUS_THREADS_PER_CORE = 100000;

    static final Data NULL = new Data();

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final Executor executor;
    private final SerializationService serializationService;
    private final ConcurrentMap<Connection, ClientEndpoint> endpoints =
            new ConcurrentHashMap<Connection, ClientEndpoint>();
    private final ILogger logger;
    private final ConnectionListener connectionListener = new ConnectionListenerImpl();

    public ClientEngineImpl(Node node) {
        this.node = node;
        this.serializationService = node.getSerializationService();
        this.nodeEngine = node.nodeEngine;
        int coreSize = Runtime.getRuntime().availableProcessors();
        this.executor = nodeEngine.getExecutionService().register(ExecutionService.CLIENT_EXECUTOR,
                coreSize * THREADS_PER_CORE, coreSize * RIDICULOUS_THREADS_PER_CORE,
                ExecutorType.CONCRETE);
        this.logger = node.getLogger(ClientEngine.class);
    }

    //needed for testing purposes
    public ConnectionListener getConnectionListener() {
        return connectionListener;
    }

    @Override
    public int getClientEndpointCount() {
        return endpoints.size();
    }

    public void handlePacket(ClientPacket packet) {
        executor.execute(new ClientPacketProcessor(packet));
    }

    @Override
    public Object toObject(Data data) {
        return serializationService.toObject(data);
    }

    @Override
    public Data toData(Object obj) {
        return serializationService.toData(obj);
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
    public SerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public EventService getEventService() {
        return nodeEngine.getEventService();
    }

    @Override
    public ProxyService getProxyService() {
        return nodeEngine.getProxyService();
    }

    void sendOperation(Operation op, Address target) {
        getOperationService().send(op, target);
    }

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, final int partitionId) {
        return getOperationService().createInvocationBuilder(serviceName, op, partitionId);
    }

    private OperationService getOperationService() {
        return nodeEngine.getOperationService();
    }

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        return getOperationService().createInvocationBuilder(serviceName, op, target);
    }

    Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory)
            throws Exception {
        return getOperationService().invokeOnAllPartitions(serviceName, operationFactory);
    }

    Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                            Collection<Integer> partitions) throws Exception {
        return getOperationService().invokeOnPartitions(serviceName, operationFactory, partitions);
    }

    void sendResponse(ClientEndpoint endpoint, ClientResponse response) {
        Data resultData = serializationService.toData(response);
        Connection conn = endpoint.getConnection();
        conn.write(new DataAdapter(resultData, serializationService.getPortableContext()));
    }

    @Override
    public TransactionManagerService getTransactionManagerService() {
        return nodeEngine.getTransactionManagerService();
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

    @Override
    public ILogger getLogger(String className) {
        return node.getLogger(className);
    }

    Set<ClientEndpoint> getEndpoints(String uuid) {
        Set<ClientEndpoint> endpointSet = new HashSet<ClientEndpoint>();
        for (ClientEndpoint endpoint : endpoints.values()) {
            if (uuid.equals(endpoint.getUuid())) {
                endpointSet.add(endpoint);
            }
        }
        return endpointSet;
    }

    ClientEndpoint getEndpoint(Connection conn) {
        return endpoints.get(conn);
    }

    ClientEndpoint createEndpoint(Connection conn) {
        if (!conn.live()) {
            logger.severe("Can't create and endpoint for a dead connection");
            return null;
        }

        String clientUuid = UuidUtil.createClientUuid(conn.getEndPoint());
        ClientEndpoint endpoint = new ClientEndpoint(ClientEngineImpl.this, conn, clientUuid);
        if (endpoints.putIfAbsent(conn, endpoint) != null) {
            logger.severe("An endpoint already exists for connection:" + conn);
        }
        return endpoint;
    }

    ClientEndpoint removeEndpoint(final Connection connection) {
        return removeEndpoint(connection, false);
    }

    ClientEndpoint removeEndpoint(final Connection connection, boolean closeImmediately) {
        final ClientEndpoint endpoint = endpoints.remove(connection);
        destroyEndpoint(endpoint, closeImmediately);
        return endpoint;
    }

    private void destroyEndpoint(ClientEndpoint endpoint, boolean closeImmediately) {
        if (endpoint != null) {
            logger.info("Destroying " + endpoint);
            try {
                endpoint.destroy();
            } catch (LoginException e) {
                logger.warning(e);
            }

            final Connection connection = endpoint.getConnection();
            if (closeImmediately) {
                try {
                    connection.close();
                } catch (Throwable e) {
                    logger.warning("While closing client connection: " + connection, e);
                }
            } else {
                nodeEngine.getExecutionService().schedule(new Runnable() {
                    public void run() {
                        if (connection.live()) {
                            try {
                                connection.close();
                            } catch (Throwable e) {
                                logger.warning("While closing client connection: " + e.toString());
                            }
                        }
                    }
                }, DESTROY_ENDPOINT_DELAY_MS, TimeUnit.MILLISECONDS);
            }
            sendClientEvent(endpoint);
        }
    }

    @Override
    public SecurityContext getSecurityContext() {
        return node.securityContext;
    }

    void bind(final ClientEndpoint endpoint) {
        final Connection conn = endpoint.getConnection();
        if (conn instanceof TcpIpConnection) {
            Address address = new Address(conn.getRemoteSocketAddress());
            TcpIpConnectionManager connectionManager = (TcpIpConnectionManager) node.getConnectionManager();
            connectionManager.bind((TcpIpConnection) conn, address, null, false);
        }
        sendClientEvent(endpoint);
    }

    private void sendClientEvent(ClientEndpoint endpoint) {
        if (!endpoint.isFirstConnection()) {
            final EventService eventService = nodeEngine.getEventService();
            final Collection<EventRegistration> regs = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
            eventService.publishEvent(SERVICE_NAME, regs, endpoint, endpoint.getUuid().hashCode());
        }
    }

    @Override
    public void dispatchEvent(ClientEndpoint event, ClientListener listener) {
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

        final String uuid = event.getMember().getUuid();
        try {
            nodeEngine.getExecutionService().schedule(new Runnable() {
                @Override
                public void run() {
                    Iterator<ClientEndpoint> iterator = endpoints.values().iterator();
                    while (iterator.hasNext()) {
                        ClientEndpoint endpoint = iterator.next();
                        String ownerUuid = endpoint.getPrincipal().getOwnerUuid();
                        if (uuid.equals(ownerUuid)) {
                            iterator.remove();
                            destroyEndpoint(endpoint, true);
                        }
                    }
                }
            }, ENDPOINT_REMOVE_DELAY_MS, TimeUnit.SECONDS);
        } catch (RejectedExecutionException e) {
            if (logger.isFinestEnabled()) {
                logger.finest(e);
            }
        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    String addClientListener(ClientListener clientListener) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService
                .registerLocalListener(SERVICE_NAME, SERVICE_NAME, clientListener);
        return registration.getId();
    }

    boolean removeClientListener(String registrationId) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, SERVICE_NAME, registrationId);
    }

    public ClientService getClientService() {
        return new ClientServiceProxy(this);
    }

    public Collection<Client> getClients() {
        final HashSet<Client> clients = new HashSet<Client>();
        for (ClientEndpoint endpoint : endpoints.values()) {
            if (!endpoint.isFirstConnection()) {
                clients.add(endpoint);
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
        for (ClientEndpoint endpoint : endpoints.values()) {
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
        endpoints.clear();
    }

    private final class ClientPacketProcessor implements Runnable {
        final ClientPacket packet;

        private ClientPacketProcessor(ClientPacket packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            Connection conn = packet.getConn();
            ClientEndpoint endpoint = getEndpoint(conn);
            ClientRequest request = null;
            try {
                request = loadRequest();
                if (request == null) {
                    handlePacketWithNullRequest();
                } else if (request instanceof AuthenticationRequest) {
                    endpoint = createEndpoint(conn);
                    if (endpoint != null) {
                        processRequest(endpoint, request);
                    } else {
                        handleEndpointNotCreatedConnectionNotAlive();
                    }
                } else if (endpoint == null) {
                    handleMissingEndpoint(conn);
                } else if (endpoint.isAuthenticated()) {
                    processRequest(endpoint, request);
                } else {
                    handleAuthenticationFailure(conn, endpoint, request);
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

        private void handleProcessingFailure(ClientEndpoint endpoint, ClientRequest request, Throwable e) {
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

        private void processRequest(ClientEndpoint endpoint, ClientRequest request) throws Exception {
            request.setEndpoint(endpoint);
            initService(request);
            request.setClientEngine(ClientEngineImpl.this);
            final Credentials credentials = endpoint.getCredentials();
            interceptBefore(credentials, request);
            checkPermissions(endpoint, request);
            request.process();
            interceptAfter(credentials, request);
        }

        private void interceptBefore(Credentials credentials, ClientRequest request) {
            final SecurityContext securityContext = getSecurityContext();
            final String methodName = request.getMethodName();
            if (securityContext != null && methodName != null) {
                final String distributedObjectType = request.getDistributedObjectType();
                securityContext.interceptBefore(credentials, distributedObjectType, methodName, request.getParameters());
            }
        }

        private void interceptAfter(Credentials credentials, ClientRequest request) {
            final SecurityContext securityContext = getSecurityContext();
            final String methodName = request.getMethodName();
            if (securityContext != null && methodName != null) {
                final String distributedObjectType = request.getDistributedObjectType();
                securityContext.interceptAfter(credentials, distributedObjectType, methodName);
            }
        }

        private void checkPermissions(ClientEndpoint endpoint, ClientRequest request) {
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

        private void handleAuthenticationFailure(Connection conn, ClientEndpoint endpoint, ClientRequest request) {
            Exception exception;
            if (nodeEngine.isActive()) {
                String message = "Client " + conn + " must authenticate before any operation.";
                logger.severe(message);
                exception = new AuthenticationException(message);
            } else {
                exception = new HazelcastInstanceNotActiveException();
            }
            endpoint.sendResponse(exception, request.getCallId());
            removeEndpoint(conn);
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
                ClientEndpoint endpoint = endpoints.get(connection);
                if (endpoint == null) {
                    return;
                }

                String localMemberUuid = node.getLocalMember().getUuid();
                String ownerUuid = endpoint.getPrincipal().getOwnerUuid();
                if (localMemberUuid.equals(ownerUuid)) {
                    doRemoveEndpoint(connection, endpoint);
                }
            }
        }

        private void doRemoveEndpoint(Connection connection, ClientEndpoint endpoint) {
            removeEndpoint(connection, true);
            if (!endpoint.isFirstConnection()) {
                return;
            }

            NodeEngine nodeEngine = node.nodeEngine;
            Collection<MemberImpl> memberList = nodeEngine.getClusterService().getMemberList();
            OperationService operationService = nodeEngine.getOperationService();
            for (MemberImpl member : memberList) {
                ClientDisconnectionOperation op = new ClientDisconnectionOperation(endpoint.getUuid());
                op.setNodeEngine(nodeEngine)
                        .setServiceName(SERVICE_NAME)
                        .setService(ClientEngineImpl.this)
                        .setResponseHandler(createEmptyResponseHandler());

                if (member.localMember()) {
                    operationService.runOperationOnCallingThread(op);
                } else {
                    operationService.send(op, member.getAddress());
                }
            }
        }
    }

}
