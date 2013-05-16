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
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.UuidUtil;

import javax.security.auth.login.LoginException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Level;

/**
 * @mdogan 2/20/13
 */
public class ClientEngineImpl implements ClientEngine, ConnectionListener, CoreService,
        ManagedService, MembershipAwareService, EventPublishingService<ClientEndpoint, ClientListener> {

    public static final String SERVICE_NAME = "hz:core:clientEngine";

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final Executor executor;
    private final SerializationService serializationService;
    private final ConcurrentMap<Connection, ClientEndpoint> endpoints = new ConcurrentHashMap<Connection, ClientEndpoint>();
    private final ILogger logger;

    public ClientEngineImpl(Node node) {
        this.node = node;
        this.serializationService = node.getSerializationService();
        nodeEngine = node.nodeEngine;
        executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.CLIENT_EXECUTOR);
        logger = node.getLogger(ClientEngine.class);
    }

    public void handlePacket(ClientPacket packet) {
        executor.execute(new ClientPacketProcessor(packet));
    }

    public Object toObject(Data data) {
        return serializationService.toObject(data);
    }

    public Data toData(Object obj) {
        return serializationService.toData(obj);
    }

    public PartitionService getPartitionService() {
        return nodeEngine.getPartitionService();
    }

    public ClusterService getClusterService() {
        return nodeEngine.getClusterService();
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    void sendOperation(Operation op, Address target) {
        nodeEngine.getOperationService().send(op, target);
    }

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, final int partitionId) {
        return nodeEngine.getOperationService().createInvocationBuilder(serviceName, op, partitionId);
    }

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        return nodeEngine.getOperationService().createInvocationBuilder(serviceName, op, target);
    }

    Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory)
            throws Exception {
        return nodeEngine.getOperationService().invokeOnAllPartitions(serviceName, operationFactory);
    }

    Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                            Collection<Integer> partitions) throws Exception {
        return nodeEngine.getOperationService().invokeOnPartitions(serviceName, operationFactory, partitions);
    }

    private static final Data NULL = new Data();

    public void sendResponse(ClientEndpoint endpoint, Object response) {
        if (response instanceof Throwable) {
            Throwable t = (Throwable) response;
            StringWriter s = new StringWriter();
            t.printStackTrace(new PrintWriter(s));
            response = new GenericError(s.toString(), 0);
        }
        final Data resultData = response != null ? serializationService.toData(response) : NULL;
        Connection conn = endpoint.getConnection();
        conn.write(new DataAdapter(resultData, serializationService.getSerializationContext()));
    }

    public Address getMasterAddress() {
        return node.getMasterAddress();
    }

    public Address getThisAddress() {
        return node.getThisAddress();
    }

    public MemberImpl getLocalMember() {
        return node.getLocalMember();
    }

    public Config getConfig() {
        return node.getConfig();
    }

    public ILogger getILogger(Class clazz) {
        return node.getLogger(clazz);
    }

    public ILogger getILogger(String className) {
        return node.getLogger(className);
    }

    private final ConstructorFunction<Connection, ClientEndpoint> endpointConstructor
            = new ConstructorFunction<Connection, ClientEndpoint>() {
        public ClientEndpoint createNew(Connection conn) {
            return new ClientEndpoint(conn, UuidUtil.createClientUuid(conn.getEndPoint()));
        }
    };

    ClientEndpoint getEndpoint(String uuid) {
        for (ClientEndpoint endpoint : endpoints.values()) {
            if (uuid.equals(endpoint.getUuid())) {
                return endpoint;
            }
        }
        return null;
    }

    ClientEndpoint getEndpoint(Connection conn) {
        return ConcurrencyUtil.getOrPutIfAbsent(endpoints, conn, endpointConstructor);
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
            logger.log(Level.INFO, "Destroying " + endpoint);
            try {
                endpoint.destroy();
            } catch (LoginException e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }

            final Connection connection = endpoint.getConnection();
            if (closeImmediately) {
                try {
                    connection.close();
                } catch (Throwable e) {
                    logger.log(Level.WARNING, "While closing client connection: " + e.toString());
                }
            } else {
                nodeEngine.getExecutionService().schedule(new Runnable() {
                    public void run() {
                        if (connection.live()) {
                            try {
                                connection.close();
                            } catch (Throwable e) {
                                logger.log(Level.WARNING, "While closing client connection: " + e.toString());
                            }
                        }
                    }
                }, 1111, TimeUnit.MILLISECONDS);
            }
            sendClientEvent(endpoint);
        }
    }

    public void connectionAdded(Connection connection) {
    }

    public void connectionRemoved(Connection connection) {
        if (connection.isClient() && connection instanceof TcpIpConnection) {
            final ClientEndpoint endpoint = endpoints.get(connection);
            if (endpoint != null && node.getLocalMember().getUuid().equals(endpoint.getPrincipal().getOwnerUuid())) {
                removeEndpoint(connection, true);
                NodeEngine nodeEngine = node.nodeEngine;
                final Collection<MemberImpl> memberList = nodeEngine.getClusterService().getMemberList();
                for (MemberImpl member : memberList) {
                    final ClientDisconnectionOperation op = new ClientDisconnectionOperation(endpoint.getUuid());
                    op.setNodeEngine(nodeEngine).setServiceName(SERVICE_NAME).setService(this)
                            .setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());

                    if (member.localMember()) {
                        nodeEngine.getOperationService().runOperation(op);
                    } else {
                        nodeEngine.getOperationService().send(op, member.getAddress());
                    }
                }
            }
        }
    }

    SecurityContext getSecurityContext() {
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
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> regs = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        eventService.publishEvent(SERVICE_NAME, regs, endpoint);
    }

    public void dispatchEvent(ClientEndpoint event, ClientListener listener) {
        if (event.isAuthenticated()) {
            listener.clientConnected(event);
        } else {
            listener.clientDisconnected(event);
        }
    }

    public void memberAdded(MembershipServiceEvent event) {
    }

    public void memberRemoved(MembershipServiceEvent event) {
        final String uuid = event.getMember().getUuid();
        nodeEngine.getExecutionService().schedule(new Runnable() {
            public void run() {
                final Iterator<ClientEndpoint> iter = endpoints.values().iterator();
                while (iter.hasNext()) {
                    final ClientEndpoint endpoint = iter.next();
                    if (uuid.equals(endpoint.getPrincipal().getOwnerUuid())) {
                        iter.remove();
                        destroyEndpoint(endpoint, true);
                    }
                }
            }
        }, 10, TimeUnit.SECONDS);
    }

    String addClientListener(ClientListener clientListener) {
        final EventRegistration registration = nodeEngine.getEventService().registerLocalListener(SERVICE_NAME, SERVICE_NAME, clientListener);
        return registration.getId();
    }

    boolean removeClientListener(String registrationId) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, SERVICE_NAME, registrationId);
    }

    public ClientService getClientService() {
        return new ClientServiceProxy(this);
    }

    private class ClientPacketProcessor implements Runnable {
        final ClientPacket packet;

        private ClientPacketProcessor(ClientPacket packet) {
            this.packet = packet;
        }

        public void run() {
            final Connection conn = packet.getConn();
            final ClientEndpoint endpoint = getEndpoint(conn);
            try {
                final Data data = packet.getData();
                final ClientRequest request = (ClientRequest) serializationService.toObject(data);
                if (endpoint.isAuthenticated() || request instanceof AuthenticationRequest) {
                    request.setEndpoint(endpoint);
                    if (request.getServiceName() != null) {
                        final Object service = nodeEngine.getService(request.getServiceName());
                        if (service == null) {
                            throw new IllegalArgumentException("No service registered with name: " + request.getServiceName());
                        }
                        request.setService(service);
                    }
                    request.setClientEngine(ClientEngineImpl.this);
                    request.process();
                } else {
                    String message = "Client " + conn + " must authenticate before any operation.";
                    logger.log(Level.SEVERE, message);
                    sendResponse(endpoint, new GenericError(message, 0));
                    removeEndpoint(conn);
                }
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
                sendResponse(endpoint, e);
            }
        }
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(ClientPortableHook.ID, ClientPortableHook.PRINCIPAL);
        builder.addUTFField("uuid").addUTFField("ownerUuid");
        serializationService.getSerializationContext().registerClassDefinition(builder.build());
        node.getConnectionManager().addConnectionListener(this);
    }

    @Override
    public void reset() {
    }

    public void shutdown() {
        for (ClientEndpoint endpoint : endpoints.values()) {
            try {
                endpoint.destroy();
            } catch (LoginException e) {
                logger.log(Level.FINEST, e.getMessage());
            }
            try {
                final Connection conn = endpoint.getConnection();
                if (conn.live()) {
                    conn.close( );
                }
            } catch (Exception e) {
                logger.log(Level.FINEST, e.getMessage(), e);
            }
        }
        endpoints.clear();
    }

}
