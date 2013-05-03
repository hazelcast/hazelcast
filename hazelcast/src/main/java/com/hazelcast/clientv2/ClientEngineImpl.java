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

package com.hazelcast.clientv2;

import com.hazelcast.client.ClientDisconnectionOperation;
import com.hazelcast.config.Config;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataAdapter;
import com.hazelcast.nio.serialization.SerializationService;
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
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Level;

/**
 * @mdogan 2/20/13
 */
public class ClientEngineImpl implements ClientEngine, ConnectionListener, CoreService, ManagedService {

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

    public SerializationService getSerializationService() {
        return serializationService;
    }

    public Object invoke(String serviceName, Operation op, Object key)
            throws InterruptedException, ExecutionException, TimeoutException {
        return invoke(serviceName, op, nodeEngine.getPartitionService().getPartitionId(key));
    }

    public Object invoke(String serviceName, Operation op, Data key)
            throws InterruptedException, ExecutionException, TimeoutException {
        return invoke(serviceName, op, nodeEngine.getPartitionService().getPartitionId(key));
    }

    public Object invoke(String serviceName, Operation op, int partitionId)
            throws InterruptedException, ExecutionException, TimeoutException {
        return invoke(serviceName, op, partitionId, 0);
    }

    public Object invoke(String serviceName, Operation op, int partitionId, int replicaIndex)
            throws InterruptedException, ExecutionException, TimeoutException {
        final ClientEndpoint endpoint = getCurrentEndpoint();
        final String uuid = endpoint != null ? endpoint.uuid : null;
        return new ClientInvocation(nodeEngine, op, serviceName, partitionId, replicaIndex, uuid).invoke();
    }

    public Object invoke(String serviceName, Operation op, Address target)
            throws InterruptedException, ExecutionException, TimeoutException {
        final ClientEndpoint endpoint = getCurrentEndpoint();
        final String uuid = endpoint != null ? endpoint.uuid : null;
        return new ClientInvocation(nodeEngine, op, serviceName, target, uuid).invoke();
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

    ClientEndpoint getEndpoint(Connection conn) {
        return ConcurrencyUtil.getOrPutIfAbsent(endpoints, conn, endpointConstructor);
    }

    ClientEndpoint removeEndpoint(final Connection connection) {
        final ClientEndpoint endpoint = endpoints.remove(connection);
        if (endpoint != null) {
            logger.log(Level.INFO, "Destroying " + endpoint);
            try {
                endpoint.destroy();
            } catch (LoginException e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
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
        return endpoint;
    }

    @Override
    public void connectionAdded(Connection connection) {
    }

    public void connectionRemoved(Connection connection) {
        if (connection.isClient() && connection instanceof TcpIpConnection) {
            final ClientEndpoint endpoint = removeEndpoint(connection);
            if (endpoint != null) {
                NodeEngine nodeEngine = node.nodeEngine;
                final Collection<MemberImpl> memberList = nodeEngine.getClusterService().getMemberList();
                for (MemberImpl member : memberList) {
                    final ClientDisconnectionOperation op = new ClientDisconnectionOperation(endpoint.uuid);
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

    public SecurityContext getSecurityContext() {
        return node.securityContext;
    }

    public void bind(Connection connection) {
        if (connection instanceof TcpIpConnection) {
            Address endpoint = new Address(connection.getRemoteSocketAddress());
            TcpIpConnectionManager connectionManager = (TcpIpConnectionManager) node.getConnectionManager();
            connectionManager.bind((TcpIpConnection) connection, endpoint, null, false);
        }
    }

    private final ThreadLocal<ClientEndpoint> currentEndpoint = new ThreadLocal<ClientEndpoint>();

    private ClientEndpoint getCurrentEndpoint() {
        return currentEndpoint.get();
    }

    private class ClientPacketProcessor implements Runnable {
        final ClientPacket packet;

        private ClientPacketProcessor(ClientPacket packet) {
            this.packet = packet;
        }

        public void run() {
            final Connection conn = packet.getConn();
            try {
                final ClientEndpoint endpoint = getEndpoint(conn);
                currentEndpoint.set(endpoint);
                final Data data = packet.getData();
                final ClientRequest request = (ClientRequest) serializationService.toObject(data);
                if (endpoint.isAuthenticated() || request instanceof ClientAuthenticationRequest) {
                    request.setConnection(conn);
                    request.setService(nodeEngine.getService(request.getServiceName()));
                    request.setClientEngine(ClientEngineImpl.this);
                    final Object result = request.process();
                    final Data resultData = result != null ? serializationService.toData(result) : new Data();
                    sendResponse(conn, resultData);
                } else {
                    String message = "Client " + conn + " must authenticate before any operation.";
                    logger.log(Level.SEVERE, message);
                    sendResponse(conn, new GenericError(message, 0));
                    removeEndpoint(conn);
                }
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
                StringWriter w = new StringWriter();
                e.printStackTrace(new PrintWriter(w));
                sendResponse(conn, new GenericError(w.toString(), 0));
            } finally {
                currentEndpoint.set(null);
            }
        }


        private void sendResponse(Connection conn, Object result) {
            final Data resultData = result != null ? serializationService.toData(result) : NULL;
            conn.write(new DataAdapter(resultData, serializationService.getSerializationContext()));
        }
    }

    private static final Data NULL = new Data();

    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        node.getConnectionManager().addConnectionListener(this);
    }

    @Override
    public void reset() {
    }

    public void shutdown() {
        endpoints.clear();
    }

}
