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

package com.hazelcast.client.impl;

import com.hazelcast.cache.impl.JCacheDetector;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEndpointManager;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.ClientEvent;
import com.hazelcast.client.ClientEventType;
import com.hazelcast.client.impl.operations.ClientDisconnectionOperation;
import com.hazelcast.client.impl.operations.GetConnectedClientsOperation;
import com.hazelcast.client.impl.operations.PostJoinClientOperation;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask;
import com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask;
import com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.task.PingMessageTask;
import com.hazelcast.config.Config;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientType;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.ClientAwareService;
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
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.util.executor.ExecutorType;

import javax.security.auth.login.LoginException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;

/**
 * Class that requests, listeners from client handled in node side.
 */
public class ClientEngineImpl implements ClientEngine, CoreService, PostJoinAwareService,
        ManagedService, MembershipAwareService, EventPublishingService<ClientEvent, ClientListener> {

    /**
     * Service name to be used in requests.
     */
    public static final String SERVICE_NAME = "hz:core:clientEngine";

    public static final int ENDPOINT_REMOVE_DELAY_SECONDS = 10;

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

    private final MessageTaskFactory messageTaskFactory;
    private final ClientExceptionFactory clientExceptionFactory;

    public ClientEngineImpl(Node node) {
        this.logger = node.getLogger(ClientEngine.class);
        this.node = node;
        this.serializationService = node.getSerializationService();
        this.nodeEngine = node.nodeEngine;
        this.endpointManager = new ClientEndpointManagerImpl(this, nodeEngine);
        this.executor = newExecutor();
        this.messageTaskFactory = new CompositeMessageTaskFactory(this.nodeEngine);
        this.clientExceptionFactory = initClientExceptionFactory();

        ClientHeartbeatMonitor heartbeatMonitor = new ClientHeartbeatMonitor(
                endpointManager, this, nodeEngine.getExecutionService(), node.getProperties());
        heartbeatMonitor.start();
    }

    private ClientExceptionFactory initClientExceptionFactory() {
        boolean jcacheAvailable = JCacheDetector.isJcacheAvailable(nodeEngine.getConfigClassLoader());
        return new ClientExceptionFactory(jcacheAvailable);
    }

    private Executor newExecutor() {
        final ExecutionService executionService = nodeEngine.getExecutionService();
        int coreSize = Runtime.getRuntime().availableProcessors();

        int threadCount = node.getProperties().getInteger(GroupProperty.CLIENT_ENGINE_THREAD_COUNT);
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
    public SerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public int getClientEndpointCount() {
        return endpointManager.size();
    }

    public void handleClientMessage(ClientMessage clientMessage, Connection connection) {
        int partitionId = clientMessage.getPartitionId();
        MessageTask messageTask = messageTaskFactory.create(clientMessage, connection);
        InternalOperationService operationService = nodeEngine.getOperationService();
        if (partitionId < 0) {
            if (isUrgent(messageTask)) {
                operationService.execute(new PriorityPartitionSpecificRunnable(messageTask));
            } else {
                executor.execute(messageTask);
            }
        } else {
            operationService.execute(messageTask);
        }
    }

    private boolean isUrgent(MessageTask messageTask) {
        Class clazz = messageTask.getClass();
        return clazz == PingMessageTask.class
                || clazz == GetPartitionsMessageTask.class
                || clazz == AuthenticationMessageTask.class
                || clazz == AuthenticationCustomCredentialsMessageTask.class
                ;
    }

    @Override
    public IPartitionService getPartitionService() {
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

    public ClientExceptionFactory getClientExceptionFactory() {
        return clientExceptionFactory;
    }

    @Override
    public SecurityContext getSecurityContext() {
        return node.securityContext;
    }

    public void bind(final ClientEndpoint endpoint) {
        final Connection conn = endpoint.getConnection();
        if (conn instanceof TcpIpConnection) {
            Address address = new Address(conn.getRemoteSocketAddress());
            ((TcpIpConnection) conn).setEndPoint(address);
        }
        ClientEvent event = new ClientEvent(endpoint.getUuid(),
                ClientEventType.CONNECTED,
                endpoint.getSocketAddress(),
                endpoint.getClientType());
        sendClientEvent(event);
    }

    void sendClientEvent(ClientEvent event) {
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> regs = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        String uuid = event.getUuid();
        eventService.publishEvent(SERVICE_NAME, regs, event, uuid.hashCode());
    }

    @Override
    public void dispatchEvent(ClientEvent event, ClientListener listener) {
        if (event.getEventType() == ClientEventType.CONNECTED) {
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
                    ENDPOINT_REMOVE_DELAY_SECONDS, TimeUnit.SECONDS);
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
            clients.add(endpoint);
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
                if (conn.isAlive()) {
                    conn.close("Shutdown of ClientEngine", null);
                }
            } catch (Exception e) {
                logger.finest(e);
            }
        }
        endpointManager.clear();
        ownershipMappings.clear();
    }

    public String addOwnershipMapping(String clientUuid, String ownerUuid) {
        return ownershipMappings.put(clientUuid, ownerUuid);
    }

    public void removeClient(String clientUuid) {
         ownershipMappings.remove(clientUuid);

        Set<ClientEndpoint> endpoints = endpointManager.getEndpoints(clientUuid);
        for (ClientEndpoint endpoint : endpoints) {
            endpointManager.removeEndpoint(endpoint, true, "Resources are being cleaned up for client " + clientUuid);
        }

        NodeEngineImpl nodeEngine = node.getNodeEngine();
        nodeEngine.onClientDisconnected(clientUuid);
        Collection<ClientAwareService> services = nodeEngine.getServices(ClientAwareService.class);
        for (ClientAwareService service : services) {
            service.clientDisconnected(clientUuid);
        }
    }

    //implemented for test purpose
    public String getOwnerUuid(String clientUuid) {
        return ownershipMappings.get(clientUuid);
    }

    public TransactionManagerService getTransactionManagerService() {
        return node.nodeEngine.getTransactionManagerService();
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
            if (connection.isClient() && nodeEngine.isRunning()) {
                final ClientEndpointImpl endpoint = (ClientEndpointImpl) endpointManager.getEndpoint(connection);
                if (endpoint == null) {
                    logger.finest("connectionRemoved: No endpoint for connection:" + connection);
                    return;
                }

                if (!endpoint.isFirstConnection()) {
                    logger.finest("connectionRemoved: Not the owner conn:" + connection + " for endpoint " + endpoint);
                    return;
                }

                String localMemberUuid = node.getLocalMember().getUuid();
                String ownerUuid = endpoint.getPrincipal().getOwnerUuid();
                if (localMemberUuid.equals(ownerUuid)) {
                    try {
                        nodeEngine.getExecutionService().schedule(new Runnable() {
                            @Override
                            public void run() {
                                callDisconnectionOperation(endpoint);
                            }
                        }, ENDPOINT_REMOVE_DELAY_SECONDS, TimeUnit.SECONDS);
                    } catch (RejectedExecutionException e) {
                        if (logger.isFinestEnabled()) {
                            logger.finest(e);
                        }
                    }
                }
            }
        }

        private void callDisconnectionOperation(ClientEndpointImpl endpoint) {
            Collection<Member> memberList = nodeEngine.getClusterService().getMembers();
            OperationService operationService = nodeEngine.getOperationService();
            String memberUuid = getLocalMember().getUuid();
            String clientUuid = endpoint.getUuid();

            String ownerMember = ownershipMappings.get(clientUuid);
            if (!memberUuid.equals(ownerMember)) {
                // do nothing if the owner already changed (double checked locking)
                return;
            }

            ClientDisconnectionOperation op = createClientDisconnectionOperation(clientUuid);
            operationService.run(op);

            for (Member member : memberList) {
                if (!member.localMember()) {
                    op = createClientDisconnectionOperation(clientUuid);
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
                .setOperationResponseHandler(createEmptyResponseHandler());
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
                    nodeEngine.getOperationService().run(op);
                }
            }
        }
    }

    @Override
    public Operation getPostJoinOperation() {
        return ownershipMappings.isEmpty() ? null : new PostJoinClientOperation(ownershipMappings);
    }

    @Override
    public Map<ClientType, Integer> getConnectedClientStats() {

        int numberOfCppClients = 0;
        int numberOfDotNetClients = 0;
        int numberOfJavaClients = 0;
        int numberOfNodeJSClients = 0;
        int numberOfPythonClients = 0;
        int numberOfOtherClients = 0;

        OperationService operationService = node.nodeEngine.getOperationService();
        Map<ClientType, Integer> resultMap = new HashMap<ClientType, Integer>();
        Map<String, ClientType> clientsMap = new HashMap<String, ClientType>();

        for (Member member : node.getClusterService().getMembers()) {
            Address target = member.getAddress();
            Operation clientInfoOperation = new GetConnectedClientsOperation();
            Future<Map<String, ClientType>> future
                    = operationService.invokeOnTarget(SERVICE_NAME, clientInfoOperation, target);
            try {
                Map<String, ClientType> endpoints = future.get();
                if (endpoints == null) {
                    continue;
                }
                //Merge connected clients according to their uuid.
                for (Map.Entry<String, ClientType> entry : endpoints.entrySet()) {
                    clientsMap.put(entry.getKey(), entry.getValue());
                }
            } catch (Exception e) {
                logger.warning("Cannot get client information from: " + target.toString(), e);
            }
        }

        //Now we are regrouping according to the client type
        for (ClientType clientType : clientsMap.values()) {
            switch (clientType) {
                case JAVA:
                    numberOfJavaClients++;
                    break;
                case CSHARP:
                    numberOfDotNetClients++;
                    break;
                case CPP:
                    numberOfCppClients++;
                    break;
                case NODEJS:
                    numberOfNodeJSClients++;
                    break;
                case PYTHON:
                    numberOfPythonClients++;
                    break;
                default:
                    numberOfOtherClients++;
            }
        }

        resultMap.put(ClientType.CPP, numberOfCppClients);
        resultMap.put(ClientType.CSHARP, numberOfDotNetClients);
        resultMap.put(ClientType.JAVA, numberOfJavaClients);
        resultMap.put(ClientType.NODEJS, numberOfNodeJSClients);
        resultMap.put(ClientType.PYTHON, numberOfPythonClients);
        resultMap.put(ClientType.OTHER, numberOfOtherClients);

        return resultMap;
    }

    private static class PriorityPartitionSpecificRunnable implements PartitionSpecificRunnable, UrgentSystemOperation {

        private final MessageTask task;

        public PriorityPartitionSpecificRunnable(MessageTask task) {
            this.task = task;
        }

        @Override
        public void run() {
            task.run();
        }

        @Override
        public int getPartitionId() {
            return task.getPartitionId();
        }

        @Override
        public String toString() {
            return "PriorityPartitionSpecificRunnable:{ " + task + "}";
        }
    }
}
