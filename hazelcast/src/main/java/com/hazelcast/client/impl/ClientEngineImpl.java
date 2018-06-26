/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.operations.ClientDisconnectionOperation;
import com.hazelcast.client.impl.operations.GetConnectedClientsOperation;
import com.hazelcast.client.impl.operations.OnJoinClientOperation;
import com.hazelcast.client.impl.protocol.ClientExceptions;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask;
import com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask;
import com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.task.PingMessageTask;
import com.hazelcast.client.impl.protocol.task.map.AbstractMapQueryMessageTask;
import com.hazelcast.config.Config;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientType;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PreJoinAwareService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.ExecutorType;

import javax.security.auth.login.LoginException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.ExecutionService.CLIENT_MANAGEMENT_EXECUTOR;
import static com.hazelcast.util.SetUtil.createHashSet;

/**
 * Class that requests, listeners from client handled in node side.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class ClientEngineImpl implements ClientEngine, CoreService, PreJoinAwareService,
        ManagedService, MembershipAwareService, EventPublishingService<ClientEvent, ClientListener> {

    /**
     * Service name to be used in requests.
     */
    public static final String SERVICE_NAME = "hz:core:clientEngine";

    private static final int EXECUTOR_QUEUE_CAPACITY_PER_CORE = 100000;
    private static final int THREADS_PER_CORE = 20;
    private static final int QUERY_THREADS_PER_CORE = 1;
    private static final ConstructorFunction<String, AtomicLong> LAST_AUTH_CORRELATION_ID_CONSTRUCTOR_FUNC =
            new ConstructorFunction<String, AtomicLong>() {
                @Override
                public AtomicLong createNew(String arg) {
                    return new AtomicLong();
                }
            };
    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final Executor executor;
    private final ExecutorService clientManagementExecutor;
    private final Executor queryExecutor;

    private final SerializationService serializationService;
    // client UUID -> member UUID
    private final ConcurrentMap<String, String> ownershipMappings = new ConcurrentHashMap<String, String>();
    // client UUID -> last authentication correlation ID
    private final ConcurrentMap<String, AtomicLong> lastAuthenticationCorrelationIds
            = new ConcurrentHashMap<String, AtomicLong>();

    private final ClientEndpointManagerImpl endpointManager;
    private final ILogger logger;
    private final ConnectionListener connectionListener = new ConnectionListenerImpl();

    private final MessageTaskFactory messageTaskFactory;
    private final ClientExceptions clientExceptions;
    private final int endpointRemoveDelaySeconds;
    private final ClientPartitionListenerService partitionListenerService;

    public ClientEngineImpl(Node node) {
        this.logger = node.getLogger(ClientEngine.class);
        this.node = node;
        this.serializationService = node.getSerializationService();
        this.nodeEngine = node.nodeEngine;
        this.endpointManager = new ClientEndpointManagerImpl(nodeEngine);
        this.executor = newClientExecutor();
        this.queryExecutor = newClientQueryExecutor();
        this.clientManagementExecutor = newClientsManagementExecutor();
        this.messageTaskFactory = new CompositeMessageTaskFactory(nodeEngine);
        this.clientExceptions = initClientExceptionFactory();
        this.endpointRemoveDelaySeconds = node.getProperties().getInteger(GroupProperty.CLIENT_ENDPOINT_REMOVE_DELAY_SECONDS);
        this.partitionListenerService = new ClientPartitionListenerService(nodeEngine);
    }

    private ClientExceptions initClientExceptionFactory() {
        boolean jcacheAvailable = JCacheDetector.isJCacheAvailable(nodeEngine.getConfigClassLoader());
        return new ClientExceptions(jcacheAvailable);
    }

    private ExecutorService newClientsManagementExecutor() {
        //CLIENT_MANAGEMENT_EXECUTOR is a single threaded executor to ensure that disconnect/auth are executed in correct order.
        InternalExecutionService executionService = nodeEngine.getExecutionService();
        return executionService.register(CLIENT_MANAGEMENT_EXECUTOR, 1, Integer.MAX_VALUE, ExecutorType.CACHED);
    }

    public ExecutorService getClientManagementExecutor() {
        return clientManagementExecutor;
    }

    private Executor newClientExecutor() {
        final ExecutionService executionService = nodeEngine.getExecutionService();
        int coreSize = RuntimeAvailableProcessors.get();

        int threadCount = node.getProperties().getInteger(GroupProperty.CLIENT_ENGINE_THREAD_COUNT);
        if (threadCount <= 0) {
            threadCount = coreSize * THREADS_PER_CORE;
        }
        logger.finest("Creating new client executor with threadCount=" + threadCount);

        return executionService.register(ExecutionService.CLIENT_EXECUTOR,
                threadCount, coreSize * EXECUTOR_QUEUE_CAPACITY_PER_CORE,
                ExecutorType.CONCRETE);
    }

    private Executor newClientQueryExecutor() {
        final ExecutionService executionService = nodeEngine.getExecutionService();
        int coreSize = RuntimeAvailableProcessors.get();

        int threadCount = node.getProperties().getInteger(GroupProperty.CLIENT_ENGINE_QUERY_THREAD_COUNT);
        if (threadCount <= 0) {
            threadCount = coreSize * QUERY_THREADS_PER_CORE;
        }
        logger.finest("Creating new client query executor with threadCount=" + threadCount);

        return executionService.register(ExecutionService.CLIENT_QUERY_EXECUTOR,
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

    @Override
    public void accept(ClientMessage clientMessage) {
        int partitionId = clientMessage.getPartitionId();
        Connection connection = clientMessage.getConnection();
        MessageTask messageTask = messageTaskFactory.create(clientMessage, connection);
        InternalOperationService operationService = nodeEngine.getOperationService();
        if (partitionId < 0) {
            if (isUrgent(messageTask)) {
                operationService.execute(new PriorityPartitionSpecificRunnable(messageTask));
            } else if (isQuery(messageTask)) {
                queryExecutor.execute(messageTask);
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

    private boolean isQuery(MessageTask messageTask) {
        return messageTask instanceof AbstractMapQueryMessageTask;
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
    public String getThisUuid() {
        return node.getThisUuid();
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

    public ClientExceptions getClientExceptions() {
        return clientExceptions;
    }

    @Override
    public SecurityContext getSecurityContext() {
        return node.securityContext;
    }

    public void bind(final ClientEndpoint endpoint) {
        final Connection conn = endpoint.getConnection();
        if (conn instanceof TcpIpConnection) {
            InetSocketAddress socketAddress = conn.getRemoteSocketAddress();
            //socket address can be null if connection closed before bind
            if (socketAddress != null) {
                Address address = new Address(socketAddress);
                ((TcpIpConnection) conn).setEndPoint(address);
            }
        }
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
                    endpointRemoveDelaySeconds, TimeUnit.SECONDS);
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
        final Collection<ClientEndpoint> endpoints = endpointManager.getEndpoints();
        final Set<Client> clients = createHashSet(endpoints.size());
        for (ClientEndpoint endpoint : endpoints) {
            clients.add(endpoint);
        }
        return clients;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        node.getConnectionManager().addConnectionListener(connectionListener);

        ClientHeartbeatMonitor heartbeatMonitor = new ClientHeartbeatMonitor(
                endpointManager, getLogger(ClientHeartbeatMonitor.class), nodeEngine.getExecutionService(), node.getProperties());
        heartbeatMonitor.start();
    }

    @Override
    public void reset() {
        clear("Resetting clientEngine");
    }

    @Override
    public void shutdown(boolean terminate) {
        clear("Shutting down clientEngine");
    }

    private void clear(String reason) {
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
                    conn.close(reason, null);
                }
            } catch (Exception e) {
                logger.finest(e);
            }
        }
        endpointManager.clear();
        ownershipMappings.clear();
    }

    public boolean trySetLastAuthenticationCorrelationId(String clientUuid, long newCorrelationId) {
        AtomicLong lastCorrelationId = ConcurrencyUtil.getOrPutIfAbsent(lastAuthenticationCorrelationIds,
                clientUuid,
                LAST_AUTH_CORRELATION_ID_CONSTRUCTOR_FUNC);
        return ConcurrencyUtil.setIfGreaterThan(lastCorrelationId, newCorrelationId);
    }

    public String addOwnershipMapping(String clientUuid, String ownerUuid) {
        return ownershipMappings.put(clientUuid, ownerUuid);
    }

    public boolean removeOwnershipMapping(String clientUuid, String memberUuid) {
        lastAuthenticationCorrelationIds.remove(clientUuid);
        return ownershipMappings.remove(clientUuid, memberUuid);
    }

    public String getOwnerUuid(String clientUuid) {
        return ownershipMappings.get(clientUuid);
    }

    public TransactionManagerService getTransactionManagerService() {
        return node.nodeEngine.getTransactionManagerService();
    }

    public ClientPartitionListenerService getPartitionListenerService() {
        return partitionListenerService;
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
            if (!connection.isClient() || !nodeEngine.isRunning()) {
                return;
            }
            final ClientEndpointImpl endpoint = (ClientEndpointImpl) endpointManager.getEndpoint(connection);
            if (endpoint == null) {
                logger.finest("connectionRemoved: No endpoint for connection:" + connection);
                return;
            }

            endpointManager.removeEndpoint(endpoint);

            if (!endpoint.isOwnerConnection()) {
                logger.finest("connectionRemoved: Not the owner conn:" + connection + " for endpoint " + endpoint);
                return;
            }

            String localMemberUuid = node.getThisUuid();
            String ownerUuid = ownershipMappings.get(endpoint.getUuid());
            if (localMemberUuid.equals(ownerUuid)) {
                try {
                    nodeEngine.getExecutionService().schedule(new Runnable() {
                        @Override
                        public void run() {
                            callDisconnectionOperation(endpoint);
                        }
                    }, endpointRemoveDelaySeconds, TimeUnit.SECONDS);
                } catch (RejectedExecutionException e) {
                    if (logger.isFinestEnabled()) {
                        logger.finest(e);
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

            if (lastAuthenticationCorrelationIds.get(clientUuid).get() > endpoint.getAuthenticationCorrelationId()) {
                //a new authentication already made for that client. This check is needed to detect
                // "a disconnected client is reconnected back to same node"
                return;
            }

            for (Member member : memberList) {
                ClientDisconnectionOperation op = new ClientDisconnectionOperation(clientUuid, memberUuid);
                operationService.createInvocationBuilder(SERVICE_NAME, op, member.getAddress()).invoke();
            }
        }
    }

    private class DestroyEndpointTask implements Runnable {
        private final String deadMemberUuid;

        DestroyEndpointTask(String deadMemberUuid) {
            this.deadMemberUuid = deadMemberUuid;
        }

        @Override
        public void run() {
            InternalOperationService service = nodeEngine.getOperationService();
            Address thisAddr = getLocalMember().getAddress();
            for (Map.Entry<String, String> entry : ownershipMappings.entrySet()) {
                String clientUuid = entry.getKey();
                String memberUuid = entry.getValue();
                if (deadMemberUuid.equals(memberUuid)) {
                    ClientDisconnectionOperation op = new ClientDisconnectionOperation(clientUuid, memberUuid);
                    service.createInvocationBuilder(ClientEngineImpl.SERVICE_NAME, op, thisAddr).invoke();
                }
            }
        }
    }

    @Override
    public Operation getPreJoinOperation() {
        Set<Member> members = nodeEngine.getClusterService().getMembers();
        HashSet<String> liveMemberUUIDs = new HashSet<String>();
        for (Member member : members) {
            liveMemberUUIDs.add(member.getUuid());
        }
        Map<String, String> liveMappings = new HashMap<String, String>(ownershipMappings);
        liveMappings.values().retainAll(liveMemberUUIDs);
        return liveMappings.isEmpty() ? null : new OnJoinClientOperation(liveMappings);
    }

    @SuppressWarnings("checkstyle:methodlength")
    @Override
    public Map<ClientType, Integer> getConnectedClientStats() {
        int numberOfCppClients = 0;
        int numberOfDotNetClients = 0;
        int numberOfJavaClients = 0;
        int numberOfNodeJSClients = 0;
        int numberOfPythonClients = 0;
        int numberOfGoClients = 0;
        int numberOfOtherClients = 0;

        OperationService operationService = node.nodeEngine.getOperationService();
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
                //Merge connected clients according to their UUID
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
                case GO:
                    numberOfGoClients++;
                    break;
                default:
                    numberOfOtherClients++;
            }
        }

        final Map<ClientType, Integer> resultMap = new EnumMap<ClientType, Integer>(ClientType.class);

        resultMap.put(ClientType.CPP, numberOfCppClients);
        resultMap.put(ClientType.CSHARP, numberOfDotNetClients);
        resultMap.put(ClientType.JAVA, numberOfJavaClients);
        resultMap.put(ClientType.NODEJS, numberOfNodeJSClients);
        resultMap.put(ClientType.PYTHON, numberOfPythonClients);
        resultMap.put(ClientType.GO, numberOfGoClients);
        resultMap.put(ClientType.OTHER, numberOfOtherClients);

        return resultMap;
    }

    @Override
    public Map<String, String> getClientStatistics() {
        Collection<ClientEndpoint> clientEndpoints = endpointManager.getEndpoints();
        Map<String, String> statsMap = new HashMap<String, String>(clientEndpoints.size());
        for (ClientEndpoint e : clientEndpoints) {
            String statistics = e.getClientStatistics();
            if (null != statistics) {
                statsMap.put(e.getUuid(), statistics);
            }
        }
        return statsMap;
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
