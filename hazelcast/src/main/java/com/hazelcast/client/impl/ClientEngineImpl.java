/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.Client;
import com.hazelcast.client.ClientListener;
import com.hazelcast.client.impl.operations.GetConnectedClientsOperation;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.AuthenticationBaseMessageTask;
import com.hazelcast.client.impl.protocol.task.BlockingMessageTask;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.task.TransactionalMessageTask;
import com.hazelcast.client.impl.protocol.task.UrgentMessageTask;
import com.hazelcast.client.impl.protocol.task.map.AbstractMapQueryMessageTask;
import com.hazelcast.client.impl.statistics.ClientStatistics;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.AddressChecker;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.AddressCheckerImpl;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.internal.util.executor.ExecutorType;
import com.hazelcast.internal.util.executor.UnblockablePoolExecutorThreadFactory;
import com.hazelcast.internal.util.phonehome.PhoneHome;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.sql.impl.client.SqlAbstractMessageTask;
import com.hazelcast.transaction.TransactionManagerService;

import javax.annotation.Nonnull;
import javax.security.auth.login.LoginException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;

/**
 * Class that requests, listeners from client handled in node side.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class ClientEngineImpl implements ClientEngine, CoreService,
        ManagedService, EventPublishingService<ClientEvent, ClientListener> {

    /**
     * Service name to be used in requests.
     */
    public static final String SERVICE_NAME = "hz:core:clientEngine";
    private static final int EXECUTOR_QUEUE_CAPACITY_PER_CORE = 100000;
    private static final int BLOCKING_THREADS_PER_CORE = 20;
    private static final int THREADS_PER_CORE = 1;
    private static final int QUERY_THREADS_PER_CORE = 1;
    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final Executor executor;
    private final Executor blockingExecutor;
    private final Executor queryExecutor;

    private volatile ClientSelector clientSelector = ClientSelectors.any();

    private final ClientEndpointManagerImpl endpointManager;
    private final ILogger logger;
    private final ConnectionListener connectionListener = new ConnectionListenerImpl();

    private final MessageTaskFactory messageTaskFactory;
    private final ClientExceptionFactory clientExceptionFactory;
    private final ClusterViewListenerService clusterListenerService;
    private final boolean advancedNetworkConfigEnabled;
    private final ClientLifecycleMonitor lifecycleMonitor;
    private final Map<UUID, Consumer<Long>> backupListeners = new ConcurrentHashMap<>();
    private final AddressChecker addressChecker;

    // not final for the testing purposes
    private ClientEndpointStatisticsManager endpointStatisticsManager;

    public ClientEngineImpl(Node node) {
        this.logger = node.getLogger(ClientEngine.class);
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.endpointManager = new ClientEndpointManagerImpl(nodeEngine);
        this.executor = newClientExecutor();
        this.queryExecutor = newClientQueryExecutor();
        this.blockingExecutor = newBlockingExecutor();
        this.messageTaskFactory = new CompositeMessageTaskFactory(nodeEngine);
        this.clientExceptionFactory = initClientExceptionFactory();
        this.clusterListenerService = new ClusterViewListenerService(nodeEngine);
        this.advancedNetworkConfigEnabled = node.getConfig().getAdvancedNetworkConfig().isEnabled();
        this.lifecycleMonitor = new ClientLifecycleMonitor(endpointManager, this, logger, nodeEngine,
                nodeEngine.getExecutionService(), node.getProperties());
        Set<String> trustedInterfaces = node.getConfig().getManagementCenterConfig().getTrustedInterfaces();
        this.addressChecker = new AddressCheckerImpl(trustedInterfaces, logger);
        this.endpointStatisticsManager = PhoneHome.isPhoneHomeEnabled(node)
                ? new ClientEndpointStatisticsManagerImpl() : new NoOpClientEndpointStatisticsManager();
    }

    private ClientExceptionFactory initClientExceptionFactory() {
        ClassLoader configClassLoader = nodeEngine.getConfigClassLoader();
        boolean jcacheAvailable = JCacheDetector.isJCacheAvailable(configClassLoader);
        return new ClientExceptionFactory(jcacheAvailable, configClassLoader);
    }

    private Executor newClientExecutor() {
        //if user code deployment is enabled, we need more thread per core since operations can do blocking tasks
        // to load classes from other members
        boolean userCodeDeploymentEnabled = nodeEngine.getConfig().getUserCodeDeploymentConfig().isEnabled();
        int threadsPerCore = userCodeDeploymentEnabled ? BLOCKING_THREADS_PER_CORE : THREADS_PER_CORE;

        final ExecutionService executionService = nodeEngine.getExecutionService();
        int coreSize = RuntimeAvailableProcessors.get();

        int threadCount = node.getProperties().getInteger(ClusterProperty.CLIENT_ENGINE_THREAD_COUNT);
        if (threadCount <= 0) {
            threadCount = coreSize * threadsPerCore;
        }
        logger.finest("Creating new client executor with threadCount=" + threadCount);

        //if user code deployment enabled, don't use the unblockable thread factory since operations can do blocking tasks
        // to load classes from other members
        if (userCodeDeploymentEnabled) {
            return executionService.register(ExecutionService.CLIENT_EXECUTOR,
                    threadCount, coreSize * EXECUTOR_QUEUE_CAPACITY_PER_CORE,
                    ExecutorType.CONCRETE);
        }

        String name = ExecutionService.CLIENT_EXECUTOR;
        ClassLoader classLoader = nodeEngine.getConfigClassLoader();
        String hzName = nodeEngine.getHazelcastInstance().getName();
        String internalName = name.substring("hz:".length());
        String threadNamePrefix = createThreadPoolName(hzName, internalName);
        UnblockablePoolExecutorThreadFactory factory = new UnblockablePoolExecutorThreadFactory(threadNamePrefix, classLoader);
        return executionService.register(ExecutionService.CLIENT_EXECUTOR,
                threadCount, coreSize * EXECUTOR_QUEUE_CAPACITY_PER_CORE, factory);
    }

    private Executor newClientQueryExecutor() {
        final ExecutionService executionService = nodeEngine.getExecutionService();
        int coreSize = RuntimeAvailableProcessors.get();

        int threadCount = node.getProperties().getInteger(ClusterProperty.CLIENT_ENGINE_QUERY_THREAD_COUNT);
        if (threadCount <= 0) {
            threadCount = coreSize * QUERY_THREADS_PER_CORE;
        }
        logger.finest("Creating new client query executor with threadCount=" + threadCount);

        return executionService.register(ExecutionService.CLIENT_QUERY_EXECUTOR,
                threadCount, coreSize * EXECUTOR_QUEUE_CAPACITY_PER_CORE,
                ExecutorType.CONCRETE);
    }

    private Executor newBlockingExecutor() {
        final ExecutionService executionService = nodeEngine.getExecutionService();
        int coreSize = Runtime.getRuntime().availableProcessors();

        int threadCount = node.getProperties().getInteger(ClusterProperty.CLIENT_ENGINE_BLOCKING_THREAD_COUNT);
        if (threadCount <= 0) {
            threadCount = coreSize * BLOCKING_THREADS_PER_CORE;
        }

        logger.finest("Creating new client executor for blocking tasks with threadCount=" + threadCount);

        return executionService.register(ExecutionService.CLIENT_BLOCKING_EXECUTOR,
                threadCount, coreSize * EXECUTOR_QUEUE_CAPACITY_PER_CORE,
                ExecutorType.CONCRETE);
    }

    @Override
    public int getClientEndpointCount() {
        return endpointManager.size();
    }

    public void accept(ClientMessage clientMessage) {
        Connection connection = clientMessage.getConnection();
        MessageTask messageTask = messageTaskFactory.create(clientMessage, connection);
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        if (isUrgent(messageTask)) {
            operationService.execute((UrgentMessageTask) messageTask);
        } else if (messageTask instanceof AbstractPartitionMessageTask) {
            operationService.execute((AbstractPartitionMessageTask) messageTask);
        } else if (isQuery(messageTask)) {
            queryExecutor.execute(messageTask);
        } else if (messageTask instanceof TransactionalMessageTask) {
            blockingExecutor.execute(messageTask);
        } else if (messageTask instanceof BlockingMessageTask) {
            blockingExecutor.execute(messageTask);
        } else {
            executor.execute(messageTask);
        }
    }

    private boolean isUrgent(MessageTask messageTask) {
        if (messageTask instanceof AuthenticationBaseMessageTask) {
            return node.securityContext == null;
        }
        return messageTask instanceof UrgentMessageTask;
    }

    private boolean isQuery(MessageTask messageTask) {
        return messageTask instanceof AbstractMapQueryMessageTask
                || messageTask instanceof SqlAbstractMessageTask;
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
    public Address getThisAddress() {
        if (advancedNetworkConfigEnabled) {
            Address clientServerSocketAddress = node.getLocalMember().getAddressMap().get(CLIENT);
            assert clientServerSocketAddress != null;
            return clientServerSocketAddress;
        }
        return node.getThisAddress();
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return node.getLogger(clazz);
    }

    @Override
    public ClientEndpointManager getEndpointManager() {
        return endpointManager;
    }

    @Override
    public ClientExceptionFactory getExceptionFactory() {
        return clientExceptionFactory;
    }

    @Override
    public SecurityContext getSecurityContext() {
        return node.securityContext;
    }

    @Override
    public boolean bind(final ClientEndpoint endpoint) {
        if (!isClientAllowed(endpoint)) {
            return false;
        }

        ServerConnection conn = endpoint.getConnection();
        InetSocketAddress socketAddress = conn.getRemoteSocketAddress();
        //socket address can be null if connection closed before bind
        if (socketAddress != null) {
            conn.setRemoteAddress(new Address(socketAddress));
        }

        if (endpointManager.registerEndpoint(endpoint)) {
            // remote address can be null if connection closed before bind.
            // On such a case, `ClientEngine#connectionRemoved` will not be called for this connection since
            // we did not register the connection.
            // Endpoint removal logic(inside `ClientEngine#connectionRemoved`) will not be able to run, instead endpoint
            // will be cleaned up by ClientHeartbeatMonitor#cleanupEndpointsWithDeadConnections later.
            if (conn.getRemoteAddress() != null && endpoint.getUuid() != null) {
                node.getServer().getConnectionManager(CLIENT).register(conn.getRemoteAddress(), endpoint.getUuid(), conn);
            }
        }

        // Second check to catch concurrent change done via applySelector
        if (!isClientAllowed(endpoint)) {
            endpointManager.removeEndpoint(endpoint);
            return false;
        }
        return true;
    }


    @Override
    public void applySelector(ClientSelector newSelector) {
        logger.info("Applying a new client selector :" + newSelector);
        clientSelector = newSelector;

        for (ClientEndpoint endpoint : endpointManager.getEndpoints()) {
            if (!isClientAllowed(endpoint)) {
                endpoint.getConnection().close("Client disconnected from cluster via Management Center", null);
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

    @Nonnull
    @Override
    public Collection<Client> getClients() {
        Collection<ClientEndpoint> endpoints = endpointManager.getEndpoints();
        Set<Client> clients = createHashSet(endpoints.size());
        clients.addAll(endpoints);
        return clients;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        node.getServer().getConnectionManager(CLIENT).addConnectionListener(connectionListener);

        ClientHeartbeatMonitor heartbeatMonitor = new ClientHeartbeatMonitor(
                endpointManager, getLogger(ClientHeartbeatMonitor.class), nodeEngine.getExecutionService(), node.getProperties());
        heartbeatMonitor.start();

        lifecycleMonitor.start();
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
    }

    @Override
    public TransactionManagerService getTransactionManagerService() {
        return node.nodeEngine.getTransactionManagerService();
    }

    @Override
    public ClusterViewListenerService getClusterListenerService() {
        return clusterListenerService;
    }

    @Override
    public boolean isClientAllowed(Client client) {
        return ConnectionType.MC_JAVA_CLIENT.equals(client.getClientType()) || clientSelector.select(client);
    }

    private final class ConnectionListenerImpl implements ConnectionListener {

        @Override
        public void connectionAdded(Connection conn) {
            //no-op
            //unfortunately we can't do the endpoint creation here, because this event is only called when the
            //connection is bound, but we need to use the endpoint connection before that.
        }

        @Override
        public void connectionRemoved(Connection c) {
            ServerConnection connection = (ServerConnection) c;

            if (!connection.isClient() || !nodeEngine.isRunning()) {
                return;
            }
            final ClientEndpointImpl endpoint = (ClientEndpointImpl) endpointManager.getEndpoint(connection);
            if (endpoint == null) {
                logger.finest("connectionRemoved: No endpoint for connection:" + connection);
                return;
            }
            UUID clientUuid = endpoint.getUuid();
            if (clientUuid != null) {
                node.getLocalAddressRegistry().tryRemoveRegistration(clientUuid,
                        endpoint.getConnection().getRemoteAddress());
            }
            endpointManager.removeEndpoint(endpoint);
        }
    }

    @Override
    public Map<String, Long> getActiveClientsInCluster() {
        Map<UUID, String> clients = getClientsInCluster();
        return clients.values()
                .stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    @Override
    public Map<String, ClientEndpointStatisticsSnapshot> getEndpointStatisticsSnapshots() {
        return endpointStatisticsManager.getSnapshotsAndReset(endpointManager.getEndpoints());
    }

    @Override
    public void onEndpointAuthenticated(ClientEndpoint endpoint) {
        endpointStatisticsManager.onEndpointAuthenticated(endpoint);
    }

    @Override
    public void onEndpointDestroyed(ClientEndpoint endpoint) {
        endpointStatisticsManager.onEndpointDestroyed(endpoint);
    }

    Map<UUID, String> getClientsInCluster() {
        OperationService operationService = node.nodeEngine.getOperationService();
        Map<UUID, String> clientsMap = new HashMap<>();

        for (Member member : node.getClusterService().getMembers()) {
            Address target = member.getAddress();
            Operation clientInfoOperation = new GetConnectedClientsOperation();
            Future<Map<UUID, String>> future
                    = operationService.invokeOnTarget(SERVICE_NAME, clientInfoOperation, target);
            try {
                Map<UUID, String> endpoints = future.get();
                if (endpoints == null) {
                    continue;
                }
                // Merge connected clients according to their UUID
                clientsMap.putAll(endpoints);
            } catch (Exception e) {
                logger.warning("Cannot get client information from: " + target.toString(), e);
            }
        }
        return clientsMap;
    }

    @Override
    public Map<UUID, ClientStatistics> getClientStatistics() {
        Collection<ClientEndpoint> clientEndpoints = endpointManager.getEndpoints();
        Map<UUID, ClientStatistics> statsMap = createHashMap(clientEndpoints.size());
        for (ClientEndpoint e : clientEndpoints) {
            ClientStatistics statistics = e.getClientStatistics();
            if (null != statistics) {
                statsMap.put(e.getUuid(), statistics);
            }
        }
        return statsMap;
    }

    @Override
    public void onClientAcquiredResource(UUID uuid) {
        lifecycleMonitor.addClientToMonitor(uuid);
    }

    @Override
    public void addBackupListener(UUID clientUuid, Consumer<Long> backupListener) {
        backupListeners.put(clientUuid, backupListener);
    }

    @Override
    public void dispatchBackupEvent(UUID clientUUID, long clientCorrelationId) {
        Consumer<Long> backupListener = backupListeners.get(clientUUID);
        if (backupListener != null) {
            backupListener.accept(clientCorrelationId);
        }
    }

    @Override
    public boolean deregisterBackupListener(UUID clientUUID, Consumer<Long> backupListener) {
        return backupListeners.remove(clientUUID, backupListener);
    }

    public Map<UUID, Consumer<Long>> getBackupListeners() {
        return backupListeners;
    }

    @Override
    public AddressChecker getManagementTasksChecker() {
        return addressChecker;
    }

    public void setEndpointStatisticsManager(ClientEndpointStatisticsManager endpointStatisticsManager) {
        // this should only be used in tests
        this.endpointStatisticsManager = endpointStatisticsManager;
    }
}
