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

package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.diagnostics.BuildInfoPlugin;
import com.hazelcast.internal.diagnostics.ConfigPropertiesPlugin;
import com.hazelcast.internal.diagnostics.MemberHazelcastInstanceInfoPlugin;
import com.hazelcast.internal.diagnostics.InvocationPlugin;
import com.hazelcast.internal.diagnostics.MetricsPlugin;
import com.hazelcast.internal.diagnostics.OverloadedConnectionsPlugin;
import com.hazelcast.internal.diagnostics.PendingInvocationsPlugin;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.SlowOperationPlugin;
import com.hazelcast.internal.diagnostics.SystemPropertiesPlugin;
import com.hazelcast.internal.diagnostics.SystemLogPlugin;
import com.hazelcast.internal.metrics.metricsets.ClassLoadingMetricSet;
import com.hazelcast.internal.metrics.metricsets.FileMetricSet;
import com.hazelcast.internal.metrics.metricsets.GarbageCollectionMetricSet;
import com.hazelcast.internal.metrics.metricsets.OperatingSystemMetricSet;
import com.hazelcast.internal.metrics.metricsets.RuntimeMetricSet;
import com.hazelcast.internal.metrics.metricsets.ThreadMetricSet;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.quorum.impl.QuorumServiceImpl;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.SharedService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.ServiceNotFoundException;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.impl.executionservice.impl.ExecutionServiceImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;
import com.hazelcast.spi.impl.packetdispatcher.impl.PacketDispatcherImpl;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;
import com.hazelcast.spi.impl.servicemanager.impl.ServiceManagerImpl;
import com.hazelcast.spi.impl.waitnotifyservice.WaitNotifyService;
import com.hazelcast.spi.impl.waitnotifyservice.impl.WaitNotifyServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.wan.WanReplicationService;

import java.util.Collection;
import java.util.LinkedList;

import static com.hazelcast.internal.diagnostics.Diagnostics.METRICS_LEVEL;
import static java.lang.System.currentTimeMillis;

/**
 * The NodeEngineImpl is the where the construction of the Hazelcast dependencies take place. It can be
 * compared to a Spring ApplicationContext. It is fine that we refer to concrete types, and it is fine
 * that we cast to a concrete type within this class (e.g. to call shutdown). In an application context
 * you get exactly the same behavior.
 * <p/>
 * But the crucial thing is that we don't want to leak concrete dependencies to the outside. For example
 * we don't leak {@link com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl} to the outside.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class NodeEngineImpl implements NodeEngine {

    private final Node node;
    private final ILogger logger;
    private final EventServiceImpl eventService;
    private final OperationServiceImpl operationService;
    private final ExecutionServiceImpl executionService;
    private final WaitNotifyServiceImpl waitNotifyService;
    private final ServiceManagerImpl serviceManager;
    private final TransactionManagerServiceImpl transactionManagerService;
    private final ProxyServiceImpl proxyService;
    private final WanReplicationService wanReplicationService;
    private final PacketDispatcher packetDispatcher;
    private final QuorumServiceImpl quorumService;
    private final MetricsRegistryImpl metricsRegistry;
    private final SerializationService serializationService;
    private final LoggingServiceImpl loggingService;
    private final Diagnostics diagnostics;

    public NodeEngineImpl(final Node node) {
        this.node = node;
        this.loggingService = node.loggingService;
        this.serializationService = node.getSerializationService();
        this.logger = node.getLogger(NodeEngine.class.getName());
        this.metricsRegistry = newMetricRegistry(node);
        this.proxyService = new ProxyServiceImpl(this);
        this.serviceManager = new ServiceManagerImpl(this);
        this.executionService = new ExecutionServiceImpl(this);
        this.operationService = new OperationServiceImpl(this);
        this.eventService = new EventServiceImpl(this);
        this.waitNotifyService = new WaitNotifyServiceImpl(this);
        this.transactionManagerService = new TransactionManagerServiceImpl(this);
        this.wanReplicationService = node.getNodeExtension().createService(WanReplicationService.class);
        this.packetDispatcher = new PacketDispatcherImpl(
                logger,
                operationService.getOperationExecutor(),
                operationService.getAsyncResponseHandler(),
                operationService.getInvocationMonitor(),
                eventService,
                new ConnectionManagerPacketHandler());
        this.quorumService = new QuorumServiceImpl(this);
        this.diagnostics = newDiagnostics();

        serviceManager.registerService(InternalOperationService.SERVICE_NAME, operationService);
        serviceManager.registerService(WaitNotifyService.SERVICE_NAME, waitNotifyService);
    }

    private MetricsRegistryImpl newMetricRegistry(Node node) {
        ProbeLevel probeLevel = node.getProperties().getEnum(METRICS_LEVEL, ProbeLevel.class);
        return new MetricsRegistryImpl(node.getLogger(MetricsRegistryImpl.class), probeLevel);
    }

    private Diagnostics newDiagnostics() {
        Member localMember = node.getLocalMember();
        Address address = localMember.getAddress();
        String addressString = address.getHost().replace(":", "_") + "_" + address.getPort();
        String name = "diagnostics-" + addressString + "-" + currentTimeMillis();

        return new Diagnostics(
                name,
                loggingService.getLogger(Diagnostics.class),
                node.getHazelcastThreadGroup(),
                node.getProperties());
    }

    class ConnectionManagerPacketHandler implements PacketHandler {
        // ConnectionManager is only available after the NodeEngineImpl is available.
        @Override
        public void handle(Packet packet) throws Exception {
            PacketHandler packetHandler = (PacketHandler) node.getConnectionManager();
            packetHandler.handle(packet);
        }
    }

    public MetricsRegistry getMetricsRegistry() {
        return metricsRegistry;
    }

    public PacketDispatcher getPacketDispatcher() {
        return packetDispatcher;
    }

    public void start() {
        RuntimeMetricSet.register(metricsRegistry);
        GarbageCollectionMetricSet.register(metricsRegistry);
        OperatingSystemMetricSet.register(metricsRegistry);
        ThreadMetricSet.register(metricsRegistry);
        ClassLoadingMetricSet.register(metricsRegistry);
        FileMetricSet.register(metricsRegistry);

        metricsRegistry.collectMetrics(operationService);
        metricsRegistry.collectMetrics(proxyService);
        metricsRegistry.collectMetrics(eventService);

        serviceManager.start();
        proxyService.init();
        operationService.start();
        quorumService.start();
        diagnostics.start();

        // static loggers at beginning of file
        diagnostics.register(new BuildInfoPlugin(this));
        diagnostics.register(new SystemPropertiesPlugin(this));
        diagnostics.register(new ConfigPropertiesPlugin(this));

        // periodic loggers
        diagnostics.register(new OverloadedConnectionsPlugin(this));
        diagnostics.register(new PendingInvocationsPlugin(this));
        diagnostics.register(new MetricsPlugin(this));
        diagnostics.register(new SlowOperationPlugin(this));
        diagnostics.register(new InvocationPlugin(this));
        diagnostics.register(new MemberHazelcastInstanceInfoPlugin(this));
        diagnostics.register(new SystemLogPlugin(this));
    }

    public ServiceManager getServiceManager() {
        return serviceManager;
    }

    @Override
    public Address getThisAddress() {
        return node.getThisAddress();
    }

    @Override
    public Address getMasterAddress() {
        return node.getMasterAddress();
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
    public ClassLoader getConfigClassLoader() {
        return node.getConfigClassLoader();
    }

    @Override
    public InternalEventService getEventService() {
        return eventService;
    }

    @Override
    public SerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public InternalOperationService getOperationService() {
        return operationService;
    }

    @Override
    public InternalExecutionService getExecutionService() {
        return executionService;
    }

    @Override
    public InternalPartitionService getPartitionService() {
        return node.getPartitionService();
    }

    @Override
    public ClusterService getClusterService() {
        return node.getClusterService();
    }

    public ManagementCenterService getManagementCenterService() {
        return node.getManagementCenterService();
    }

    @Override
    public InternalProxyService getProxyService() {
        return proxyService;
    }

    public WaitNotifyService getWaitNotifyService() {
        return waitNotifyService;
    }

    @Override
    public WanReplicationService getWanReplicationService() {
        return wanReplicationService;
    }

    @Override
    public QuorumServiceImpl getQuorumService() {
        return quorumService;
    }

    @Override
    public TransactionManagerService getTransactionManagerService() {
        return transactionManagerService;
    }

    @Override
    public Data toData(Object object) {
        return serializationService.toData(object);
    }

    @Override
    public Object toObject(Object object) {
        return serializationService.toObject(object);
    }

    @Override
    public boolean isActive() {
        return isRunning();
    }

    @Override
    public boolean isRunning() {
        return node.isRunning();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return node.hazelcastInstance;
    }

    @Override
    public ILogger getLogger(String name) {
        return loggingService.getLogger(name);
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return loggingService.getLogger(clazz);
    }

    @Override
    public HazelcastProperties getProperties() {
        return node.getProperties();
    }

    @Override
    public <T> T getService(String serviceName) {
        T service = serviceManager.getService(serviceName);
        if (service == null) {
            if (isRunning()) {
                throw new HazelcastException("Service with name '" + serviceName + "' not found!",
                        new ServiceNotFoundException("Service with name '" + serviceName + "' not found!"));
            } else {
                throw new RetryableHazelcastException("HazelcastInstance[" + getThisAddress()
                        + "] is not active!");
            }
        }
        return service;
    }

    @Override
    public <T extends SharedService> T getSharedService(String serviceName) {
        return serviceManager.getSharedService(serviceName);
    }

    /**
     * Returns a list of services matching provides service class/interface.
     * <br></br>
     * <b>CoreServices will be placed at the beginning of the list.</b>
     */
    public <S> Collection<S> getServices(Class<S> serviceClass) {
        return serviceManager.getServices(serviceClass);
    }

    public Collection<ServiceInfo> getServiceInfos(Class serviceClass) {
        return serviceManager.getServiceInfos(serviceClass);
    }

    public Node getNode() {
        return node;
    }

    public void onMemberLeft(MemberImpl member) {
        waitNotifyService.onMemberLeft(member);
        operationService.onMemberLeft(member);
        eventService.onMemberLeft(member);
    }

    public void onClientDisconnected(String clientUuid) {
        waitNotifyService.onClientDisconnected(clientUuid);
    }

    public void onPartitionMigrate(MigrationInfo migrationInfo) {
        waitNotifyService.onPartitionMigrate(getThisAddress(), migrationInfo);
    }

    /**
     * Post join operations must be lock free; means no locks at all;
     * no partition locks, no key-based locks, no service level locks!
     * <p/>
     * Post join operations should return response, at least a null response.
     * <p/>
     */
    public Operation[] getPostJoinOperations() {
        final Collection<Operation> postJoinOps = new LinkedList<Operation>();
        Operation eventPostJoinOp = eventService.getPostJoinOperation();
        if (eventPostJoinOp != null) {
            postJoinOps.add(eventPostJoinOp);
        }
        Collection<PostJoinAwareService> services = getServices(PostJoinAwareService.class);
        for (PostJoinAwareService service : services) {
            final Operation postJoinOperation = service.getPostJoinOperation();
            if (postJoinOperation != null) {
                if (postJoinOperation.getPartitionId() >= 0) {
                    logger.severe(
                            "Post-join operations cannot implement PartitionAwareOperation! Service: "
                                    + service + ", Operation: "
                                    + postJoinOperation);
                    continue;
                }
                postJoinOps.add(postJoinOperation);
            }
        }
        return postJoinOps.isEmpty() ? null : postJoinOps.toArray(new Operation[postJoinOps.size()]);
    }

    public void reset() {
        waitNotifyService.reset();
        operationService.reset();
    }

    public void shutdown(final boolean terminate) {
        logger.finest("Shutting down services...");
        waitNotifyService.shutdown();
        operationService.shutdownInvocations();
        proxyService.shutdown();
        serviceManager.shutdown(terminate);
        eventService.shutdown();
        operationService.shutdownOperationExecutor();
        wanReplicationService.shutdown();
        executionService.shutdown();
        metricsRegistry.shutdown();
        diagnostics.shutdown();
    }
}
