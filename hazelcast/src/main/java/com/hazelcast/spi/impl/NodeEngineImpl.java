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

package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hotrestart.BackupTaskStatus;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.dynamicconfig.ClusterWideConfigurationService;
import com.hazelcast.internal.dynamicconfig.DynamicConfigListener;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.probing.ProbeRegistry;
import com.hazelcast.internal.probing.ProbeSource;
import com.hazelcast.internal.probing.impl.ProbeRegistryImpl;
import com.hazelcast.internal.probing.sources.GcProbeSource;
import com.hazelcast.internal.probing.sources.MachineProbeSource;
import com.hazelcast.internal.probing.ProbingCycle;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentClassLoader;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.quorum.impl.QuorumServiceImpl;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.PreJoinAwareService;
import com.hazelcast.spi.SharedService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.ServiceNotFoundException;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.impl.executionservice.impl.ExecutionServiceImpl;
import com.hazelcast.spi.impl.operationparker.OperationParker;
import com.hazelcast.spi.impl.operationparker.impl.OperationParkerImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;
import com.hazelcast.spi.impl.servicemanager.impl.ServiceManagerImpl;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.util.ProbeEnumUtils;
import com.hazelcast.util.function.Consumer;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.wan.WanReplicationService;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map.Entry;

import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.System.currentTimeMillis;

/**
 * The NodeEngineImpl is the where the construction of the Hazelcast dependencies take place. It can be
 * compared to a Spring ApplicationContext. It is fine that we refer to concrete types, and it is fine
 * that we cast to a concrete type within this class (e.g. to call shutdown). In an application context
 * you get exactly the same behavior.
 * <p>
 * But the crucial thing is that we don't want to leak concrete dependencies to the outside. For example
 * we don't leak {@link com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl} to the outside.
 */
@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity", "checkstyle:methodcount"})
public class NodeEngineImpl implements NodeEngine, ProbeSource {

    private static final String JET_SERVICE_NAME = "hz:impl:jetService";

    private final Node node;
    private final SerializationService serializationService;
    private final LoggingServiceImpl loggingService;
    private final ILogger logger;
    private final ProbeRegistry probeRegistry;
    private final ProxyServiceImpl proxyService;
    private final ServiceManagerImpl serviceManager;
    private final ExecutionServiceImpl executionService;
    private final OperationServiceImpl operationService;
    private final EventServiceImpl eventService;
    private final OperationParkerImpl operationParker;
    private final ClusterWideConfigurationService configurationService;
    private final TransactionManagerServiceImpl transactionManagerService;
    private final WanReplicationService wanReplicationService;
    private final Consumer<Packet> packetDispatcher;
    private final QuorumServiceImpl quorumService;
    private final Diagnostics diagnostics;
    private final SplitBrainMergePolicyProvider splitBrainMergePolicyProvider;

    @SuppressWarnings("checkstyle:executablestatementcount")
    public NodeEngineImpl(Node node) {
        this.node = node;
        try {
            this.serializationService = node.getSerializationService();
            this.loggingService = node.loggingService;
            this.logger = node.getLogger(NodeEngine.class.getName());
            this.proxyService = new ProxyServiceImpl(this);
            this.serviceManager = new ServiceManagerImpl(this);
            this.executionService = new ExecutionServiceImpl(this);
            this.operationService = new OperationServiceImpl(this);
            this.eventService = new EventServiceImpl(this);
            this.operationParker = new OperationParkerImpl(this);
            UserCodeDeploymentService userCodeDeploymentService = new UserCodeDeploymentService();
            DynamicConfigListener dynamicConfigListener = node.getNodeExtension().createDynamicConfigListener();
            this.configurationService = new ClusterWideConfigurationService(this, dynamicConfigListener);
            ClassLoader configClassLoader = node.getConfigClassLoader();
            if (configClassLoader instanceof UserCodeDeploymentClassLoader) {
                ((UserCodeDeploymentClassLoader) configClassLoader).setUserCodeDeploymentService(userCodeDeploymentService);
            }
            this.transactionManagerService = new TransactionManagerServiceImpl(this);
            this.wanReplicationService = node.getNodeExtension().createService(WanReplicationService.class);
            this.packetDispatcher = new PacketDispatcher(
                    logger,
                    operationService.getOperationExecutor(),
                    operationService.getInboundResponseHandlerSupplier().get(),
                    operationService.getInvocationMonitor(),
                    eventService,
                    new ConnectionManagerPacketConsumer(),
                    new JetPacketConsumer());
            this.quorumService = new QuorumServiceImpl(this);
            this.diagnostics = newDiagnostics();
            this.splitBrainMergePolicyProvider = new SplitBrainMergePolicyProvider(this);
            serviceManager.registerService(InternalOperationService.SERVICE_NAME, operationService);
            serviceManager.registerService(OperationParker.SERVICE_NAME, operationParker);
            serviceManager.registerService(UserCodeDeploymentService.SERVICE_NAME, userCodeDeploymentService);
            serviceManager.registerService(ClusterWideConfigurationService.SERVICE_NAME, configurationService);
            this.probeRegistry = new ProbeRegistryImpl();
        } catch (Throwable e) {
            try {
                shutdown(true);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
            throw rethrow(e);
        }
    }

    private Diagnostics newDiagnostics() {
        Address address = node.getThisAddress();
        String addressString = address.getHost().replace(":", "_") + "_" + address.getPort();
        String name = "diagnostics-" + addressString + "-" + currentTimeMillis();

        return new Diagnostics(
                name,
                loggingService.getLogger(Diagnostics.class),
                getHazelcastInstance().getName(),
                node.getProperties());
    }

    public LoggingService getLoggingService() {
        return loggingService;
    }

    public ProbeRegistry getProbeRegistry() {
        return probeRegistry;
    }

    /**
     * For convenience this method automatically registers services that are
     * {@link ProbeSource}s.
     */
    private void initProbeSources() {
        probeRegistry.register(this);
        probeRegistry.register(GcProbeSource.INSTANCE);
        probeRegistry.register(MachineProbeSource.INSTANCE);
        probeRegistry.register(executionService);
        probeRegistry.registerIfSource(operationService.getOperationExecutor());
        probeRegistry.registerIfSource(node.getNodeExtension());
        for (ProbeSource s : serviceManager.getServices(ProbeSource.class)) {
            probeRegistry.register(s);
        }
    }

    @Override
    public void probeIn(ProbingCycle cycle) {
        cycle.probe("proxy", proxyService);
        cycle.probe("memory", node.getNodeExtension().getMemoryStats());
        cycle.probe("operation", operationService);
        cycle.probe("operation", operationService.getInvocationRegistry());
        cycle.probe("operation", operationService.getInboundResponseHandlerSupplier());
        cycle.probe("operation.invocations", operationService.getInvocationMonitor());
        if (cycle.isProbed(ProbeLevel.INFO)) {
            cycle.probe("operation.parker", operationParker);
            InternalPartitionServiceImpl partitionService = node.partitionService;
            cycle.probe("partitions", partitionService);
            cycle.probe("partitions", partitionService.getPartitionStateManager());
            cycle.probe("partitions", partitionService.getMigrationManager());
            cycle.probe("partitions", partitionService.getReplicaManager());
            cycle.probe("transactions", transactionManagerService);
            probeHotRestartStateIn(cycle);
            probeHotBackupStateIn(cycle);
        }
    }

    private void probeHotBackupStateIn(ProbingCycle cycle) {
        HotRestartService hotRestartService = node.getNodeExtension().getHotRestartService();
        boolean enabled = hotRestartService.isHotBackupEnabled();
        cycle.openContext().prefix("hotBackup");
        cycle.probe("enabled", enabled);
        if (enabled) {
            BackupTaskStatus status = hotRestartService.getBackupTaskStatus();
            if (status != null) {
                cycle.probe("state", ProbeEnumUtils.codeOf(status.getState()));
                cycle.probe("completed", status.getCompleted());
                cycle.probe("total", status.getTotal());
            }
        }
    }

    private void probeHotRestartStateIn(ProbingCycle cycle) {
        InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
        ClusterHotRestartStatusDTO status = hotRestartService.getCurrentClusterHotRestartStatus();
        if (status != null && status.getHotRestartStatus() != ClusterHotRestartStatus.UNKNOWN) {
            cycle.openContext().prefix("hotRestart");
            cycle.probe("remainingDataLoadTime", status.getRemainingDataLoadTimeMillis());
            cycle.probe("remainingValidationTime", status.getRemainingValidationTimeMillis());
            cycle.probe("status", ProbeEnumUtils.codeOf(status.getHotRestartStatus()));
            cycle.probe("dataRecoveryPolicy", ProbeEnumUtils.codeOf(status.getDataRecoveryPolicy()));
            for (Entry<String, MemberHotRestartStatus> memberStatus : status
                    .getMemberHotRestartStatusMap().entrySet()) {
                cycle.openContext().tag(TAG_INSTANCE, memberStatus.getKey()).prefix("hotRestart");
                cycle.probe("memberStatus", ProbeEnumUtils.codeOf(memberStatus.getValue()));
            }
        }
    }

    public void start() {
        serviceManager.start();

        proxyService.init();
        operationService.start();
        quorumService.start();
        diagnostics.start();

        node.getNodeExtension().registerPlugins(diagnostics);

        initProbeSources();
    }

    public Consumer<Packet> getPacketDispatcher() {
        return packetDispatcher;
    }

    public Diagnostics getDiagnostics() {
        return diagnostics;
    }

    public ClusterWideConfigurationService getConfigurationService() {
        return configurationService;
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

    public OperationParker getOperationParker() {
        return operationParker;
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
    public <T> T toObject(Object object) {
        return serializationService.toObject(object);
    }

    @Override
    public <T> T toObject(Object object, Class klazz) {
        return serializationService.toObject(object, klazz);
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
                throw new RetryableHazelcastException("HazelcastInstance[" + getThisAddress() + "] is not active!");
            }
        }
        return service;
    }

    @Override
    public <T extends SharedService> T getSharedService(String serviceName) {
        return serviceManager.getSharedService(serviceName);
    }

    @Override
    public MemberVersion getVersion() {
        return node.getVersion();
    }

    @Override
    public SplitBrainMergePolicyProvider getSplitBrainMergePolicyProvider() {
        return splitBrainMergePolicyProvider;
    }

    @Override
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
        operationParker.onMemberLeft(member);
        operationService.onMemberLeft(member);
        eventService.onMemberLeft(member);
    }

    public void onClientDisconnected(String clientUuid) {
        operationParker.onClientDisconnected(clientUuid);
    }

    public void onPartitionMigrate(MigrationInfo migrationInfo) {
        operationParker.onPartitionMigrate(getThisAddress(), migrationInfo);
    }

    /**
     * Collects all post-join operations from {@link PostJoinAwareService}s.
     * <p>
     * Post join operations should return response, at least a {@code null} response.
     * <p>
     * <b>Note</b>: Post join operations must be lock free, meaning no locks at all:
     * no partition locks, no key-based locks, no service level locks, no database interaction!
     * The {@link Operation#getPartitionId()} method should return a negative value.
     * This means that the operations should not implement {@link PartitionAwareOperation}.
     *
     * @return the operations to be executed at the end of a finalized join
     */
    public Operation[] getPostJoinOperations() {
        Collection<Operation> postJoinOps = new LinkedList<Operation>();
        Collection<PostJoinAwareService> services = getServices(PostJoinAwareService.class);
        for (PostJoinAwareService service : services) {
            Operation postJoinOperation = service.getPostJoinOperation();
            if (postJoinOperation != null) {
                if (postJoinOperation.getPartitionId() >= 0) {
                    logger.severe("Post-join operations should not have partition ID set! Service: "
                            + service + ", Operation: "
                            + postJoinOperation);
                    continue;
                }
                postJoinOps.add(postJoinOperation);
            }
        }
        return postJoinOps.isEmpty() ? null : postJoinOps.toArray(new Operation[0]);
    }

    public Operation[] getPreJoinOperations() {
        Collection<Operation> preJoinOps = new LinkedList<Operation>();
        Collection<PreJoinAwareService> services = getServices(PreJoinAwareService.class);
        for (PreJoinAwareService service : services) {
            Operation preJoinOperation = service.getPreJoinOperation();
            if (preJoinOperation != null) {
                if (preJoinOperation.getPartitionId() >= 0) {
                    logger.severe("Pre-join operations operations should not have partition ID set! Service: "
                            + service + ", Operation: "
                            + preJoinOperation);
                    continue;
                }
                preJoinOps.add(preJoinOperation);
            }
        }
        return preJoinOps.isEmpty() ? null : preJoinOps.toArray(new Operation[0]);
    }

    public void reset() {
        operationParker.reset();
        operationService.reset();
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    public void shutdown(boolean terminate) {
        logger.finest("Shutting down services...");
        if (operationParker != null) {
            operationParker.shutdown();
        }
        if (operationService != null) {
            operationService.shutdownInvocations();
        }
        if (proxyService != null) {
            proxyService.shutdown();
        }
        if (serviceManager != null) {
            serviceManager.shutdown(terminate);
        }
        if (eventService != null) {
            eventService.shutdown();
        }
        if (operationService != null) {
            operationService.shutdownOperationExecutor();
        }
        if (wanReplicationService != null) {
            wanReplicationService.shutdown();
        }
        if (executionService != null) {
            executionService.shutdown();
        }
        if (diagnostics != null) {
            diagnostics.shutdown();
        }
    }

    private class ConnectionManagerPacketConsumer implements Consumer<Packet> {

        @Override
        public void accept(Packet packet) {
            // ConnectionManager is only available after the NodeEngineImpl is available
            Consumer<Packet> packetConsumer = (Consumer<Packet>) node.getConnectionManager();
            packetConsumer.accept(packet);
        }
    }

    private class JetPacketConsumer implements Consumer<Packet> {

        private volatile Consumer<Packet> packetConsumer;

        @Override
        public void accept(Packet packet) {
            // currently service registration is done after the creation of the packet dispatcher,
            // hence we need to lazily initialize the JetPacketConsumer
            if (packetConsumer == null) {
                packetConsumer = serviceManager.getService(JET_SERVICE_NAME);
                if (packetConsumer == null) {
                    throw new UnsupportedOperationException("Jet is not registered on this node");
                }
            }
            packetConsumer.accept(packet);
        }
    }
}
