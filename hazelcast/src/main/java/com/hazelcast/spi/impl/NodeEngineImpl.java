/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.quorum.impl.QuorumServiceImpl;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.ServiceInfo;
import com.hazelcast.spi.SharedService;
import com.hazelcast.internal.storage.DataRef;
import com.hazelcast.internal.storage.Storage;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.spi.impl.servicemanager.impl.ServiceManagerImpl;
import com.hazelcast.spi.impl.waitnotifyservice.InternalWaitNotifyService;
import com.hazelcast.spi.impl.waitnotifyservice.impl.WaitNotifyServiceImpl;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.transceiver.PacketTransceiver;
import com.hazelcast.spi.impl.transceiver.impl.PacketTransceiverImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.impl.executionservice.impl.ExecutionServiceImpl;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.wan.WanReplicationService;

import java.util.Collection;
import java.util.LinkedList;

/**
 * The NodeEngineImpl is the where the construction of the Hazelcast dependencies take place. It can be
 * compared to a Spring ApplicationContext. It is fine that we refer to concrete types, and it is fine
 * that we cast to a concrete type within this class (e.g. to call shutdown). In an application context
 * you get exactly the same behavior.
 * <p/>
 * But the crucial thing is that we don't want to leak concrete dependencies to the outside. For example
 * we don't leak {@link com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl} to the outside.
 */
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
    private final PacketTransceiver packetTransceiver;
    private final QuorumServiceImpl quorumService;

    public NodeEngineImpl(Node node) {
        this.node = node;
        this.logger = node.getLogger(NodeEngine.class.getName());
        this.proxyService = new ProxyServiceImpl(this);
        this.serviceManager = new ServiceManagerImpl(this);
        this.executionService = new ExecutionServiceImpl(this);
        this.operationService = new OperationServiceImpl(this);
        this.eventService = new EventServiceImpl(this);
        this.waitNotifyService = new WaitNotifyServiceImpl(this);
        this.transactionManagerService = new TransactionManagerServiceImpl(this);
        this.wanReplicationService = node.getNodeExtension().createService(WanReplicationService.class);
        this.packetTransceiver = new PacketTransceiverImpl(
                node, logger, operationService, eventService, wanReplicationService, executionService);
        quorumService = new QuorumServiceImpl(this);
    }

    public PacketTransceiver getPacketTransceiver() {
        return packetTransceiver;
    }

    public void start() {
        serviceManager.start();
        proxyService.init();
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
        return node.getSerializationService();
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

    @Override
    public InternalWaitNotifyService getWaitNotifyService() {
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
    public Data toData(final Object object) {
        return node.getSerializationService().toData(object);
    }

    @Override
    public Object toObject(final Object object) {
        if (object instanceof Data) {
            return node.getSerializationService().toObject(object);
        }
        return object;
    }

    @Override
    public boolean isActive() {
        return node.isActive();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return node.hazelcastInstance;
    }

    @Override
    public ILogger getLogger(String name) {
        return node.getLogger(name);
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return node.getLogger(clazz);
    }

    @Override
    public GroupProperties getGroupProperties() {
        return node.getGroupProperties();
    }

    public <T> T getService(String serviceName) {
        return serviceManager.getService(serviceName);
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

    @Override
    public Storage<DataRef> getOffHeapStorage() {
        return node.getNodeExtension().getNativeDataStorage();
    }

    public void reset() {
        waitNotifyService.reset();
        operationService.reset();
    }

    public void shutdown(final boolean terminate) {
        logger.finest("Shutting down services...");
        waitNotifyService.shutdown();
        proxyService.shutdown();
        serviceManager.shutdown(terminate);
        eventService.shutdown();
        operationService.shutdown();
        wanReplicationService.shutdown();
        executionService.shutdown();
    }
}
