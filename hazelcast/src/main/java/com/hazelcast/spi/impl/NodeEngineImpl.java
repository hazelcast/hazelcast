/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Cluster;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.transaction.TransactionImpl;

import java.util.Collection;

public class NodeEngineImpl implements NodeEngine {

    private final Node node;
    private final ILogger logger;
    private final int partitionCount;
    private final ServiceManager serviceManager;

    final OperationServiceImpl operationService;
    final ExecutionServiceImpl executionService;
    final EventServiceImpl eventService;
    final WaitNotifyService waitNotifyService;

    public NodeEngineImpl(Node node) {
        this.node = node;
        logger = node.getLogger(NodeEngine.class.getName());
        partitionCount = node.groupProperties.PARTITION_COUNT.getInteger();
        serviceManager = new ServiceManager(this);
        executionService = new ExecutionServiceImpl(this);
        operationService = new OperationServiceImpl(this);
        eventService = new EventServiceImpl(this);
        waitNotifyService = new WaitNotifyService(this, new WaitingOpProcessorImpl());
    }

    @PrivateApi
    public void start() {
        serviceManager.start();
    }

    public Cluster getCluster() {
        return getClusterService().getClusterProxy();
    }

    public Address getThisAddress() {
        return node.getThisAddress();
    }

    public final int getPartitionId(Data key) {
        return node.partitionService.getPartitionId(key);
    }

    public PartitionInfo getPartitionInfo(int partitionId) {
        PartitionInfo p = node.partitionService.getPartition(partitionId);
        if (p.getOwner() == null) {
            // probably ownerships are not set yet.
            // force it.
            node.partitionService.getPartitionOwner(partitionId);
        }
        return p;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public Config getConfig() {
        return node.getConfig();
    }

    public EventService getEventService() {
        return eventService;
    }

    public OperationService getOperationService() {
        return operationService;
    }

    public ExecutionService getExecutionService() {
        return executionService;
    }

    public Data toData(final Object object) {
        ThreadContext.get().setCurrentInstance(node.hazelcastInstance);
        return IOUtil.toData(object);
    }

    public Object toObject(final Object object) {
        ThreadContext.get().setCurrentInstance(node.hazelcastInstance);
        return IOUtil.toObject(object);
    }

    public TransactionImpl getTransaction() {
        return ThreadContext.get().getTransaction();
    }

    public ILogger getLogger(String name) {
        return node.getLogger(name);
    }

    public GroupProperties getGroupProperties() {
        return node.getGroupProperties();
    }

    @PrivateApi
    public void handlePacket(Packet packet) {
        if (packet.isHeaderSet(Packet.HEADER_OP)) {
            operationService.handleOperation(packet);
        } else if (packet.isHeaderSet(Packet.HEADER_EVENT)) {
            eventService.handleEvent(packet);
        } else {
            throw new IllegalArgumentException("Unknown packet type !");
        }
    }

    @PrivateApi
    public <T> T getService(String serviceName) {
        return serviceManager.getService(serviceName);
    }

    /**
     * Returns a list of services matching provides service class/interface.
     * <br></br>
     * <b>CoreServices will be placed at the beginning of the list.</b>
     */
    @PrivateApi
    public <S> Collection<S> getServices(Class<S> serviceClass) {
        return serviceManager.getServices(serviceClass);
    }

    @PrivateApi
    public Node getNode() {
        return node;
    }

    @PrivateApi
    public ClusterService getClusterService() {
        return node.getClusterService();
    }

    @PrivateApi
    public void onMemberLeft(MemberImpl member) {
        onMemberDisconnect(member.getAddress());
    }

    @PrivateApi
    public void onMemberDisconnect(Address disconnectedAddress) {
        waitNotifyService.onMemberDisconnect(disconnectedAddress);
        operationService.onMemberDisconnect(disconnectedAddress);
    }

    @PrivateApi
    public void onPartitionMigrate(MigrationInfo migrationInfo) {
        waitNotifyService.onPartitionMigrate(getThisAddress(), migrationInfo);
    }

    @PrivateApi
    public void shutdown() {
        waitNotifyService.shutdown();
        serviceManager.shutdown();
        executionService.shutdown();
        eventService.shutdown();
        operationService.shutdown();
    }

    private class WaitingOpProcessorImpl implements WaitNotifyService.WaitingOpProcessor {

        public void process(final WaitNotifyService.WaitingOp so) throws Exception {
            executionService.execute(new Runnable() {
                public void run() {
                    operationService.runOperation(so);
                }
            });
        }

        public void processUnderExistingLock(Operation operation) {
            operationService.runOperationUnderExistingLock(operation);
        }
    }
}
