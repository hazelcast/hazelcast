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

package com.hazelcast.spi.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationContext;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.ServiceInfo;
import com.hazelcast.spi.SharedService;
import com.hazelcast.spi.WaitNotifyService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.storage.DataRef;
import com.hazelcast.storage.Storage;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.wan.WanReplicationService;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class NodeEngineImpl implements NodeEngine {

    private final Node node;
    private final ILogger logger;

    private final ServiceManager serviceManager;
    private final TransactionManagerServiceImpl transactionManagerService;
    private final ProxyServiceImpl proxyService;
    private final WanReplicationService wanReplicationService;

    final OperationServiceImpl operationService;
    final ExecutionServiceImpl executionService;
    final EventServiceImpl eventService;
    final WaitNotifyServiceImpl waitNotifyService;

    public NodeEngineImpl(Node node) {
        this.node = node;
        logger = node.getLogger(NodeEngine.class.getName());
        proxyService = new ProxyServiceImpl(this);
        serviceManager = new ServiceManager(this);
        executionService = new ExecutionServiceImpl(this);
        operationService = new OperationServiceImpl(this);
        eventService = new EventServiceImpl(this);
        waitNotifyService = new WaitNotifyServiceImpl(this);
        transactionManagerService = new TransactionManagerServiceImpl(this);
        wanReplicationService = new WanReplicationService(node);
    }

    @PrivateApi
    public void start() {
        serviceManager.start();
        proxyService.init();
    }

    public Address getThisAddress() {
        return node.getThisAddress();
    }

    public Address getMasterAddress() {
        return node.getMasterAddress();
    }

    public MemberImpl getLocalMember() {
        return node.getLocalMember();
    }

    public Config getConfig() {
        return node.getConfig();
    }

    public ClassLoader getConfigClassLoader() {
        return node.getConfigClassLoader();
    }

    public EventService getEventService() {
        return eventService;
    }

    public SerializationService getSerializationService() {
        return node.getSerializationService();
    }

    public SerializationContext getSerializationContext() {
        return node.getSerializationService().getSerializationContext();
    }

    public OperationService getOperationService() {
        return operationService;
    }

    public ExecutionService getExecutionService() {
        return executionService;
    }

    public InternalPartitionService getPartitionService() {
        return node.getPartitionService();
    }

    public ClusterService getClusterService() {
        return node.getClusterService();
    }

    public ManagementCenterService getManagementCenterService() {
        return node.getManagementCenterService();
    }

    public ProxyService getProxyService() {
        return proxyService;
    }

    public WaitNotifyService getWaitNotifyService() {
        return waitNotifyService;
    }

    public WanReplicationService getWanReplicationService() {
        return wanReplicationService;
    }

    public TransactionManagerService getTransactionManagerService() {
        return transactionManagerService;
    }

    public Data toData(final Object object) {
        return node.getSerializationService().toData(object);
    }

    public Object toObject(final Object object) {
        if (object instanceof Data) {
            return node.getSerializationService().toObject((Data) object);
        }
        return object;
    }

    public boolean isActive() {
        return node.isActive();
    }

    public HazelcastInstance getHazelcastInstance() {
        return node.hazelcastInstance;
    }

    public boolean send(Packet packet, Connection connection) {
        if (connection == null || !connection.live()) return false;
        final MemberImpl memberImpl = node.getClusterService().getMember(connection.getEndPoint());
        if (memberImpl != null) {
            memberImpl.didWrite();
        }
        return connection.write(packet);
    }

    /**
     * Retries sending packet maximum 5 times until connection to target becomes available.
     */
    public boolean send(Packet packet, Address target) {
        return send(packet, target, null);
    }

    private boolean send(Packet packet, Address target, FutureSend futureSend) {
        final ConnectionManager connectionManager = node.getConnectionManager();
        final Connection connection = connectionManager.getConnection(target);
        if (connection != null) {
            return send(packet, connection);
        } else {
            if (futureSend == null) {
                futureSend = new FutureSend(packet, target);
            }
            final int retries = futureSend.retries;
            if (retries < 5 && node.isActive()) {
                connectionManager.getOrConnect(target, true);
                // TODO: Caution: may break the order guarantee of the packets sent from the same thread!
                executionService.schedule(futureSend, (retries + 1) * 100, TimeUnit.MILLISECONDS);
                return true;
            }
            return false;
        }
    }

    private class FutureSend implements Runnable {
        private final Packet packet;
        private final Address target;
        private volatile int retries = 0;

        private FutureSend(Packet packet, Address target) {
            this.packet = packet;
            this.target = target;
        }

        public void run() {
            retries++;
            if (logger.isFinestEnabled()) {
                logger.finest( "Retrying[" + retries + "] packet send operation to: " + target);
            }
            send(packet, target, this);
        }
    }

    public ILogger getLogger(String name) {
        return node.getLogger(name);
    }

    public ILogger getLogger(Class clazz) {
        return node.getLogger(clazz);
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
        } else if (packet.isHeaderSet(Packet.HEADER_WAN_REPLICATION)) {
            wanReplicationService.handleEvent(packet);
        } else {
            logger.severe("Unknown packet type! Header: " + packet.getHeader());
        }
    }

    @PrivateApi
    public <T> T getService(String serviceName) {
        final ServiceInfo serviceInfo = serviceManager.getServiceInfo(serviceName);
        return serviceInfo != null ? (T) serviceInfo.getService() : null;
    }

    public <T extends SharedService> T getSharedService(String serviceName) {
        final Object service = getService(serviceName);
        if (service == null) {
            return null;
        }
        if (service instanceof SharedService) {
            return (T) service;
        }
        throw new IllegalArgumentException("No SharedService registered with name: " + serviceName);
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
    public Collection<ServiceInfo> getServiceInfos(Class serviceClass) {
        return serviceManager.getServiceInfos(serviceClass);
    }

    @PrivateApi
    public Node getNode() {
        return node;
    }

    @PrivateApi
    public void onMemberLeft(MemberImpl member) {
        waitNotifyService.onMemberLeft(member);
        operationService.onMemberLeft(member);
        eventService.onMemberLeft(member);
    }

    @PrivateApi
    public void onClientDisconnected(String clientUuid) {
        waitNotifyService.onClientDisconnected(clientUuid);
    }

    @PrivateApi
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
    @PrivateApi
    public Operation[] getPostJoinOperations() {
        final Collection<Operation> postJoinOps = new LinkedList<Operation>();
        Operation eventPostJoinOp = eventService.getPostJoinOperation();
        if (eventPostJoinOp != null) {
            postJoinOps.add(eventPostJoinOp);
        }
        Collection<PostJoinAwareService> services = getServices(PostJoinAwareService.class);
        for (PostJoinAwareService service : services) {
            final Operation pjOp = service.getPostJoinOperation();
            if (pjOp != null) {
                if (pjOp instanceof PartitionAwareOperation) {
                    logger.severe("Post-join operations cannot implement PartitionAwareOperation!" +
                            " Service: " + service + ", Operation: " + pjOp);
                    continue;
                }
                postJoinOps.add(pjOp);
            }
        }
        return postJoinOps.isEmpty() ? null : postJoinOps.toArray(new Operation[postJoinOps.size()]);
    }

    public long getClusterTime() {
        return node.getClusterService().getClusterTime();
    }

    public Storage<DataRef> getOffHeapStorage() {
        return node.initializer.getOffHeapStorage();
    }

    @PrivateApi
    public void shutdown() {
        logger.finest( "Shutting down services...");
        waitNotifyService.shutdown();
        proxyService.shutdown();
        serviceManager.shutdown();
        executionService.shutdown();
        eventService.shutdown();
        operationService.shutdown();
        wanReplicationService.shutdown();
    }
}
