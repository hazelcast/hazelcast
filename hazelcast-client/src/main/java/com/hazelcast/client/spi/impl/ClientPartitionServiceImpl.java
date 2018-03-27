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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddPartitionListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.util.HashUtil;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * The {@link ClientPartitionService} implementation.
 */
public final class ClientPartitionServiceImpl
        extends ClientAddPartitionListenerCodec.AbstractEventHandler
        implements EventHandler<ClientMessage>, ClientPartitionService {

    private static final long PERIOD = 10;
    private static final long INITIAL_DELAY = 10;

    private final ExecutionCallback<ClientMessage> refreshTaskCallback = new RefreshTaskCallback();
    private final ConcurrentHashMap<Integer, Address> partitions = new ConcurrentHashMap<Integer, Address>(271, 0.75f, 1);
    private final ClientExecutionServiceImpl clientExecutionService;
    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;

    private volatile int partitionCount;
    private volatile int lastPartitionStateVersion = -1;
    private final Object lock = new Object();

    public ClientPartitionServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClientPartitionService.class);
        clientExecutionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
    }

    public void start() {
        //scheduling left in place to support server versions before 3.9.
        clientExecutionService.scheduleWithRepetition(new RefreshTask(), INITIAL_DELAY, PERIOD, TimeUnit.SECONDS);
    }

    public void listenPartitionTable(Connection ownerConnection) throws Exception {
        //when we connect to cluster back we need to reset partition state version
        lastPartitionStateVersion = -1;
        if (((ClientConnection) ownerConnection).getConnectedServerVersion() >= BuildInfo.calculateVersion("3.9")) {
            //Servers after 3.9 supports listeners
            ClientMessage clientMessage = ClientAddPartitionListenerCodec.encodeRequest();
            ClientInvocation invocation = new ClientInvocation(client, clientMessage, null, ownerConnection);
            invocation.setEventHandler(this);
            invocation.invokeUrgent().get();
        }
    }

    public void refreshPartitions() {
        try {
            // use internal execution service for all partition refresh process (do not use the user executor thread)
            clientExecutionService.execute(new RefreshTask());
        } catch (RejectedExecutionException ignored) {
            ignore(ignored);
        }
    }

    @Override
    public void handle(Collection<Map.Entry<Address, List<Integer>>> collection, int partitionStateVersion) {
        processPartitionResponse(collection, partitionStateVersion, true);
    }

    @Override
    public void beforeListenerRegister() {

    }

    @Override
    public void onListenerRegister() {

    }

    private void waitForPartitionsFetchedOnce() {
        while (partitionCount == 0 && client.getConnectionManager().isAlive()) {
            if (isClusterFormedByOnlyLiteMembers()) {
                throw new NoDataMemberInClusterException(
                        "Partitions can't be assigned since all nodes in the cluster are lite members");
            }
            ClientMessage requestMessage = ClientGetPartitionsCodec.encodeRequest();
            ClientInvocationFuture future = new ClientInvocation(client, requestMessage, null).invokeUrgent();
            try {
                ClientMessage responseMessage = future.get();
                ClientGetPartitionsCodec.ResponseParameters response =
                        ClientGetPartitionsCodec.decodeResponse(responseMessage);
                processPartitionResponse(response.partitions,
                        response.partitionStateVersion, response.partitionStateVersionExist);
            } catch (Exception e) {
                if (client.getLifecycleService().isRunning()) {
                    logger.warning("Error while fetching cluster partition table!", e);
                }
            }
        }
    }

    private boolean isClusterFormedByOnlyLiteMembers() {
        final ClientClusterService clusterService = client.getClientClusterService();
        return clusterService.getMembers(MemberSelectors.DATA_MEMBER_SELECTOR).isEmpty();
    }

    private boolean processPartitionResponse(Collection<Map.Entry<Address, List<Integer>>> partitions,
                                             int partitionStateVersion,
                                             boolean partitionStateVersionExist) {
        synchronized (lock) {
            if (!partitionStateVersionExist || partitionStateVersion > lastPartitionStateVersion) {
                for (Map.Entry<Address, List<Integer>> entry : partitions) {
                    Address address = entry.getKey();
                    for (Integer partition : entry.getValue()) {
                        this.partitions.put(partition, address);
                    }
                }
                partitionCount = this.partitions.size();
                lastPartitionStateVersion = partitionStateVersion;
                if (logger.isFinestEnabled()) {
                    logger.finest("Processed partition response. partitionStateVersion : "
                            + (partitionStateVersionExist ? partitionStateVersion : "NotAvailable")
                            + ", partitionCount :" + partitionCount);
                }
            }

        }
        return partitionCount > 0;
    }

    public void stop() {
        partitions.clear();
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
        waitForPartitionsFetchedOnce();
        return partitions.get(partitionId);
    }

    @Override
    public int getPartitionId(Data key) {
        final int pc = getPartitionCount();
        if (pc <= 0) {
            return 0;
        }
        int hash = key.getPartitionHash();
        return HashUtil.hashToIndex(hash, pc);
    }

    @Override
    public int getPartitionId(Object key) {
        final Data data = client.getSerializationService().toData(key);
        return getPartitionId(data);
    }

    @Override
    public int getPartitionCount() {
        waitForPartitionsFetchedOnce();
        return partitionCount;
    }

    @Override
    public Partition getPartition(int partitionId) {
        return new PartitionImpl(partitionId);
    }

    private final class PartitionImpl implements Partition {

        private final int partitionId;

        private PartitionImpl(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public Member getOwner() {
            Address owner = getPartitionOwner(partitionId);
            if (owner != null) {
                return client.getClientClusterService().getMember(owner);
            }
            return null;
        }

        @Override
        public String toString() {
            return "PartitionImpl{partitionId=" + partitionId + '}';
        }
    }

    private final class RefreshTask implements Runnable {

        private RefreshTask() {
        }

        @Override
        public void run() {
            try {
                ClientConnectionManager connectionManager = client.getConnectionManager();
                Connection connection = connectionManager.getOwnerConnection();
                if (connection == null) {
                    return;
                }
                ClientMessage requestMessage = ClientGetPartitionsCodec.encodeRequest();
                ClientInvocationFuture future = new ClientInvocation(client, requestMessage, null).invokeUrgent();
                future.andThen(refreshTaskCallback);
            } catch (Exception e) {
                if (client.getLifecycleService().isRunning()) {
                    logger.warning("Error while fetching cluster partition table!", e);
                }
            }
        }
    }

    private class RefreshTaskCallback implements ExecutionCallback<ClientMessage> {

        @Override
        public void onResponse(ClientMessage responseMessage) {
            if (responseMessage == null) {
                return;
            }
            ClientGetPartitionsCodec.ResponseParameters response = ClientGetPartitionsCodec.decodeResponse(responseMessage);
            processPartitionResponse(response.partitions, response.partitionStateVersion, response.partitionStateVersionExist);
        }

        @Override
        public void onFailure(Throwable t) {
            if (client.getLifecycleService().isRunning()) {
                logger.warning("Error while fetching cluster partition table!", t);
            }
        }
    }
}
