/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.HashUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link ClientPartitionService} implementation.
 */
public final class ClientPartitionServiceImpl implements ClientPartitionService {

    private static final long PERIOD = 10;
    private static final long INITIAL_DELAY = 10;
    private static final int PARTITION_WAIT_TIME = 1000;

    private final ExecutionCallback<ClientMessage> refreshTaskCallback = new RefreshTaskCallback();
    private final ConcurrentHashMap<Integer, Address> partitions = new ConcurrentHashMap<Integer, Address>(271, 0.75f, 1);
    private final AtomicBoolean updating = new AtomicBoolean(false);
    private final ClientExecutionServiceImpl clientExecutionService ;
    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;

    private volatile int partitionCount;

    public ClientPartitionServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClientPartitionService.class);
        clientExecutionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
    }

    public void start() {
        // use internal execution service for all partition refresh process (do not use the user executor thread)
        clientExecutionService.scheduleWithRepetition(new RefreshTask(), INITIAL_DELAY, PERIOD, TimeUnit.SECONDS);
    }

    public void refreshPartitions() {
        ClientExecutionServiceImpl executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        try {
            // use internal execution service for all partition refresh process (do not use the user executor thread)
            executionService.execute(new RefreshTask());
        } catch (RejectedExecutionException ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    private void getPartitionsBlocking() {
        while (!getPartitions() && client.getConnectionManager().isAlive()) {
            if (isClusterFormedByOnlyLiteMembers()) {
                throw new NoDataMemberInClusterException(
                        "Partitions can't be assigned since all nodes in the cluster are lite members");
            }
            try {
                Thread.sleep(PARTITION_WAIT_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isClusterFormedByOnlyLiteMembers() {
        final ClientClusterService clusterService = client.getClientClusterService();
        return clusterService.getMembers(MemberSelectors.DATA_MEMBER_SELECTOR).isEmpty();
    }

    private Connection getOwnerConnection() {
        ClientConnectionManager connectionManager = client.getConnectionManager();
        Connection connection = connectionManager.getOwnerConnection();
        return connection;
    }

    private boolean getPartitions() {
        Connection connection = getOwnerConnection();
        if (connection == null) {
            return false;
        }
        try {
            Future<ClientMessage> future = getPartitionsFrom(connection);
            ClientMessage responseMessage = future.get();
            ClientGetPartitionsCodec.ResponseParameters response = ClientGetPartitionsCodec.decodeResponse(responseMessage);
            if (response == null) {
                return false;
            }
            return processPartitionResponse(response);
        } catch (Exception e) {
            if (client.getLifecycleService().isRunning()) {
                logger.warning("Error while fetching cluster partition table!", e);
            }
        }
        return false;
    }

    private ClientInvocationFuture getPartitionsFrom(Connection connection) {
        ClientMessage requestMessage = ClientGetPartitionsCodec.encodeRequest();
        return new ClientInvocation(client, requestMessage, connection).invokeUrgent();
    }

    private boolean processPartitionResponse(ClientGetPartitionsCodec.ResponseParameters response) {
        logger.finest("Processing partition response.");
        List<Map.Entry<Address, List<Integer>>> partitions = response.partitions;
        for (Map.Entry<Address, List<Integer>> entry : partitions) {
            Address address = entry.getKey();
            for (Integer partition : entry.getValue()) {
                this.partitions.put(partition, address);
            }
        }
        partitionCount = this.partitions.size();
        return partitions.size() > 0;
    }

    public void stop() {
        partitions.clear();
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
        Address address = partitions.get(partitionId);
        if (address == null) {
            getPartitionsBlocking();
        }
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
        if (partitionCount == 0) {
            getPartitionsBlocking();
        }
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
            if (!updating.compareAndSet(false, true)) {
                return;
            }

            Connection connection = getOwnerConnection();
            if (connection == null) {
                updating.set(false);
                return;
            }

            try {
                ClientInvocationFuture clientInvocationFuture = getPartitionsFrom(connection);
                clientInvocationFuture.andThen(refreshTaskCallback);
            } catch (Exception e) {
                if (client.getLifecycleService().isRunning()) {
                    logger.warning("Error while fetching cluster partition table!", e);
                }
                updating.set(false);
            }
        }
    }

    private class RefreshTaskCallback implements ExecutionCallback<ClientMessage> {

        @Override
        public void onResponse(ClientMessage responseMessage) {
            try {
                if (responseMessage == null) {
                    return;
                }
                ClientGetPartitionsCodec.ResponseParameters response = ClientGetPartitionsCodec.decodeResponse(responseMessage);
                processPartitionResponse(response);
            } finally {
                updating.set(false);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (client.getLifecycleService().isRunning()) {
                logger.warning("Error while fetching cluster partition table!", t);
            }
            updating.set(false);
        }
    }
}
