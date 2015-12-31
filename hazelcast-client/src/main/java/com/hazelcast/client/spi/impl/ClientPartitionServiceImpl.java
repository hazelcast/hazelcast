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

package com.hazelcast.client.spi.impl;

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
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.HashUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link ClientPartitionService} implementation.
 */
public final class ClientPartitionServiceImpl
        implements ClientPartitionService {

    private static final ILogger LOGGER = Logger.getLogger(ClientPartitionService.class);
    private static final long PERIOD = 10;
    private static final long INITIAL_DELAY = 10;
    private static final int PARTITION_WAIT_TIME = 1000;
    private final ExecutionCallback<ClientMessage> refreshTaskCallback = new RefreshTaskCallback();

    private final HazelcastClientInstanceImpl client;

    private final ConcurrentHashMap<Integer, Address> partitions = new ConcurrentHashMap<Integer, Address>(271, 0.75f, 1);

    private final AtomicBoolean updating = new AtomicBoolean(false);

    private volatile int partitionCount;

    public ClientPartitionServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    public void start() {
        ClientExecutionServiceImpl clientExecutionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        // Use internal execution service for all partition refresh process (Do not use the user executor thread)
        ExecutorService internalExecutor = clientExecutionService.getInternalExecutor();
        clientExecutionService.scheduleWithFixedDelay(new RefreshTask(internalExecutor), INITIAL_DELAY, PERIOD, TimeUnit.SECONDS);
    }

    public void refreshPartitions() {
        ClientExecutionServiceImpl executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        try {
            // Use internal execution service for all partition refresh process (Do not use the user executor thread)
            ExecutorService internalExecutor = executionService.getInternalExecutor();
            executionService.submitInternal(new RefreshTask(internalExecutor));
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
        ClientClusterService clusterService = client.getClientClusterService();
        Address ownerAddress = clusterService.getOwnerConnectionAddress();
        if (ownerAddress == null) {
            return null;
        }
        Connection connection = client.getConnectionManager().getConnection(ownerAddress);
        if (connection == null) {
            return null;
        }
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
                LOGGER.warning("Error while fetching cluster partition table!", e);
            }
        }
        return false;
    }

    private ClientInvocationFuture getPartitionsFrom(Connection connection) {
        ClientMessage requestMessage = ClientGetPartitionsCodec.encodeRequest();
        return new ClientInvocation(client, requestMessage, connection).invokeUrgent();
    }

    private boolean processPartitionResponse(ClientGetPartitionsCodec.ResponseParameters response) {
        LOGGER.finest("Processing partition response.");
        Map<Address, List<Integer>> partitionResponse = response.partitions;
        for (Map.Entry<Address, List<Integer>> entry : partitionResponse.entrySet()) {
            Address address = entry.getKey();
            for (Integer partition : entry.getValue()) {
                this.partitions.put(partition, address);
            }
        }
        partitionCount = partitions.size();
        return partitionResponse.size() > 0;
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

    private final class PartitionImpl
            implements Partition {

        private final int partitionId;

        private PartitionImpl(int partitionId) {
            this.partitionId = partitionId;
        }

        public int getPartitionId() {
            return partitionId;
        }

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

    private class RefreshTask implements Runnable {
        private ExecutorService executionService;

        public RefreshTask(ExecutorService service) {
            this.executionService = service;
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
                clientInvocationFuture.andThen(refreshTaskCallback, executionService);
            } catch (Exception e) {
                if (client.getLifecycleService().isRunning()) {
                    LOGGER.warning("Error while fetching cluster partition table!", e);
                }
            } finally {
                updating.set(false);
            }
        }
    }
    private class RefreshTaskCallback
            implements ExecutionCallback<ClientMessage> {
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
                LOGGER.warning("Error while fetching cluster partition table!", t);
            }
            updating.set(false);
        }
    }
}
