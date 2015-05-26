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
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link ClientPartitionService} implementation.
 */
public final class ClientPartitionServiceImpl implements ClientPartitionService {

    private static final ILogger LOGGER = Logger.getLogger(ClientPartitionService.class);
    private static final long PERIOD = 10;
    private static final long INITIAL_DELAY = 10;
    private static final int PARTITION_WAIT_TIME = 1000;

    private final HazelcastClientInstanceImpl client;

    private final ConcurrentHashMap<Integer, Address> partitions = new ConcurrentHashMap<Integer, Address>(271, 0.75f, 1);

    private final AtomicBoolean updating = new AtomicBoolean(false);

    private volatile int partitionCount;

    public ClientPartitionServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    public void start() {
        ClientExecutionService clientExecutionService = client.getClientExecutionService();
        clientExecutionService.scheduleWithFixedDelay(new RefreshTask(), INITIAL_DELAY, PERIOD, TimeUnit.SECONDS);
    }

    public void refreshPartitions() {
        ClientExecutionServiceImpl executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        try {
            executionService.executeInternal(new RefreshTask());
        } catch (RejectedExecutionException ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    private void getPartitionsBlocking() {
        while (!getPartitions() && client.getConnectionManager().isAlive()) {
            try {
                Thread.sleep(PARTITION_WAIT_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean getPartitions() {
        ClientClusterService clusterService = client.getClientClusterService();
        Address ownerAddress = clusterService.getOwnerConnectionAddress();
        if (ownerAddress == null) {
            return false;
        }
        Connection connection = client.getConnectionManager().getConnection(ownerAddress);
        ClientGetPartitionsCodec.ResponseParameters response = getPartitionsFrom(connection);
        if (response != null) {
            return processPartitionResponse(response);
        }
        return false;
    }

    private ClientGetPartitionsCodec.ResponseParameters getPartitionsFrom(Connection connection) {
        if (connection == null) {
            return null;
        }
        try {
            ClientMessage requestMessage = ClientGetPartitionsCodec.encodeRequest();
            Future<ClientMessage> future = new ClientInvocation(client, requestMessage, connection).invoke();
            ClientMessage responseMessage = future.get();
            return ClientGetPartitionsCodec.decodeResponse(responseMessage);
        } catch (Exception e) {
            if (client.getLifecycleService().isRunning()) {
                LOGGER.severe("Error while fetching cluster partition table!", e);
            }
        }
        return null;
    }

    private boolean processPartitionResponse(ClientGetPartitionsCodec.ResponseParameters response) {
        int[] ownerIndexes = response.ownerIndexes;
        if (ownerIndexes.length == 0) {
            return false;
        }
        Address[] members = response.members;
        if (partitionCount == 0) {
            partitionCount = ownerIndexes.length;
        }
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            final int ownerIndex = ownerIndexes[partitionId];
            if (ownerIndex > -1) {
                partitions.put(partitionId, members[ownerIndex]);
            }
        }
        return true;
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
        return (hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash) % pc;
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
            final StringBuilder sb = new StringBuilder("PartitionImpl{");
            sb.append("partitionId=").append(partitionId);
            sb.append('}');
            return sb.toString();
        }
    }

    private class RefreshTask implements Runnable {

        @Override
        public void run() {
            if (!updating.compareAndSet(false, true)) {
                return;
            }

            try {
                getPartitions();
            } catch (HazelcastInstanceNotActiveException ignored) {
                EmptyStatement.ignore(ignored);
            } finally {
                updating.set(false);
            }
        }
    }
}
