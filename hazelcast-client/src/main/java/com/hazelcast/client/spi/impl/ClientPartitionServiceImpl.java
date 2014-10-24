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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.client.GetPartitionsRequest;
import com.hazelcast.partition.client.PartitionsResponse;
import com.hazelcast.util.EmptyStatement;

import java.util.Collection;
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

    private final HazelcastClientInstanceImpl client;

    private final ConcurrentHashMap<Integer, Address> partitions = new ConcurrentHashMap<Integer, Address>(271, 0.75f, 1);

    private final AtomicBoolean updating = new AtomicBoolean(false);

    private volatile int partitionCount;

    public ClientPartitionServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    public void start() {
        getInitialPartitions();
        ClientExecutionService clientExecutionService = client.getClientExecutionService();
        clientExecutionService.scheduleWithFixedDelay(new RefreshTask(), INITIAL_DELAY, PERIOD, TimeUnit.SECONDS);
    }

    public void refreshPartitions() {
        try {
            client.getClientExecutionService().execute(new RefreshTask());
        } catch (RejectedExecutionException ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    private void getInitialPartitions() {
        ClientClusterService clusterService = client.getClientClusterService();
        Collection<MemberImpl> memberList = clusterService.getMemberList();
        for (MemberImpl member : memberList) {
            Address target = member.getAddress();
            PartitionsResponse response = getPartitionsFrom(target);
            if (response != null) {
                processPartitionResponse(response);
                return;
            }
        }
        throw new IllegalStateException("Cannot get initial partitions!");
    }

    private PartitionsResponse getPartitionsFrom(Address address) {
        try {
            ClientInvocationService invocationService = client.getInvocationService();
            Future<PartitionsResponse> future = invocationService.invokeOnTarget(new GetPartitionsRequest(), address);
            return client.getSerializationService().toObject(future.get());
        } catch (Exception e) {
            if (client.getLifecycleService().isRunning()) {
                LOGGER.severe("Error while fetching cluster partition table!", e);
            }
        }
        return null;
    }

    private void processPartitionResponse(PartitionsResponse response) {
        Address[] members = response.getMembers();
        int[] ownerIndexes = response.getOwnerIndexes();
        if (partitionCount == 0) {
            partitionCount = ownerIndexes.length;
        }
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            final int ownerIndex = ownerIndexes[partitionId];
            if (ownerIndex > -1) {
                partitions.put(partitionId, members[ownerIndex]);
            }
        }
    }

    public void stop() {
        partitions.clear();
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
        return partitions.get(partitionId);
    }

    @Override
    public int getPartitionId(Data key) {
        final int pc = partitionCount;
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
                ClientClusterService clusterService = client.getClientClusterService();
                Address master = clusterService.getMasterAddress();
                PartitionsResponse response = getPartitionsFrom(master);
                if (response != null) {
                    processPartitionResponse(response);
                }
            } catch (HazelcastInstanceNotActiveException ignored) {
                EmptyStatement.ignore(ignored);
            } finally {
                updating.set(false);
            }
        }
    }
}
