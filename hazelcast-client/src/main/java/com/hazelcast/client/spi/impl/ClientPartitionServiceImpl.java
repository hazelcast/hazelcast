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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.spi.ClientClusterService;
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

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mdogan 5/16/13
 */
public final class ClientPartitionServiceImpl implements ClientPartitionService {

    private static final ILogger logger = Logger.getLogger(ClientPartitionService.class);

    private final HazelcastClient client;

    private final ConcurrentHashMap<Integer, Address> partitions = new ConcurrentHashMap<Integer, Address>(271, 0.75f, 1);

    private final AtomicBoolean updating = new AtomicBoolean(false);

    private volatile int partitionCount;

    public ClientPartitionServiceImpl(HazelcastClient client) {
        this.client = client;
    }

    public void start() {
        getInitialPartitions();
        client.getClientExecutionService().scheduleWithFixedDelay(new RefreshTask(), 10, 10, TimeUnit.SECONDS);
    }

    public void refreshPartitions() {
        try {
            client.getClientExecutionService().execute(new RefreshTask());
        } catch (RejectedExecutionException ignored) {
        }
    }

    private class RefreshTask implements Runnable {
        public void run() {
            if (updating.compareAndSet(false, true)) {
                try {
                    final ClientClusterService clusterService = client.getClientClusterService();
                    final Address master = clusterService.getMasterAddress();
                    final PartitionsResponse response = getPartitionsFrom(master);
                    if (response != null) {
                        processPartitionResponse(response);
                    }
                } catch (HazelcastInstanceNotActiveException ignored) {
                } finally {
                    updating.set(false);
                }
            }
        }
    }

    private void getInitialPartitions() {
        final ClientClusterService clusterService = client.getClientClusterService();
        final Collection<MemberImpl> memberList = clusterService.getMemberList();
        for (MemberImpl member : memberList) {
            final Address target = member.getAddress();
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
            final Future<PartitionsResponse> future = client.getInvocationService().invokeOnTarget(new GetPartitionsRequest(), address);
            return client.getSerializationService().toObject(future.get());
        } catch (Exception e) {
            logger.severe("Error while fetching cluster partition table!", e);
        }
        return null;
    }

    private void processPartitionResponse(PartitionsResponse response) {
        final Address[] members = response.getMembers();
        final int[] ownerIndexes = response.getOwnerIndexes();
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

    private class PartitionImpl implements Partition {

        private final int partitionId;

        private PartitionImpl(int partitionId) {
            this.partitionId = partitionId;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public Member getOwner() {
            final Address owner = getPartitionOwner(partitionId);
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
}
