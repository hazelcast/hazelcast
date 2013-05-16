package com.hazelcast.client.spi;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.client.GetPartitionsRequest;
import com.hazelcast.partition.client.PartitionsResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @mdogan 5/16/13
 */
public final class ClientPartitionService {

    private final HazelcastClient client;

    private volatile int partitionCount;

    private ConcurrentHashMap<Integer, Address> partitions;

    public ClientPartitionService(HazelcastClient client) {
        this.client = client;
    }

    public void start() {
        getInitialPartitions();
        System.err.println("partitionCount = " + partitionCount);
        client.getExecutionService().scheduleWithFixedDelay(new Runnable() {
            public void run() {
                final ClientClusterService clusterService = client.getClusterService();
                final Address master = clusterService.getMasterAddress();
                final PartitionsResponse response = getPartitionsFrom(clusterService, master);
                if (response != null) {
                    processPartitionResponse(response);
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    private void getInitialPartitions() {
        final ClientClusterService clusterService = client.getClusterService();
        final Collection<MemberImpl> memberList = clusterService.getMemberList();
        for (MemberImpl member : memberList) {
            final Address target = member.getAddress();
            PartitionsResponse response = getPartitionsFrom(clusterService, target);
            if (response != null) {
                processPartitionResponse(response);
                return;
            }
        }
        throw new IllegalStateException("Cannot get initial partitions!");
    }

    private PartitionsResponse getPartitionsFrom(ClientClusterService clusterService, Address address) {
        try {
            return clusterService.sendAndReceive(address, new GetPartitionsRequest());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void processPartitionResponse(PartitionsResponse response) {
        final Address[] members = response.getMembers();
        final int[] ownerIndexes = response.getOwnerIndexes();
        if (partitionCount == 0) {
            partitions = new ConcurrentHashMap<Integer, Address>(271, 0.75f, 1);
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

    }

    public Address getPartitionOwner(int partitionId) {
        return partitions.get(partitionId);
    }

    public int getPartitionId(Data key) {
        final int pc = partitionCount;
        int hash = key.getPartitionHash();
        return (hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash) % pc;
    }

    public int getPartitionId(Object key) {
        final Data data = client.getSerializationService().toData(key);
        return getPartitionId(data);
    }

    public int getPartitionCount() {
        return partitionCount;
    }

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
                return client.getClusterService().getMember(owner);
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
