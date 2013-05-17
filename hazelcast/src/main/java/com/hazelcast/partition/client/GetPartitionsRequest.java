package com.hazelcast.partition.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionDataSerializerHook;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.partition.PartitionServiceImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @mdogan 5/13/13
 */
public final class GetPartitionsRequest extends CallableClientRequest implements IdentifiedDataSerializable {

    public Object call() throws Exception {
        final PartitionServiceImpl service = getService();
        service.firstArrangement();
        final ClusterService clusterService = getClientEngine().getClusterService();
        final Collection<MemberImpl> memberList = clusterService.getMemberList();
        final Address[] addresses = new Address[memberList.size()];
        final Map<Address, Integer> addressMap = new HashMap<Address, Integer>(memberList.size());
        int k = 0;
        for (MemberImpl member : memberList) {
            final Address address = member.getAddress();
            addresses[k] = address;
            addressMap.put(address, k);
            k++;
        }
        final PartitionInfo[] partitions = service.getPartitions();
        final int[] indexes = new int[partitions.length];
        for (int i = 0; i < indexes.length; i++) {
            final Address owner = partitions[i].getOwner();
            int index = -1;
            if (owner != null) {
                final Integer idx = addressMap.get(owner);
                if (idx != null) {
                    index = idx;
                }

            }
            indexes[i] = index;
        }
        return new PartitionsResponse(addresses, indexes);
    }

    @Override
    public String getServiceName() {
        return PartitionServiceImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.GET_PARTITIONS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
