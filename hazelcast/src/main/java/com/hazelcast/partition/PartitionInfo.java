package com.hazelcast.partition;

import com.hazelcast.nio.Address;

/**
 * A DTO for Partition Information.
 */
public class PartitionInfo {
    private final Address[] addresses;
    private final int partitionId;

    public PartitionInfo(int partitionId, Address[] addresses) {
        this.addresses = addresses;
        this.partitionId = partitionId;
    }

    public Address getReplicaAddress(int index){
        return addresses[index];
    }

    public Address[] getReplicaAddresses(){
        return addresses;
    }

    public int getPartitionId(){
        return partitionId;
    }

    public int getReplicaCount(){
        return addresses.length;
    }
}
