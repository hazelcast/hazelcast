package com.hazelcast.partition;

import com.hazelcast.nio.Address;

import java.util.Arrays;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * A DTO for Partition Information.
 */
public class PartitionInfo {
    private final Address[] addresses;
    private final int partitionId;

    public PartitionInfo(int partitionId, Address[] addresses) {
        this.addresses = isNotNull(addresses, "addresses");
        this.partitionId = partitionId;
    }

    public Address getReplicaAddress(int index) {
        return addresses[index];
    }

    //Internal structure, so it doesn't matter if we expose this array.
    public Address[] getReplicaAddresses() {
        return addresses;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getReplicaCount() {
        return addresses.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionInfo that = (PartitionInfo) o;

        if (partitionId != that.partitionId) {
            return false;
        }
        if (!Arrays.equals(addresses, that.addresses)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(addresses);
        result = 31 * result + partitionId;
        return result;
    }

    @Override
    public String toString() {
        return "PartitionInfo{"
                + "addresses=" + Arrays.toString(addresses)
                + ", partitionId=" + partitionId
                + '}';
    }
}
