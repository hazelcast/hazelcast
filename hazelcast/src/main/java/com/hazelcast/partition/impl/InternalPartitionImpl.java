package com.hazelcast.partition.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;

import java.util.Arrays;

class InternalPartitionImpl implements InternalPartition {

    // The content of this array will never be updated, so it can be safely read using a volatile read.
    // Writing to 'addresses' is done under InternalPartitionServiceImpl.lock,
    // so there's no need to guard `addresses` field or to use a CAS.
    @edu.umd.cs.findbugs.annotations.SuppressWarnings("VO_VOLATILE_REFERENCE_TO_ARRAY")
    private volatile Address[] addresses = new Address[MAX_REPLICA_COUNT];
    private final int partitionId;
    private final PartitionListener partitionListener;
    private volatile boolean isMigrating;

    InternalPartitionImpl(int partitionId, PartitionListener partitionListener) {
        this.partitionId = partitionId;
        this.partitionListener = partitionListener;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean isMigrating() {
        return isMigrating;
    }

    public void setMigrating(boolean isMigrating) {
        this.isMigrating = isMigrating;
    }

    @Override
    public Address getOwnerOrNull() {
        return addresses[0];
    }

    @Override
    public Address getReplicaAddress(int replicaIndex) {
        return addresses[replicaIndex];
    }

    // This method is called under InternalPartitionServiceImpl.lock,
    // so there's no need to guard `addresses` field or to use a CAS.
    boolean onDeadAddress(Address deadAddress) {
        Address[] currentAddresses = addresses;
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (!deadAddress.equals(currentAddresses[i])) {
                continue;
            }

            Address[] newAddresses = Arrays.copyOf(addresses, MAX_REPLICA_COUNT);
            for (int a = i; a + 1 < MAX_REPLICA_COUNT; a++) {
                newAddresses[a] = newAddresses[a + 1];
            }
            newAddresses[MAX_REPLICA_COUNT - 1] = null;
            addresses = newAddresses;
            callPartitionListener(newAddresses, currentAddresses);
            return true;
        }
        return false;
    }


    // Not doing a defensive copy of given Address[]
    // This method is called under InternalPartitionServiceImpl.lock,
    // so there's no need to guard `addresses` field or to use a CAS.
    void setReplicaAddresses(Address[] newAddresses) {
        Address[] oldAddresses = addresses;
        addresses = newAddresses;
        callPartitionListener(newAddresses, oldAddresses);

    }

    private void callPartitionListener(Address[] newAddresses, Address[] oldAddresses) {
        if (partitionListener != null) {
            for (int replicaIndex = 0; replicaIndex < MAX_REPLICA_COUNT; replicaIndex++) {
                Address oldAddress = oldAddresses[replicaIndex];
                Address newAddress = newAddresses[replicaIndex];
                callPartitionListener(replicaIndex, oldAddress, newAddress);
            }
        }
    }

    private void callPartitionListener(int replicaIndex, Address oldAddress, Address newAddress) {
        boolean changed;
        if (oldAddress == null) {
            changed = newAddress != null;
        } else {
            changed = !oldAddress.equals(newAddress);
        }
        if (changed) {
            PartitionReplicaChangeEvent event
                    = new PartitionReplicaChangeEvent(partitionId, replicaIndex, oldAddress, newAddress);
            partitionListener.replicaChanged(event);
        }
    }

    @Override
    public boolean isOwnerOrBackup(Address address) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (address.equals(getReplicaAddress(i))) {
                return true;
            }
        }
        return false;
    }

    int getReplicaIndex(Address address) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (address.equals(getReplicaAddress(i))) {
                return i;
            }
        }
        return -1;
    }

    void reset() {
        addresses = new Address[MAX_REPLICA_COUNT];
        setMigrating(false);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Partition [").append(partitionId).append("]{\n");
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            Address address = addresses[i];
            if (address != null) {
                sb.append('\t');
                sb.append(i).append(":").append(address);
                sb.append("\n");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
