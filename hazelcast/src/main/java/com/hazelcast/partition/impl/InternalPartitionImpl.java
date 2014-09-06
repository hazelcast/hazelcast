package com.hazelcast.partition.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.lang.System.arraycopy;

class InternalPartitionImpl implements InternalPartition {

    private static final AtomicReferenceFieldUpdater<InternalPartitionImpl, Address[]> ADDRESSES_UPDATER
            = AtomicReferenceFieldUpdater.newUpdater(InternalPartitionImpl.class, Address[].class, "addresses");

    //The content of this array will never be updated, so it can be safely read using a volatile read.
    //Writing to 'addresses' is done using the 'addressUpdater' AtomicReferenceFieldUpdater which involves a
    //cas to prevent lost updates.
    //The old approach relied on a AtomicReferenceArray, but this performed a lot slower that the current approach.
    //Number of reads will outweigh the number of writes to the field.

    @edu.umd.cs.findbugs.annotations.SuppressWarnings("VO_VOLATILE_REFERENCE_TO_ARRAY")
    volatile Address[] addresses = new Address[MAX_REPLICA_COUNT];
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

    void setOwner(Address ownerAddress) {
        setReplicaAddress(0, ownerAddress);
    }

    @Override
    public Address getReplicaAddress(int replicaIndex) {
        return addresses[replicaIndex];
    }

    void setReplicaAddress(int replicaIndex, Address newAddress) {
        boolean changed = false;
        Address oldAddress;
        for (;;) {
            Address[] oldAddresses = addresses;
            oldAddress = oldAddresses[replicaIndex];
            if (partitionListener != null) {
                if (oldAddress == null) {
                    changed = newAddress != null;
                } else {
                    changed = !oldAddress.equals(newAddress);
                }
            }

            Address[] newAddresses = createNewAddresses(replicaIndex, newAddress, oldAddresses);
            if (ADDRESSES_UPDATER.compareAndSet(this, oldAddresses, newAddresses)) {
                break;
            }
        }

        if (changed) {
            PartitionReplicaChangeEvent event
                    = new PartitionReplicaChangeEvent(partitionId, replicaIndex, oldAddress, newAddress);
            partitionListener.replicaChanged(event);
        }
    }

    private Address[] createNewAddresses(int replicaIndex, Address newAddress, Address[] oldAddresses) {
        Address[] newAddresses = new Address[MAX_REPLICA_COUNT];
        arraycopy(oldAddresses, 0, newAddresses, 0, MAX_REPLICA_COUNT);
        newAddresses[replicaIndex] = newAddress;
        return newAddresses;
    }

    boolean onDeadAddress(Address deadAddress) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (!deadAddress.equals(addresses[i])) {
                continue;
            }

            for (int a = i; a + 1 < MAX_REPLICA_COUNT; a++) {
                setReplicaAddress(a, addresses[a + 1]);
            }
            setReplicaAddress(MAX_REPLICA_COUNT - 1, null);
            return true;
        }
        return false;
    }

    void setPartitionInfo(Address[] replicas) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            setReplicaAddress(i, replicas[i]);
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
