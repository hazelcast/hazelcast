/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.nio.Address;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

public class InternalPartitionImpl implements InternalPartition {

    @SuppressFBWarnings(value = "VO_VOLATILE_REFERENCE_TO_ARRAY", justification =
            "The contents of this array will never be updated, so it can be safely read using a volatile read."
                    + " Writing to `addresses` is done under InternalPartitionServiceImpl.lock,"
                    + " so there's no need to guard `addresses` field or to use a CAS.")
    private volatile Address[] addresses = new Address[MAX_REPLICA_COUNT];
    private final int partitionId;
    private final PartitionListener partitionListener;
    private final Address thisAddress;
    private volatile boolean isMigrating;

    InternalPartitionImpl(int partitionId, PartitionListener partitionListener, Address thisAddress) {
        this.partitionId = partitionId;
        this.partitionListener = partitionListener;
        this.thisAddress = thisAddress;
    }

    InternalPartitionImpl(int partitionId, PartitionListener listener, Address thisAddress,
            Address[] addresses) {
        this(partitionId, listener, thisAddress);
        this.addresses = addresses;
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
    public boolean isLocal() {
        return thisAddress.equals(getOwnerOrNull());
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
    int removeAddress(Address deadAddress) {
        Address[] currentAddresses = addresses;
        Address[] newAddresses = Arrays.copyOf(addresses, MAX_REPLICA_COUNT);

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (!deadAddress.equals(currentAddresses[i])) {
                continue;
            }

            newAddresses[i] = null;
            addresses = newAddresses;
            callPartitionListener(i, deadAddress, null);
            return i;
        }
        return -1;
    }

    void swapAddresses(int index1, int index2) {
        Address[] newAddresses = Arrays.copyOf(addresses, MAX_REPLICA_COUNT);

        Address a1 = newAddresses[index1];
        Address a2 = newAddresses[index2];
        newAddresses[index1] = a2;
        newAddresses[index2] = a1;

        addresses = newAddresses;
        callPartitionListener(index1, a1, a2);
        callPartitionListener(index2, a2, a1);
    }

    // Not doing a defensive copy of given Address[]
    // This method is called under InternalPartitionServiceImpl.lock,
    // so there's no need to guard `addresses` field or to use a CAS.
    void setInitialReplicaAddresses(Address[] newAddresses) {
        Address[] oldAddresses = addresses;
        for (int replicaIndex = 0; replicaIndex < MAX_REPLICA_COUNT; replicaIndex++) {
            if (oldAddresses[replicaIndex] != null) {
                throw new IllegalStateException("Partition is already initialized!");
            }
        }
        addresses = newAddresses;
    }

    // Not doing a defensive copy of given Address[]
    // This method is called under InternalPartitionServiceImpl.lock,
    // so there's no need to guard `addresses` field or to use a CAS.
    void setReplicaAddresses(Address[] newAddresses) {
        Address[] oldAddresses = addresses;
        addresses = newAddresses;
        callPartitionListener(newAddresses, oldAddresses);
    }

    void setReplicaAddress(int replicaIndex, Address newAddress) {
        Address[] newAddresses = Arrays.copyOf(addresses, MAX_REPLICA_COUNT);
        Address oldAddress = newAddresses[replicaIndex];
        newAddresses[replicaIndex] = newAddress;
        addresses = newAddresses;
        callPartitionListener(replicaIndex, oldAddress, newAddress);
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

    InternalPartitionImpl copy(PartitionListener listener) {
        return new InternalPartitionImpl(partitionId, listener, thisAddress, Arrays.copyOf(addresses, MAX_REPLICA_COUNT));
    }

    Address[] getReplicaAddresses() {
        return addresses;
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
    public int getReplicaIndex(Address address) {
        return getReplicaIndex(addresses, address);
    }

    public static int getReplicaIndex(Address[] addresses, Address address) {
        if (address == null) {
            return -1;
        }

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (address.equals(addresses[i])) {
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
