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

package com.hazelcast.partition;

import com.hazelcast.nio.Address;

import java.util.concurrent.atomic.AtomicReferenceArray;

public final class PartitionInfo {

    public static final int MAX_REPLICA_COUNT = 7;
    public static final int MAX_BACKUP_COUNT = MAX_REPLICA_COUNT - 1;

    private final int partitionId;
    private final AtomicReferenceArray<Address> addresses = new AtomicReferenceArray<Address>(MAX_REPLICA_COUNT);
    private final PartitionListener partitionListener;

    PartitionInfo(int partitionId, PartitionListener partitionListener) {
        this.partitionId = partitionId;
        this.partitionListener = partitionListener;
    }

    PartitionInfo(int partitionId) {
        this(partitionId, null);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Address getOwner() {
        return addresses.get(0);
    }

    void setOwner(Address ownerAddress) {
        setReplicaAddress(0, ownerAddress);
    }

    public Address getReplicaAddress(int index) {
        return (addresses.length() > index)
                ? addresses.get(index) : null;
    }

    void setReplicaAddress(int index, Address address) {
        boolean changed = false;
        Address currentAddress = addresses.get(index);
        if (partitionListener != null) {
            if (currentAddress == null) {
                changed = (address != null);
            } else {
                changed = !currentAddress.equals(address);
            }
        }
        addresses.set(index, address);
        if (changed) {
            partitionListener.replicaChanged(new PartitionReplicaChangeEvent(partitionId, index, currentAddress, address));
        }
    }

    boolean onDeadAddress(Address deadAddress) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (deadAddress.equals(addresses.get(i))) {
                for (int a = i; a + 1 < MAX_REPLICA_COUNT; a++) {
                    setReplicaAddress(a, addresses.get(a + 1));
                }
                setReplicaAddress(MAX_REPLICA_COUNT - 1, null);
                return true;
            }
        }
        return false;
    }

    void setPartitionInfo(PartitionInfo partitionInfo) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            setReplicaAddress(i, partitionInfo.getReplicaAddress(i));
        }
    }

    public boolean isBackup(Address address) {
        for (int i = 1; i < MAX_REPLICA_COUNT; i++) {
            if (address.equals(getReplicaAddress(i))) {
                return true;
            }
        }
        return false;
    }

    public boolean isOwnerOrBackup(Address address) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (address.equals(getReplicaAddress(i))) {
                return true;
            }
        }
        return false;
    }

    public int getReplicaIndexOf(Address address) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (address.equals(addresses.get(i))) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionInfo that = (PartitionInfo) o;
        if (partitionId != that.partitionId) return false;
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            Address a1 = addresses.get(i);
            Address a2 = that.addresses.get(i);
            if (a1 == null) {
                if (a2 != null) {
                    return false;
                }
            } else if (!a1.equals(a2)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            Address address = addresses.get(i);
            result = 31 * result + (address != null ? address.hashCode() : 0);
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Partition [")
                .append(partitionId).append("]{\n");
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            Address address = addresses.get(i);
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
