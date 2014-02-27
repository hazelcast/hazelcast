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

package com.hazelcast.partition.impl;

import com.hazelcast.nio.Address;

public class PartitionReplicaChangeEvent {
    private int partitionId;
    private int replicaIndex;
    private Address oldAddress;
    private Address newAddress;

    public PartitionReplicaChangeEvent(int partitionId, int replicaIndex, Address oldAddress, Address newAddress) {
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.oldAddress = oldAddress;
        this.newAddress = newAddress;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public Address getOldAddress() {
        return oldAddress;
    }

    public Address getNewAddress() {
        return newAddress;
    }

    @Override
    public String toString() {
        return "PartitionReplicaChangeEvent{"
                + "partitionId=" + partitionId
                + ", replicaIndex=" + replicaIndex
                + ", oldAddress=" + oldAddress
                + ", newAddress=" + newAddress
                + '}';
    }
}
