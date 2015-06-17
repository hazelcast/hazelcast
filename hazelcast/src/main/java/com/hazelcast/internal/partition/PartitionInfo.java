/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.nio.Address;

import java.util.Arrays;

import static com.hazelcast.util.Preconditions.isNotNull;

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
    @edu.umd.cs.findbugs.annotations.SuppressWarnings("EI_EXPOSE_REP")
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
