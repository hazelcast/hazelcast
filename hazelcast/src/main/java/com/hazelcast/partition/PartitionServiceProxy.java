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

import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.Partition;
import com.hazelcast.nio.Address;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PartitionServiceProxy implements com.hazelcast.core.PartitionService {

    private final InternalPartitionService partitionService;
    private final ConcurrentMap<Integer, PartitionProxy> mapPartitions
            = new ConcurrentHashMap<Integer, PartitionProxy>();
    private final Set<Partition> partitions = new TreeSet<Partition>();
    private final Random random = new Random();

    public PartitionServiceProxy(InternalPartitionService partitionService) {
        this.partitionService = partitionService;
        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            PartitionProxy partitionProxy = new PartitionProxy(i);
            partitions.add(partitionProxy);
            mapPartitions.put(i, partitionProxy);
        }
    }

    @Override
    public String randomPartitionKey() {
        return Integer.toString(random.nextInt(partitionService.getPartitionCount()));
    }

    @Override
    public Set<Partition> getPartitions() {
        return partitions;
    }

    @Override
    public PartitionProxy getPartition(Object key) {
        int partitionId = partitionService.getPartitionId(key);
        return getPartition(partitionId);
    }

    @Override
    public String addMigrationListener(final MigrationListener migrationListener) {
        return partitionService.addMigrationListener(migrationListener);
    }

    @Override
    public boolean removeMigrationListener(final String registrationId) {
        return partitionService.removeMigrationListener(registrationId);
    }

    public PartitionProxy getPartition(int partitionId) {
        return mapPartitions.get(partitionId);
    }

    public class PartitionProxy implements Partition, Comparable {

        final int partitionId;

        PartitionProxy(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public Member getOwner() {
            Address address = partitionService.getPartitionOwner(partitionId);
            if (address == null) {
                return null;
            }

            //todo: why are we calling the partitionService twice, why don't we immediately get the member?
            return partitionService.getMember(address);
        }

        @Override
        public int compareTo(Object o) {
            PartitionProxy partition = (PartitionProxy) o;
            Integer id = partitionId;
            return (id.compareTo(partition.getPartitionId()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionProxy partition = (PartitionProxy) o;
            return partitionId == partition.partitionId;
        }

        @Override
        public int hashCode() {
            return partitionId;
        }

        @Override
        public String toString() {
            return "Partition [" + +partitionId + "], owner=" + getOwner();
        }
    }
}
