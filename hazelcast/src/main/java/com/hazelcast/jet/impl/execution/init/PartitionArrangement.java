/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.MutableInteger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Collaborator of {@link ExecutionPlan} that takes care of assigning
 * partition IDs to processors.
 */
class PartitionArrangement {
    /**
     * Mapping from each remote member address to the partition IDs it owns.
     * Members without partitions are missing.
     */
    private final Map<Address, int[]> remotePartitionAssignment;

    /** An array of [0, 1, 2, ... partitionCount-1] */
    private final int[] allPartitions;

    /** Array of local partitions */
    private final int[] localPartitions;

    PartitionArrangement(Address[] partitionOwners, Address thisAddress) {
        Map<Address, MutableInteger> memberCounts = new HashMap<>();
        for (Address partitionOwner : partitionOwners) {
            memberCounts.computeIfAbsent(partitionOwner, x -> new MutableInteger()).value++;
        }
        localPartitions = new int[memberCounts.get(thisAddress).value];
        remotePartitionAssignment = new HashMap<>();
        for (Entry<Address, MutableInteger> en : memberCounts.entrySet()) {
            if (en.getKey() != thisAddress) {
                MutableInteger count = en.getValue();
                remotePartitionAssignment.put(en.getKey(), new int[count.value]);
                count.value = 0;
            }
        }

        int localPartitionIndex = 0;
        for (int i = 0; i < partitionOwners.length; i++) {
            Address address = partitionOwners[i];
            if (address == thisAddress) {
                localPartitions[localPartitionIndex++] = i;
            } else {
                int index = memberCounts.get(address).value++;
                remotePartitionAssignment.get(address)[index] = i;
            }
        }

        allPartitions = new int[partitionOwners.length];
        for (int i = 0; i < allPartitions.length; i++) {
            allPartitions[i] = i;
        }
    }

    Map<Address, int[]> getRemotePartitionAssignment() {
        return remotePartitionAssignment;
    }

    /**
     * Determines for each processor instance the partition IDs it will be in
     * charge of. The method is called separately for each edge, defined by
     * {@code localParallelism}. For a distributed edge, only partitions owned
     * by the local member will be assigned; for a non-distributed edge, every
     * partition ID will be assigned. Repeating the invocation with the same
     * arguments will always yield the same result.
     *
     * @param localParallelism    number of processor instances
     * @param isEdgeDistributed whether the edge is distributed
     * @return a 2D-array where the major index is the index of a processor and
     * the {@code int[]} at that index is the array of partition IDs assigned to
     * the processor
     */
    int[][] assignPartitionsToProcessors(int localParallelism, boolean isEdgeDistributed) {
        final int[] ptions = isEdgeDistributed ? localPartitions : allPartitions;

        final int[][] ptionsPerProcessor = createPtionArrays(ptions.length, localParallelism);
        for (int i = 0; i < localParallelism; i++) {
            for (int j = 0, ptionIndex = i; ptionIndex < ptions.length; ptionIndex += localParallelism, j++) {
                ptionsPerProcessor[i][j] = ptions[ptionIndex];
            }
        }
        return ptionsPerProcessor;
    }

    private static int[][] createPtionArrays(int ptionCount, int processorCount) {
        final int[][] ptionsPerProcessor = new int[processorCount][];
        final int quot = ptionCount / processorCount;
        final int rem = ptionCount % processorCount;
        Arrays.setAll(ptionsPerProcessor, i -> new int[quot + (i < rem ? 1 : 0)]);
        return ptionsPerProcessor;
    }

    /**
     * Returns an assignment where all partitions are assigned to the target
     * member and no partitions are assigned to other members.
     */
    public Map<Address, int[]> remotePartitionAssignmentToOne(Address target) {
        Map<Address, int[]> res = new HashMap<>();
        for (Address address : remotePartitionAssignment.keySet()) {
            res.put(address, address.equals(target) ? allPartitions : new int[0]);
        }
        return res;
    }
}
