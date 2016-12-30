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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.MemoizingSupplier.memoize;
import static java.util.stream.Collectors.toMap;

/**
 * Collaborator of {@link ExecutionPlan} that takes care of assigning
 * partition IDs to processors.
 */
class PartitionArrangement {
    /**
     * Mapping from each remote member address to the partition IDs it owns.
     */
    final Supplier<Map<Address, int[]>> remotePartitionAssignment;
    private final Supplier<int[]> localPartitions;
    private final Supplier<int[]> allPartitions;

    PartitionArrangement(NodeEngine nodeEngine) {
        localPartitions = memoize(() -> arrangeLocalPartitions(nodeEngine));
        allPartitions = memoize(() -> arrangeAllPartitions(nodeEngine, localPartitions.get()));
        remotePartitionAssignment = memoize(() -> remotePartitionAssignment(nodeEngine));
    }

    /**
     * Determines for each processor instance the partition IDs it will be in charge of
     * (processors are identified by their index). The method is called separately for
     * each edge. For a distributed edge, only partitions owned by the local member need
     * to be assigned; for a non-distributed edge, every partition ID must be assigned.
     * Local partitions will get the same assignments in both cases, and repeating the
     * invocation with the same arguments will always yield the same result.
     *
     * @param processorCount    number of processor instances
     * @param isEdgeDistributed whether the edge is distributed
     * @return a 2D-array where the major index is the index of a processor and
     * the {@code int[]} at that index is the array of partition IDs assigned to
     * the processor
     */
    int[][] assignPartitionsToProcessors(int processorCount, boolean isEdgeDistributed) {
        final int[] arrangedPtions = (isEdgeDistributed ? localPartitions : allPartitions).get();
        final int[][] ptionsPerProcessor = createPtionArrays(arrangedPtions.length, processorCount);
        int majorIndex = 0;
        int minorIndex = 0;
        for (int ptionId : arrangedPtions) {
            ptionsPerProcessor[majorIndex][minorIndex] = ptionId;
            if (++majorIndex == processorCount) {
                majorIndex = 0;
                minorIndex++;
            }
        }
        return ptionsPerProcessor;
    }

    private static int[] arrangeLocalPartitions(NodeEngine nodeEngine) {
        return nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress())
                         .stream().mapToInt(x -> x).sorted().toArray();
    }

    private static int[] arrangeAllPartitions(NodeEngine nodeEngine, int[] localPartitions) {
        final int totalPartitionCount = nodeEngine.getPartitionService().getPartitionCount();
        final int[] allPartitions = Arrays.copyOf(localPartitions, totalPartitionCount);
        int i = localPartitions.length;
        for (int ption = 0; ption < totalPartitionCount; ption++) {
            if (Arrays.binarySearch(localPartitions, ption) < 0) {
                allPartitions[i++] = ption;
            }
        }
        return allPartitions;
    }

    private static int[][] createPtionArrays(int ptionCount, int processorCount) {
        final int[][] ptionsPerProcessor = new int[processorCount][];
        final int quot = ptionCount / processorCount;
        final int rem = ptionCount % processorCount;
        Arrays.setAll(ptionsPerProcessor, i -> new int[quot + (i < rem ? 1 : 0)]);
        return ptionsPerProcessor;
    }

    private static Map<Address, int[]> remotePartitionAssignment(NodeEngine nodeEngine) {
        final Address localAddress = nodeEngine.getThisAddress();
        final Map<Address, List<Integer>> partitionOwnerMap =
                nodeEngine.getPartitionService().getMemberPartitionsMap();
        final Map<Address, List<Integer>> addrToPartitions = partitionOwnerMap
                .entrySet().stream()
                .filter(e -> !e.getKey().equals(localAddress))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        return addrToPartitions.entrySet().stream().collect(toMap(
                Map.Entry::getKey, e -> e.getValue().stream().mapToInt(x -> x).toArray()));
    }
}
