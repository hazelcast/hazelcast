/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.partitiongroup.MemberGroup;

import java.util.Collection;

public interface PartitionStateGenerator {

    /**
     * Arranges the partition layout.
     * <p>
     * This method does not actually change the partitions, but send back the updated layout.
     * <p>
     * A two-dimensional array of addresses is returned where the first index is the partition ID, and
     * the second index is the replica index.
     *
     * @param groups       member groups
     * @param currentState current partition state.
     * @return proposed partition table
     */
    PartitionReplica[][] arrange(Collection<MemberGroup> groups, InternalPartition[] currentState);

    /**
     * Arranges the partition layout.
     * <p>
     * This method does not actually change the partitions, but send back the updated layout.
     * <p>
     * A two-dimensional array of addresses is returned where the first index is the partition ID, and
     * the second index is the replica index.
     * <p>
     * When null partitions is given, all partitions will be arranged,
     * similar to {@link #arrange(Collection, InternalPartition[])}.
     *
     * @param groups       member groups
     * @param currentState current partition state.
     * @param partitions Partitions to be arranged only.
     * @return proposed partition table
     */
    PartitionReplica[][] arrange(Collection<MemberGroup> groups, InternalPartition[] currentState,
            Collection<Integer> partitions);
}
