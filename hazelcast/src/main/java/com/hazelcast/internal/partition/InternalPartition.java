/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.util.stream.IntStream;

public interface InternalPartition extends IPartition {

    int MAX_REPLICA_COUNT = MAX_BACKUP_COUNT + 1;

    /**
     * Returns the partition replica of the owner of this partition.
     * <p>
     * If no owner has been set yet, null is returned. So be careful with assuming that a non {@code null} value is returned.
     * <p>
     * The value could be stale when returned.
     *
     * @return the owner
     */
    PartitionReplica getOwnerReplicaOrNull();

    /**
     * Return the index of partition replica for this partition.
     *
     * @param replica partition replica
     * @return the replica index or -1 if the replica is null or the replica is not in the replica list
     */
    int getReplicaIndex(PartitionReplica replica);

    /**
     * Returns the partition replica assigned to the replica index.
     * <p>
     * The owner has replica index 0.
     * <p>
     * The returned value could be {@code null} if the owner/replica has not yet been set.
     * <p>
     * The returned value could be stale when it is returned.
     *
     * @param replicaIndex the index of the replica
     * @return the partition replica
     * @throws ArrayIndexOutOfBoundsException when replica index is out of bounds
     */
    PartitionReplica getReplica(int replicaIndex);

    /**
     * Returns the integer replica indices of {@code InternalPartition} as a stream.
     */
    static IntStream replicaIndices() {
        return IntStream.range(0, InternalPartition.MAX_REPLICA_COUNT);
    }
}
