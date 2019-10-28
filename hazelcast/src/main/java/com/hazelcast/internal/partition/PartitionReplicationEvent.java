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

/**
 * An event send to {@link MigrationAwareService} when partition changes happen.
 */
public class PartitionReplicationEvent {

    private final int partitionId;

    private final int replicaIndex;

    /**
     * Creates a PartitionReplicationEvent
     *
     * @param partitionId  the partition ID
     * @param replicaIndex the replica index
     */
    public PartitionReplicationEvent(int partitionId, int replicaIndex) {
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
    }

    /**
     * Gets the ID of the partition.
     *
     * @return the ID of the partition
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Gets the replica index. 0 is primary, the rest is backup.
     *
     * @return the replica index
     */
    public int getReplicaIndex() {
        return replicaIndex;
    }

    @Override
    public String toString() {
        return "PartitionReplicationEvent{partitionId=" + partitionId + ", replicaIndex=" + replicaIndex + '}';
    }
}
