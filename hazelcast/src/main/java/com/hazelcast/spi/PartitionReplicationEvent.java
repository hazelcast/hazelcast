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

package com.hazelcast.spi;

import java.util.EventObject;

/**
 * An event send to {@link com.hazelcast.spi.MigrationAwareService} when partitions changes happen.
 */
public class PartitionReplicationEvent extends EventObject {

    private final int partitionId;

    private final int replicaIndex;

    /**
     * Creates a PartitionReplicationEvent
     *
     * @param partitionId the partition id.
     * @param replicaIndex the replica index.
     */
    public PartitionReplicationEvent(int partitionId, int replicaIndex) {
        super(partitionId);
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
    }

    /**
     * The id of the partition.
     *
     * @return the id of the partition
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * The replica index. 0 is priimary, the rest is backup.
     *
     * @return the replica index
     */
    public int getReplicaIndex() {
        return replicaIndex;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PartitionReplicationEvent{");
        sb.append("partitionId=").append(partitionId);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append('}');
        return sb.toString();
    }
}
