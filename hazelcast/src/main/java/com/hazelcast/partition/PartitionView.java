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

/**
 *
 *
 * @author mdogan 6/17/13
 */
public interface PartitionView {

    int MAX_REPLICA_COUNT = 7;
    int MAX_BACKUP_COUNT = MAX_REPLICA_COUNT - 1;

    /**
     * Returns the partition id. The partition id will be between 0 and partitionCount (exclusive).
     *
     * @return the id of the partition.
     */
    int getPartitionId();

    /**
     * Returns the Address of the owner of this partition.
     *
     * If no owner has been set yet, null is returned.
     *
     * The value could be stale when returned.
     *
     * @return the owner.
     */
    Address getOwner();

    /**
     * Checks if there currently is a migration going on in this partition.
     *
     * The returned value could be stale when it is returned.
     *
     * @return true if there is a migration going on, false otherwise.
     */
    boolean isMigrating();

    /**
     * Returns Address of the replica.
     *
     * The owner has replica index 0.
     *
     * The returned value could be null if the owner/replica has not yet been set.
     *
     * todo: what to do when index is out of bounds.
     *
     * The returned value could be stale when it is returned.
     *
     * @param replicaIndex the index of the replica.
     * @return the Address of the replica.
     */
    Address getReplicaAddress(int replicaIndex);

    boolean isOwnerOrBackup(Address address);
}
