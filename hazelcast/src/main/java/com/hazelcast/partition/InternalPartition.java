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
 * Represents a Partition. It is comparable to the {@link com.hazelcast.core.Partition} but it is optimized for internal
 * usage, so it exposes method not meant for regular Hazelcast users.
 * <p/>
 * The InternalPartition provides access to information about a partition, most importantly the addresses of the
 * replica's and this information will be updated. So one can cache the InternalPartition and keep asking for
 * partition information.
 *
 */
public interface InternalPartition {

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
     * <p/>
     * If no owner has been set yet, null is returned. So be careful with assuming that a non null value is returned.
     * <p/>
     * The value could be stale when returned.
     *
     * @return the owner.
     */
    Address getOwnerOrNull();

    /**
     * Checks if there currently is a migration going on in this partition.
     * <p/>
     * The returned value could be stale when it is returned.
     *
     * @return true if there is a migration going on, false otherwise.
     */
    boolean isMigrating();

    /**
     * Returns Address of the replica.
     * <p/>
     * The owner has replica index 0.
     * <p/>
     * The returned value could be null if the owner/replica has not yet been set.
     * <p/>
     * The returned value could be stale when it is returned.
     *
     * @param replicaIndex the index of the replica.
     * @throws java.lang.ArrayIndexOutOfBoundsException when replica index is out of bounds
     * @return the Address of the replica.
     */
    Address getReplicaAddress(int replicaIndex);

    /**
     * Checks if given address is owner of primary or backup of this partition.
     *
     * @param address owner address
     * @return true if address is owner or backup, false otherwise
     */
    boolean isOwnerOrBackup(Address address);
}
