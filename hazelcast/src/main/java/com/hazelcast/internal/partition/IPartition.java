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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.partition.Partition;

/**
 * Represents a Partition. It is comparable to the {@link Partition} but it is optimized for SPI
 * usage, so it exposes method not meant for regular Hazelcast users.
 * <p>
 * The IPartition provides access to information about a partition, most importantly the addresses of the
 * replica's and this information will be updated. So one can cache the IPartition and keep asking for
 * partition information.
 */
public interface IPartition {

    /**
     * The maximum number of backups.
     */
    int MAX_BACKUP_COUNT = 6;

    /**
     * Checks if the partition is local.
     * <p>
     * A partition is local if and only if the {@link #getOwnerOrNull()} returns the same address as 'this' address of the
     * {@link ClusterService#getThisAddress()}. If the address is {@code null} or a different address, {@code false}
     * is returned.
     *
     * @return {@code true} if local, {@code false} otherwise
     * @since 3.5
     */
    boolean isLocal();

    /**
     * Returns the partition ID.
     * <p>
     * The partition ID will be between 0 and partitionCount (exclusive).
     *
     * @return the ID of the partition
     */
    int getPartitionId();

    /**
     * Returns the address of the owner of this partition.
     * <p>
     * If no owner has been set yet, null is returned. So be careful with assuming that a non {@code null} value is returned.
     * <p>
     * The value could be stale when returned.
     *
     * @return the owner
     */
    Address getOwnerOrNull();

    /**
     * Checks if there currently is a migration going on in this partition.
     * <p>
     * The returned value could be stale when it is returned.
     *
     * @return {@code true} if there is a migration going on, {@code false} otherwise
     */
    boolean isMigrating();

    /**
     * Returns the address of the replica.
     * <p>
     * The owner has replica index 0.
     * <p>
     * The returned value could be {@code null} if the owner/replica has not yet been set.
     * <p>
     * The returned value could be stale when it is returned.
     *
     * @param replicaIndex the index of the replica
     * @return the address of the replica
     * @throws ArrayIndexOutOfBoundsException when replica index is out of bounds
     */
    Address getReplicaAddress(int replicaIndex);

    /**
     * Checks if given address is owner of primary or backup of this partition.
     *
     * @param address owner address
     * @return {@code true} if address is owner or backup, {@code false} otherwise
     */
    boolean isOwnerOrBackup(Address address);

    /**
     * Returns the version of the partition.
     * Partition version is incremented by one, on each replica
     * assignment change on the partition.
     *
     * @return version of the partition
     */
    int version();
}
