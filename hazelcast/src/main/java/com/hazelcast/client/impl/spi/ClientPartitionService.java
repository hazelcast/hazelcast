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

package com.hazelcast.client.impl.spi;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.partition.Partition;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * Partition service for Hazelcast clients.
 *
 * Allows to retrieve information about the partition count, the partition owner or the partitionId of a key.
 */
public interface ClientPartitionService {

    /**
     * @param partitionId
     * @return the owner of the partition or null if a partition is not assigned yet
     */
    UUID getPartitionOwner(int partitionId);

    /**
     * @return the partition id associated with given data
     * @throws com.hazelcast.spi.exception.RetryableHazelcastException if partition table is not arrived yet
     */
    int getPartitionId(@Nonnull Data key);

    /**
     * @return the partition id associated with given Object
     * @throws com.hazelcast.spi.exception.RetryableHazelcastException if partition table is not arrived yet
     */
    int getPartitionId(@Nonnull Object key);

    /**
     * If partition table is not fetched yet, this method returns zero
     *
     * @return the partition count
     */
    int getPartitionCount();

    Partition getPartition(int partitionId);

}
