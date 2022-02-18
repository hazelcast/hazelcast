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

package com.hazelcast.partition;

import com.hazelcast.cluster.Member;

/**
 * In Hazelcast the data is split up in partitions: by default, 271 and configurable through the 'hazelcast.partition.count'
 * ClusterProperty. Each partition is owned by one member and the ownership can change if members join or leave the cluster.
 *
 * Using this Partition object, you get access to who is the owner of a given partition. This object is not a DTO, so it will
 * be updated when a member changes ownership.
 */
public interface Partition {

    /**
     * Returns the ID of the partition. This value will never change and will always be greater to  or equal to 0 and smaller
     * than the partition-count.
     *
     * @return the ID of the partition
     */
    int getPartitionId();

    /**
     * Returns the current member that owns this partition.
     *
     * The returned value could be stale as soon as it is returned.
     *
     * It can be that null is returned if the owner of a partition has not been established.
     *
     * @return the owner member of the partition
     */
    Member getOwner();
}
