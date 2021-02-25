/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;

/**
 * The event is fired when a primary replica of the partition lost.
 * If a backup node crashes when owner of the partition is still alive,
 * a partition lost event won't be fired.
 *
 * @see Partition
 * @see PartitionService
 * @see PartitionLostListener
 */
public interface PartitionLostEvent extends PartitionEvent {

    /**
     * Return the replica index up to which partition replicas are lost
     * together with the primary replica.
     * <ul>
     * <li> 0 means that only the primary replica is lost. In other
     * words, the node which owns the partition is unreachable, hence
     * removed from the cluster. If there is a data structure
     * configured with no backups, its data is lost for this partition.
     * <li> 1 means that both the primary replica and the first backup
     * replica are lost. In other words, the partition owner node and
     * the first backup node have became unreachable. If a data
     * structure is configured with less than 2 backups, its data is
     * lost for this partition.
     * <li>The idea works same for higher backup counts.
     * </ul>
     */
    int getLostBackupCount();

    /**
     * Returns true if all replicas of a partition are lost
     */
    boolean allReplicasInPartitionLost();

    /**
     * Returns the address of the node that dispatches the event
     *
     * @return the address of the node that dispatches the event
     */
    Address getEventSource();
}
