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

import java.util.EventListener;

/**
 * PartitionLostListener provides the ability to be notified upon a
 * possible data loss when a partition loses a replica.
 * <p>
 * When the cluster is initialized, each node becomes owner of some of the
 * partitions and of a backup of some other partitions. We call the replica on the
 * partition owner a "primary replica" and on the backup nodes "backup replicas".
 * Partition replicas are ordered. A primary replica node keeps all data
 * that is mapped to the partition. If a Hazelcast data structure is
 * configured with 1 backup, its data is put into the primary replica and
 * the first backup replica. Similarly, data of a Hazelcast data structure
 * that is configured with 2 backups is put into the primary replica, the
 * first backup replica and the second backup replica and so on.
 * <p>
 * When a node fails, primary replicas of its partitions are lost. In this
 * case, ownership of each partition owned by the unreachable node is
 * transferred to the first available backup node. After this point, other
 * backup nodes sync themselves from the new partition owner node in order
 * to populate the missing backup data. This sync only happens when backup
 * partition replica versions are not equal to the primary ones.
 * <p>
 * In this context the partition loss detection algorithm works as follows:
 * {@link PartitionLostEvent#getLostBackupCount()} denotes the replica
 * index up to which partition replicas are lost:<ul>
 * <li>0 means that only the primary replica is lost. In other words, the node
 * which owns the partition is unreachable, hence removed from the cluster.
 * If there is a data structure configured with no backups, its data is
 * lost for this partition.
 * <li>1 means that both the primary replica and the first backup replica are
 * lost. In other words, the partition owner node and the first backup node
 * have became unreachable. If a data structure is configured with less
 * than 2 backups, its data is lost for this partition.
 * <li>The idea works same for higher backup counts.
 * </ul>
 *
 * Please note that node failures that do not involve a primary replica
 * do not lead to partition lost events. For instance, if a backup node
 * crashes when owner of the partition is still alive, a partition lost
 * event is not fired. In this case, Hazelcast tries to assign a new backup
 * replica to populate the missing backup.
 *
 * @see Partition
 * @see PartitionService
 * @since 3.5
 */
@FunctionalInterface
public interface PartitionLostListener extends EventListener {

    /**
     * Invoked when a primary replica of the partition is lost. Node
     * failures that do not involve a primary replica do not lead to
     * partition lost events.
     *
     * @param event the event that contains the partition ID and the
     *             backup count that has been lost
     */
    void partitionLost(PartitionLostEvent event);
}
