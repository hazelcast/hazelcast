/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;

import java.util.EventListener;

/**
 * PartitionLostListener provides the ability to be notified upon a possible data loss when a partition has no owner and backups.
 * IMPORTANT: Please note that it may dispatch false-positive partition lost events when partial network split errors occur.
 *
 * Partition loss detection algorithm works as follows:
 * When the cluster is initialized, each node becomes owner of some of the partitions, and backup of the some other partitions.
 * On a node failure, the first backup node becomes owner for a partition and all other backup nodes are shifted up in the
 * partition backup order. Therefore, some nodes sync themselves from the new partition owner node since they are shifted up in
 * the partition table and do not contain the backup data yet. Whey they issue a partition sync request, they set themselves in
 * a custom state that indicates that a node is waiting for a partition sync for a particular backup. When the sync operation is
 * completed, that sync flag is reset.
 * Lets say the owner of the partition crashes before it sends backup data to sync-waiting partitions.
 * The first backup which will become the owner of the partition checks its replica-sync flags. If they are set, it detects that
 * the owner failed before the backup node completes the sync process and issues a partition lost event.
 *
 * @see Partition
 * @see PartitionService
 *
 * @since 3.5
 */
public interface PartitionLostListener extends EventListener {

    /**
     * Invoked when a partition loses its owner and all backups.
     *
     * @param event the event that contains the partition id and the backup count that has been lost
     */
    void partitionLost(PartitionLostEvent event);

}
