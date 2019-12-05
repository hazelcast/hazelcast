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

package com.hazelcast.partition;

import com.hazelcast.cluster.Address;

/**
 * The event that is fired when a partition lost its owner and all backups.
 *
 * @see Partition
 * @see PartitionService
 * @see PartitionLostListener
 */
public interface PartitionLostEvent extends PartitionEvent {

    /**
     * Returns the number of lost backups for the partition. 0: the owner, 1: first backup, 2: second backup...
     */
    int getLostBackupCount();

    /**
     * Returns the address of the node that dispatches the event
     *
     * @return the address of the node that dispatches the event
     */
    Address getEventSource();
}
