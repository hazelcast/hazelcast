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

package com.hazelcast.partition.impl;

/*
    This enum represents internal state of the com.hazelcast.partition.impl.InternalPartitionServiceImpl over all partitions
    @see com.hazelcast.partition.impl.InternalPartitionServiceImpl#getMemberState
 */
public enum InternalPartitionServiceState {

    /**
     *  Indicates that there is no migration operation on the system and all first backups are sync for partitions
     *  owned by this node
     */
    SAFE,

    /**
     *  Indicates that there is a migration operation ongoing on this node
     */
    MIGRATION_LOCAL,

    /**
     *  Indicates that master node manages a migration operation between 2 other nodes
     */
    MIGRATION_ON_MASTER,

    /**
     *  Indicates that there is an not-sync first replica on other nodes for owned partitions of the this node
     */
    REPLICA_NOT_SYNC

}
