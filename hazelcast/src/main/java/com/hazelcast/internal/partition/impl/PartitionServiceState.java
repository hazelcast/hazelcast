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

package com.hazelcast.internal.partition.impl;

/**
 * Represents partition service state of current member.
 */
public enum PartitionServiceState {

    /**
     * Indicates that there are no ongoing migrations on the system
     * and all backups are sync for partitions owned by this node.
     */
    SAFE,

    /**
     * Indicates that there is an ongoing  migration on this node.
     */
    MIGRATION_LOCAL,

    /**
     * Indicates that there are migrations ongoing and scheduled by master node.
     */
    MIGRATION_ON_MASTER,

    /**
     * Indicates that there are out-of-sync replicas for owned partitions by this node.
     */
    REPLICA_NOT_SYNC,

    /**
     * Indicates that there are some unowned replicas.
     */
    REPLICA_NOT_OWNED,

    /**
     * Indicates that new master still fetching partition tables from cluster members
     * to determine the most recent partition table published by the previous master.
     */
    FETCHING_PARTITION_TABLE

}
