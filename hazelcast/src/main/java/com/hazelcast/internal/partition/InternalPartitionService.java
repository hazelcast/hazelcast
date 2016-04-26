/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.concurrent.TimeUnit;

public interface InternalPartitionService extends IPartitionService {

    /**
     * Retry count for migration operations.
     */
    int MIGRATION_RETRY_COUNT = 6;

    /**
     * Retry pause for migration operations.
     */
    long MIGRATION_RETRY_PAUSE = 10000;

    /**
     * Delay for anti-entropy replica synchronization.
     */
    long DEFAULT_REPLICA_SYNC_DELAY = 5000L;

    /**
     * Retry delay for replica synchronization.
     */
    long REPLICA_SYNC_RETRY_DELAY = 500L;

    /**
     * Static constant for dispatching and listening migration events
     */
    String MIGRATION_EVENT_TOPIC = ".migration";

    /**
     * Static constant for dispatching and listening internal partition lost events
     */
    String PARTITION_LOST_EVENT_TOPIC = ".partitionLost";

    InternalPartition getPartition(int partitionId);

    InternalPartition getPartition(int partitionId, boolean triggerOwnerAssignment);

    int getMemberGroupsSize();

    void pauseMigration();

    void resumeMigration();

    void memberAdded(MemberImpl newMember);

    void memberRemoved(MemberImpl deadMember);

    boolean prepareToSafeShutdown(long timeout, TimeUnit seconds);

    InternalPartition[] getInternalPartitions();

    void firstArrangement();

    PartitionRuntimeState createPartitionState();

    boolean isPartitionReplicaVersionStale(int partitionId, long[] versions, int replicaIndex);

    long[] getPartitionReplicaVersions(int partitionId);

    void updatePartitionReplicaVersions(int partitionId, long[] replicaVersions, int replicaIndex);

    long[] incrementPartitionReplicaVersions(int partitionId, int totalBackupCount);
}
