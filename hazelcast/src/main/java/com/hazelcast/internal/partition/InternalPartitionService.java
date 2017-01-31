/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Address;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface InternalPartitionService extends IPartitionService {

    /**
     * Retry count for migration operations.
     * <p>
     * Current Invocation mechanism retries first 5 invocations without pausing.
     */
    int MIGRATION_RETRY_COUNT = 12;

    /**
     * Retry pause for migration operations in milliseconds.
     */
    long MIGRATION_RETRY_PAUSE = 10000;

    /**
     * Delay for anti-entropy replica synchronization in milliseconds.
     */
    long DEFAULT_REPLICA_SYNC_DELAY = 5000L;

    /**
     * Retry delay for replica synchronization in milliseconds.
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

    /** Pause all migrations */
    void pauseMigration();

    /** Resume all migrations */
    void resumeMigration();

    boolean isMemberAllowedToJoin(Address address);

    void memberAdded(MemberImpl newMember);

    void memberRemoved(MemberImpl deadMember);

    boolean prepareToSafeShutdown(long timeout, TimeUnit seconds);

    InternalPartition[] getInternalPartitions();

    void firstArrangement();

    PartitionRuntimeState createPartitionState();

    boolean isPartitionReplicaVersionStale(int partitionId, long[] versions, int replicaIndex);

    long[] getPartitionReplicaVersions(int partitionId);

    /**
     * Updates the partition replica version and triggers replica sync if the replica is dirty (e.g. the
     * received version is not expected and this node might have missed an update)
     * @param partitionId the id of the partition for which we received a new version
     * @param replicaVersions the received replica versions
     * @param replicaIndex the index of this replica
     */
    void updatePartitionReplicaVersions(int partitionId, long[] replicaVersions, int replicaIndex);

    long[] incrementPartitionReplicaVersions(int partitionId, int totalBackupCount);

    PartitionTableView createPartitionTableView();

    /**
     * Returns partition id list assigned to given target if partitions are assigned when method is called.
     * Does not trigger partition assignment otherwise.
     *
     * @return partition id list assigned to given target if partitions are assigned already
     */
    List<Integer> getMemberPartitionsIfAssigned(Address target);
}
