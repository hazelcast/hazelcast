/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.CoreService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface InternalPartitionService extends CoreService {

    String SERVICE_NAME = "hz:core:partitionService";

    long DEFAULT_REPLICA_SYNC_DELAY = 5000L;
    long REPLICA_SYNC_RETRY_DELAY = 500L;

    /**
     * Gets the owner of the partition if it's set.
     * Otherwise it will trigger partition assignment.
     *
     * @param partitionId the partitionId
     * @return owner of partition or null if it's not set yet.
     */
    Address getPartitionOwner(int partitionId);

    /**
     * Gets the owner of the partition. If none is set, it will wait till the owner is set.
     *
     * @param partitionId  the partitionId
     * @return owner of partition
     * @throws InterruptedException
     */
    Address getPartitionOwnerOrWait(int partitionId) throws InterruptedException;

    /**
     * Returns the InternalPartition for a given partitionId.
     * If owner of the partition is not set yet, it will trigger partition assignment.
     * <p/>
     * The InternalPartition for a given partitionId wil never change; so it can be cached safely.
     *
     * @param partitionId the partitionId
     * @return the InternalPartition.
     */
    InternalPartition getPartition(int partitionId);

    /**
     * Returns the InternalPartition for a given partitionId.
     * If owner of the partition is not set yet and {@code triggerOwnerAssignment} is true,
     * it will trigger partition assignment.
     * <p/>
     * The InternalPartition for a given partitionId wil never change; so it can be cached safely.
     *
     * @param partitionId the partitionId
     * @param triggerOwnerAssignment flag to trigger partition assignment
     * @return the InternalPartition.
     */
    InternalPartition getPartition(int partitionId, boolean triggerOwnerAssignment);

    /**
     * Returns the partition id for a Data key.
     *
     * @param key the Data key.
     * @return the partition id.
     * @throws java.lang.NullPointerException if key is null.
     */
    int getPartitionId(Data key);

    /**
     * Returns the partition id for a given object.
     *
     * @param key the object key.
     * @return the partition id.
     */
    int getPartitionId(Object key);

    /**
     * Returns the number of partitions.
     *
     * @return the number of partitions.
     */
    int getPartitionCount();

    /**
     * Checks if there currently are any migrations.
     *
     * @return true if there are migrations, false otherwise.
     */
    boolean hasOnGoingMigration();

    List<Integer> getMemberPartitions(Address target);

    /**
     * Gets member partition IDs. Blocks until partitions are assigned.
     * @return map of member address to partition Ids
     **/
    Map<Address, List<Integer>> getMemberPartitionsMap();

    int getMemberGroupsSize();

    int getMaxBackupCount();

    String addMigrationListener(MigrationListener migrationListener);

    boolean removeMigrationListener(String registrationId);

    Member getMember(Address address);

    long getMigrationQueueSize();

    void pauseMigration();

    void resumeMigration();

    void memberAdded(MemberImpl newMember);

    void memberRemoved(MemberImpl deadMember);

    boolean prepareToSafeShutdown(long timeout, TimeUnit seconds);

    /**
     * Query and return if this member in a safe state or not.
     * This method just checks for a safe state, it doesn't force this member to be in a safe state.
     *
     * @return <code>true</code> if this member in a safe state, otherwise <code>false</code>
     */
    boolean isMemberStateSafe();

    InternalPartition[] getPartitions();

    Collection<MigrationInfo> getActiveMigrations();

    void firstArrangement();

    long[] getPartitionReplicaVersions(int partitionId);

    void updatePartitionReplicaVersions(int partitionId, long[] replicaVersions, int replicaIndex);

    long[] incrementPartitionReplicaVersions(int partitionId, int totalBackupCount);

    void setPartitionReplicaVersions(int partitionId, long[] versions, int replicaOffset);

    void clearPartitionReplicaVersions(int partitionId);

    com.hazelcast.core.PartitionService getPartitionServiceProxy();

}
