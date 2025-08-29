/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.instance.impl.ClusterTopologyIntent;
import com.hazelcast.instance.impl.ClusterTopologyIntentTracker;
import com.hazelcast.internal.cluster.impl.ClusterJoinManager;
import com.hazelcast.internal.cluster.impl.operations.OnJoinOp;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.operation.SafeStateCheckOperation;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Internal service for interacting with hot restart related functionalities (e.g. force and partial start)
 */
public interface InternalHotRestartService {

    /**
     * Name of the Hot Restart service.
     */
    String SERVICE_NAME = "hz:ee:internalHotRestartService";

    /**
     * Key of internal member attribute for persistence.
     * When the attribute is present and its value is {@code true},
     * persistence is enabled.
     */
    String PERSISTENCE_ENABLED_ATTRIBUTE = "hazelcast.persistence.enabled";

    /**
     * Returns whether hot-restart is enabled or not.
     */
    boolean isEnabled();

    /**
     * If recovery is in progress and no other deferred cluster state was already set
     * (with a previous call to {@code trySetDeferredClusterState}),
     * stores the given {@code newClusterState} and applies it after recovery is complete,
     * returning {@code true}. Otherwise, does nothing and returns {@code false}.
     *
     * @return {@code true} if recovery is in progress, indicating that the new cluster
     * state will be applied once recovery is complete, otherwise {@code false}.
     */
    boolean trySetDeferredClusterState(ClusterState newClusterState);

    /**
     * @return {@code true} when recovery process is completed and all members reached final state, otherwise {@code false}.
     */
    boolean isStartCompleted();

    /**
     * Forces node to start by skipping hot-restart completely and removing all hot-restart data
     * even if node is still on validation phase or loading hot-restart data.
     *
     * @return true if force start is triggered successfully. force start cannot be triggered if hot restart is disabled or
     * the master is not known yet
     */
    boolean triggerForceStart();

    /**
     * Triggers partial start if the cluster cannot be started with full recovery and
     * {@link HotRestartPersistenceConfig#clusterDataRecoveryPolicy} is set accordingly.
     *
     * @return true if partial start is triggered.
     */
    boolean triggerPartialStart();

    /**
     * Checks if the given member has been excluded during the cluster start or not.
     * If returns true, it means that the given member is not allowed to join to the cluster.
     *
     * @param memberAddress address of the member to check
     * @param memberUuid    UUID of the member to check
     * @return true if the member has been excluded on cluster start.
     */
    boolean isMemberExcluded(Address memberAddress, UUID memberUuid);

    /**
     * Returns UUIDs of the members that have been excluded during the cluster start.
     *
     * @return UUIDs of the members that have been excluded during the cluster start
     */
    Set<UUID> getExcludedMemberUuids();

    /**
     * Notifies the excluded member and triggers the member force start process.
     *
     * @param memberAddress address of the member to notify
     */
    void notifyExcludedMember(Address memberAddress);

    /**
     * Handles the UUID set of excluded members only if this member is also excluded, and triggers the member force start process.
     *
     * @param sender              the member that has sent the excluded members set
     * @param excludedMemberUuids UUIDs of the members that have been excluded during the cluster start
     */
    void handleExcludedMemberUuids(Address sender, Set<UUID> excludedMemberUuids);

    /**
     * Returns latest Hot Restart status as Management Center DTO. An empty status object will
     * be returned if Hot Restart is not available (not EE) or not enabled.
     */
    ClusterHotRestartStatusDTO getCurrentClusterHotRestartStatus();

    /**
     * Resets local service data, removes hot restart directories and recreates them.
     *
     * @param isAfterJoin true if this is called after node joins the cluster, false otherwise
     */
    void resetService(boolean isAfterJoin);

    /**
     * Executes force-start process, resets local hot restart data and member gets a new UUID.
     * <p>
     * If the local node has already completed the start process, or it isn't excluded in cluster start,
     * this method fails with an exception.
     */
    void forceStartBeforeJoin();

    /**
     * Waits until partition replicas (primaries and backups) get in sync.
     *
     * @param unit time unit
     * @param supplier the {@link SafeStateCheckOperation} supplier
     * @throws IllegalStateException when timeout happens or a member leaves the cluster while waiting
     */
    void waitPartitionReplicaSyncOnCluster(long timeout, TimeUnit unit, Supplier<SafeStateCheckOperation> supplier);

    void setRejoiningActiveCluster(boolean rejoiningActiveCluster);

    /**
     * Apply given {@link PartitionRuntimeState} after recovery is successfully completed.
     *
     * @see InternalPartitionService#createPartitionState()
     * @see ClusterJoinManager#startJoin
     */
    void deferApplyPartitionState(@Nullable PartitionRuntimeState partitionRuntimeState);

    /**
     * Apply given {@link OnJoinOp} after recovery is successfully completed.
     */
    void deferPostJoinOps(OnJoinOp postJoinOp);

    /**
     * @return {@code true} if cluster metadata were found on disk
     *         so recovery will proceed from disk, otherwise {@code false}.
     */
    boolean isClusterMetadataFoundOnDisk();

    /**
     * Invoked each time an enabled {@link ClusterTopologyIntentTracker} detects a change of cluster
     * topology in the runtime environment. Only used when running in a managed context (Kubernetes).
     */
    void onClusterTopologyIntentChange();

    /**
     * Conveys information about the detected cluster topology intent on master member.
     * @param clusterTopologyIntent the cluster topology intent, as detected by master member.
     */
    void setClusterTopologyIntentOnMaster(ClusterTopologyIntent clusterTopologyIntent);

    /**
     * Callback method when a member has left the cluster, and what the cluster's state is
     * as the member leaves. This is used to track crashed members and mark them as unsafe
     * if they crash (leave the cluster in an ACTIVE-like state) while 1 or more other nodes
     * are marked as crashed.
     *
     * @param member       the member which is discovered to have crashed.
     * @param clusterState the cluster's state when the member left
     */
    void onMemberLeft(Member member, ClusterState clusterState);

    /**
     * Callback method when a member joins the cluster. This is used to remove crashed
     * members from tracking on non-master nodes, as only the master node responds to
     * Hot Restart requests, but other nodes need to keep their tracker up-to-date as
     * they may become master in the future.
     *
     * @param member the member which has joined the cluster.
     */
    void onMemberJoined(Member member);
}
