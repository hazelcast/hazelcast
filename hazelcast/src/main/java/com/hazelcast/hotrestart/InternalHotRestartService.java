/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hotrestart;

import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.nio.Address;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Internal service for interacting with hot restart related functionalities (e.g. force and partial start)
 */
public interface InternalHotRestartService {
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
    boolean isMemberExcluded(Address memberAddress, String memberUuid);

    /**
     * Returns UUIDs of the members that have been excluded during the cluster start.
     *
     * @return UUIDs of the members that have been excluded during the cluster start
     */
    Set<String> getExcludedMemberUuids();

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
    void handleExcludedMemberUuids(Address sender, Set<String> excludedMemberUuids);

    /**
     * Returns latest Hot Restart status as Management Center DTO. An empty status object will
     * be returned if Hot Restart is not available (not EE) or not enabled.
     */
    ClusterHotRestartStatusDTO getCurrentClusterHotRestartStatus();

    /**
     * Resets local hot restart data and gets a new UUID, if the local node hasn't completed the start process and
     * it is excluded in cluster start.
     */
    void resetHotRestartData();

    /**
     * Waits until partition replicas (primaries and backups) get in sync.
     *
     * @param timeout timeout
     * @param unit time unit
     * @throws IllegalStateException when timeout happens or a member leaves the cluster while waiting
     */
    void waitPartitionReplicaSyncOnCluster(long timeout, TimeUnit unit);
}
