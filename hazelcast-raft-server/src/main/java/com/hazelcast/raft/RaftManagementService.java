/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft;

import com.hazelcast.core.ICompletableFuture;

import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 */
public interface RaftManagementService {

    /**
     * Returns all Raft group ids.
     */
    Collection<RaftGroupId> getRaftGroupIds();

    /**
     * Returns the Raft group associated with the group id.
     */
    RaftGroup getRaftGroup(RaftGroupId groupId);

    /**
     * Wipes & resets Raft state and initializes it as if this node starting up initially. This method should only used
     * when active Raft members lose the majority and Raft cluster cannot progress anymore.
     * <p>
     * After this method is called, all state and data will be wiped and Raft members will start empty.
     * <p>
     * <strong>Use with caution:
     * This method is NOT idempotent and multiple invocations on the same member can break the whole system!</strong>
     */
    void resetAndInitRaftState();

    /**
     * Promotes local Hazelcast cluster member to a Raft cluster member.
     * <p>
     * If member is in active members list already, then this method will have no effect.
     * <p>
     * If member is being removed from active members list, then {@link IllegalArgumentException} will be returned as
     * response.
     *
     * @return a Future representing pending completion of the operation
     * @throws IllegalStateException If local member is already an active Raft member
     */
    ICompletableFuture<Void> triggerRaftMemberPromotion();

    /**
     * Removes the unreachable member from active members list and from all Raft groups it belongs to.
     * Another active member will replace it in those Raft groups, when possible.
     * <p>
     * If member is not in active members list (anymore), then this method will have no effect.
     *
     * @return a Future representing pending completion of the operation
     * @throws IllegalStateException When member removal initiated by a non-master member
     *                               or member is alive/reachable.
     */
    ICompletableFuture<Void> triggerRemoveRaftMember(RaftMember member);

    /**
     * Initiates a rebalancing process for Raft groups has missing members. If there's an ongoing member removal
     * operation, then rebalance call will fail with {@link IllegalStateException}.
     * <p>
     * This method is idempotent.
     *
     * @return a Future representing pending completion of the operation
     */
    ICompletableFuture<Void> triggerRebalanceRaftGroups();

    /**
     * Unconditionally destroys the Raft group without using Raft mechanics.
     * This method should only used when the Raft group lose the majority and cannot progress anymore.
     * <p>
     * This method is idempotent, has no effect if the group is already destroyed.
     *
     * @return a Future representing pending completion of the operation
     */
    ICompletableFuture<Void> forceDestroyRaftGroup(RaftGroupId groupId);
}
