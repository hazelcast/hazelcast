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

package com.hazelcast.raft;

import com.hazelcast.core.ICompletableFuture;

import java.util.Collection;

/**
 * The public API used for managing CP nodes and Raft groups.
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
     * Wipes & resets the local Raft state and initializes it as if this node starting up initially.
     * This method must be used only when the Metadata Raft group loses its majority and cannot make progress anymore.
     * <p>
     * After this method is called, all Raft state and data will be wiped and Raft members will start empty.
     * <p>
     * <strong>Use with caution:
     * This method is NOT idempotent and multiple invocations on the same member can break the whole system!</strong>
     */
    void resetAndInitRaftState();

    /**
     * Promotes the local Hazelcast cluster member to a Raft cluster member.
     * <p>
     * This method is idempotent.
     * If the local member is in the active Raft members list already, then this method will have no effect.
     * <p>
     * If the local member is currently being removed from the active Raft members list,
     * then the returning Future object will throw {@link IllegalArgumentException}.
     *
     * @return a Future representing pending completion of the operation
     * @throws IllegalArgumentException If the local member is currently being removed from the active Raft members list
     */
    ICompletableFuture<Void> triggerRaftMemberPromotion();

    /**
     * Removes the given unreachable member from the active Raft members list and all Raft groups it belongs to.
     * If other active Raft members are available, they will replace the removed member in the Raft groups.
     * Otherwise, the Raft groups the removed member is a member of will shrink and their majority values will be recalculated.
     * <p>
     * This method can be invoked only from the Hazelcast master node.
     * <p>
     * This method is idempotent.
     * If the given member is not in the active Raft members list, then this method will have no effect.
     *
     * @return a Future representing pending completion of the operation
     * @throws IllegalStateException When member removal initiated by a non-master member
     *                               or the given member is still member of the Hazelcast cluster
     *                               or another CP member is being removed from the CP sub-system
     */
    ICompletableFuture<Void> triggerRemoveRaftMember(RaftMember member);

    /**
     * Unconditionally destroys the Raft group without using Raft mechanics.
     * This method must be used only when the given Raft group loses its majority and cannot make progress anymore.
     * Normally, membership changes in Raft groups are done via the Raft algorithm.
     * However, this method forcefully terminates the remaining nodes of the given Raft group.
     * It also performs a Raft commit to the Metadata group in order to update status of the destroyed group.
     * <p>
     * This method is idempotent. It has no effect if the given Raft group is already destroyed.
     *
     * @return a Future representing pending completion of the operation
     */
    ICompletableFuture<Void> forceDestroyRaftGroup(RaftGroupId groupId);
}
