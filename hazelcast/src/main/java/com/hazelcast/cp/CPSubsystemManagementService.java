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

package com.hazelcast.cp;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.exception.CPGroupDestroyedException;

import java.util.Collection;

/**
 * The public API for managing CP members and groups.
 *
 * Unlike dynamic nature of Hazelcast clusters, the CP subsystem requires
 * manual intervention while expanding / shrinking its size, or when
 * a CP member crashes or becomes unreachable. When a CP member becomes
 * unreachable, it cannot be automatically removed from the CP subsystem
 * because it could be still alive and partitioned away.
 * <p>
 * Moreover, the current CP subsystem implementation works only in memory
 * without persisting any state to disk. It means that a crashed CP member
 * will not be able to recover by reloading its previous state. Therefore,
 * crashed CP members create a danger for gradually losing majority of
 * CP groups and eventually total loss of availability of the CP subsystem.
 * To prevent such kind of situations, {@link CPSubsystemManagementService}
 * offers APIs for dynamic management of CP members.
 * <p>
 * The CP subsystem relies on Hazelcast's failure detectors to test
 * reachability of CP members. To be able to remove a CP member from
 * the CP subsystem, it must be declared as unreachable by Hazelcast's failure
 * detector, and removed from Hazelcast's member list.
 * <p>
 * CP member additions and removals are internally handled by performing
 * a single membership change at a time. When multiple CP members are shutting
 * down concurrently, their shutdown process is executed serially. First,
 * the Metadata CP group creates a membership change plan for CP groups. Then,
 * scheduled changes are applied to CP groups one by one. After all removals
 * are done, the shutting down CP member is removed the active CP members list
 * and its shutdown process is completed.
 * <p>
 * When a CP member is being shut down, it is replaced with another available
 * CP member in all of its CP groups, including the Metadata group, in order to
 * not to decrease or more importantly not to lose majority of CP groups.
 * If there is no available CP member to replace a shutting down CP member in a
 * CP group, that group's size will be reduced by 1 and its majority value will
 * be recalculated.
 * <p>
 * A new CP member can be added to the CP subsystem to either increase number
 * of available CP members for new CP groups or to fulfill missing slots in
 * existing CP groups. After the initial Hazelcast cluster startup is done,
 * an existing Hazelcast member can be be promoted to the CP member role.
 * This new CP member will automatically join to CP groups that has missing
 * members, and majority value of these CP groups will be recalculated.
 * <p>
 * A CP member may crash due to hardware problems or a defect in user code,
 * or it may become unreachable because of connection problems, such as network
 * partitions, network hardware failures, etc. If a CP member is known to be
 * alive but only has temporary communication issues, it will catch up the
 * other CP members and continue to operate normally after its communication
 * issues are resolved. If it is known to be crashed or communication issues
 * cannot be resolved in a short time, it can be preferable to remove this CP
 * member from the CP subsystem, hence from all its CP groups. In this case,
 * unreachable CP member should be terminated to prevent any accidental
 * communication with the rest of the CP subsystem.
 * <p>
 * When majority of a CP group is lost for any reason, that CP group cannot
 * make progress anymore. Even a new CP member cannot join to this CP group,
 * because member additions also go through the Raft consensus algorithm.
 * For this reason, the only option is to force-destroy the CP group via the
 * {@link #forceDestroyCPGroup(String)} API. When this API is used, the CP
 * group is terminated non-gracefully, without the Raft algorithm mechanics.
 * Then, all CP data structure proxies that talk to this CP group fail with
 * {@link CPGroupDestroyedException}. However, if a new proxy is created
 * afterwards, then this CP group will be re-created from scratch with a new
 * set of CP members. Losing majority of a CP group can be likened to
 * partition-loss scenario of AP Hazelcast.
 * <p>
 * Please note that CP groups that have lost their majority must be
 * force-destroyed immediately, because they can block the Metadata CP group
 * to perform membership changes.
 * <p>
 * Loss of majority of the Metadata CP group is the doomsday scenario for the
 * CP subsystem. It is a fatal failure and the only solution is to reset the
 * whole CP subsystem state via the {@link #restart()} API. To be able to
 * reset the CP subsystem, the initial size of the CP subsystem must be
 * satisfied, which is defined by {@link CPSubsystemConfig#getCPMemberCount()}.
 * For instance, {@link CPSubsystemConfig#getCPMemberCount()} is 5 and only 1
 * CP member is currently alive, when {@link #restart()} is called,
 * additional 4 regular Hazelcast members should exist in the cluster.
 * New Hazelcast members can be started to satisfy
 * {@link CPSubsystemConfig#getCPMemberCount()}.
 *
 * @see CPMember
 * @see CPSubsystemConfig
 */
public interface CPSubsystemManagementService {

    /**
     * Returns all active CP group ids.
     */
    ICompletableFuture<Collection<CPGroupId>> getCPGroupIds();

    /**
     * Returns the active CP group with the given name.
     * There can be at most one active CP group with a given name.
     */
    ICompletableFuture<CPGroup> getCPGroup(String name);

    /**
     * Unconditionally destroys the given active CP group without using
     * the Raft algorithm mechanics. This method must be used only when
     * a CP group loses its majority and cannot make progress anymore.
     * Normally, membership changes in CP groups, such as CP member promotion
     * or removal, are done via the Raft consensus algorithm mechanics.
     * However, when a CP group loses its majority, it will not be able to
     * commit any new operation. Therefore, this method ungracefully terminates
     * remaining members of the given CP group. It also performs a Raft commit
     * to the Metadata CP group to update status of the destroyed group.
     * Once a CP group id is destroyed, all CP data structure proxies created
     * before the destroy will fail with {@link CPGroupDestroyedException}.
     * <p>
     * Once a CP group is destroyed, it can be created again with a new set of
     * CP members.
     * <p>
     * This method is idempotent. It has no effect if the given CP group is
     * already destroyed.
     *
     * @return a Future representing pending completion of the operation
     */
    ICompletableFuture<Void> forceDestroyCPGroup(String groupName);

    /**
     * Returns the current list of CP members
     *
     * @return the current list of CP members
     */
    ICompletableFuture<Collection<CPMember>> getCPMembers();

    /**
     * Promotes the local Hazelcast member to a CP member.
     * <p>
     * This method is idempotent.
     * If the local member is already in the active CP members list, then this
     * method will have no effect. When the current member is promoted to CP
     * member, its member UUID is assigned as CP member UUID.
     * <p>
     * If the local member is currently being removed from
     * the active CP members list, then the returning Future object
     * will throw {@link IllegalArgumentException}.
     *
     * @return a Future representing pending completion of the operation
     * @throws IllegalArgumentException If the local member is currently being
     *         removed from the active CP members list
     */
    ICompletableFuture<Void> promoteToCPMember();

    /**
     * Removes the given unreachable CP member from the active CP members list
     * and all CP groups it belongs to. If any other active CP member
     * is available, it will replace the removed CP member in its CP groups.
     * Otherwise, CP groups which the removed CP member is a member of will
     * shrink and their majority values will be recalculated.
     * <p>
     * This method can be invoked only from the Hazelcast master member.
     * <p>
     * This method is idempotent.
     * If the given member is not in the active CP members list,
     * then this method will have no effect.
     *
     * @return a Future representing pending completion of the operation
     * @throws IllegalStateException When member removal initiated by
     *         a non-master member or the given member is still member of
     *         the Hazelcast cluster or another CP member is being removed
     *         from the CP sub-system
     */
    ICompletableFuture<Void> removeCPMember(String cpMemberUuid);

    /**
     * Wipes & resets the whole CP subsystem and initializes it as if the Hazelcast
     * cluster is starting up initially.
     * This method must be used only when the Metadata CP group loses
     * its majority and cannot make progress anymore.
     * <p>
     * After this method is called, all CP state and data will be wiped
     * and CP members will start with empty state.
     * <p>
     * This method can be invoked only from the Hazelcast master member.
     * <p>
     * <strong>Use with caution:
     * This method is NOT idempotent and multiple invocations
     * can break the whole system!</strong>
     */
    ICompletableFuture<Void> restart();

}
