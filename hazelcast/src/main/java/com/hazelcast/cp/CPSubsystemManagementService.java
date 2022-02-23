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

package com.hazelcast.cp;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.exception.CPGroupDestroyedException;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * This interface offers APIs for managing CP members and groups.
 * <p>
 * Unlike the dynamic nature of Hazelcast clusters, CP Subsystem requires
 * manual intervention while expanding/shrinking its size, or when a CP member
 * crashes or becomes unreachable. When a CP member becomes unreachable, it
 * is not automatically removed from CP Subsystem because it could be still
 * alive and partitioned away.
 * <p>
 * Moreover, by default CP Subsystem works in memory without persisting any
 * state to disk. It means that a crashed CP member will not be able to recover
 * by reloading its CP identity and state. Therefore, crashed CP members create
 * a danger for gradually losing majority of CP groups and eventually total
 * loss of the availability of CP Subsystem. To prevent such situations,
 * {@link CPSubsystemManagementService} offers a set of APIs. In addition, CP
 * Subsystem Persistence can be enabled to make CP members persist their CP
 * state to stable storage. Please see {@link CPSubsystem} and
 * {@link CPSubsystemConfig#setPersistenceEnabled(boolean)} for more details
 * about CP Subsystem Persistence.
 * <p>
 * CP Subsystem relies on Hazelcast's failure detectors to test reachability of
 * CP members. Before removing a CP member from CP Subsystem, please make sure
 * that it is declared as unreachable by Hazelcast's failure detector and
 * removed from Hazelcast cluster's member list.
 * <p>
 * CP member additions and removals are internally handled by performing
 * a single membership change at a time. When multiple CP members are shutting
 * down concurrently, their shutdown process is executed serially. When a CP
 * membership change is triggered, the METADATA CP group creates a membership
 * change plan for CP groups. Then, the scheduled changes are applied to the CP
 * groups one by one. After all CP group member removals are done, the shutting
 * down CP member is removed from the active CP members list and its shutdown
 * process is completed. A shut-down CP member is automatically replaced with
 * another available CP member in all of its CP groups, including the METADATA
 * CP group, in order not to decrease or more importantly not to lose
 * the majority of CP groups. If there is no available CP member to replace
 * a shutting down CP member in a CP group, that group's size is reduced by 1
 * and its majority value is recalculated. Please note that this behaviour is
 * when CP Subsystem Persistence is disabled. When CP Subsystem Persistence is
 * enabled, shut-down CP members are not automatically removed from the active
 * CP members list and they are still considered as part of CP groups
 * and majority calculations, because they can come back by restoring their
 * local CP state from stable storage. If you know that a shut-down CP member
 * will not be restarted, you need to remove that member from CP Subsystem via
 * {@link #removeCPMember(UUID)}.
 * <p>
 * A new CP member can be added to CP Subsystem to either increase the number
 * of available CP members for new CP groups or to fill the missing slots in
 * existing CP groups. After the initial Hazelcast cluster startup is done,
 * an existing Hazelcast member can be be promoted to the CP member role. This
 * new CP member automatically joins to CP groups that have missing members,
 * and majority values of these CP groups are recalculated.
 *
 * @see CPSubsystem
 * @see CPMember
 * @see CPSubsystemConfig
 */
public interface CPSubsystemManagementService {

    /**
     * Returns the local CP member if this Hazelcast member is part of
     * CP Subsystem, returns null otherwise.
     * <p>
     * This field is initialized when the local Hazelcast member is one of
     * the first {@link CPSubsystemConfig#getCPMemberCount()} members
     * in the cluster and the CP discovery process is completed.
     * <p></p>
     * This method fails with {@link HazelcastException} if CP Subsystem is not
     * enabled.
     *
     * @return local CP member if available, null otherwise
     * @throws HazelcastException if CP Subsystem is not enabled
     * @see #isDiscoveryCompleted()
     * @see #awaitUntilDiscoveryCompleted(long, TimeUnit)
     */
    CPMember getLocalCPMember();

    /**
     * Returns all active CP group ids.
     */
    CompletionStage<Collection<CPGroupId>> getCPGroupIds();

    /**
     * Returns the active CP group with the given name.
     * There can be at most one active CP group with a given name.
     */
    CompletionStage<CPGroup> getCPGroup(String name);

    /**
     * Unconditionally destroys the given active CP group without using
     * the Raft algorithm mechanics. This method must be used only when
     * a CP group permanently loses its majority and cannot make progress
     * anymore. Normally, membership changes in CP groups, such as CP member
     * promotion or removal, are done via the Raft consensus algorithm.
     * However, when a CP group permanently loses its majority, it will not be
     * able to commit any new operation. Therefore, this method ungracefully
     * terminates execution of the Raft algorithm for this CP group on
     * the remaining CP group members. It also performs a Raft commit to
     * the METADATA CP group in order to update the status of the destroyed CP
     * group. Once a CP group is destroyed, all CP data structure proxies
     * created before the destroy call fails with
     * {@link CPGroupDestroyedException}. However, if a new proxy is created
     * afterwards, then this CP group is re-created from scratch with a new set
     * of CP members.
     * <p>
     * This method is idempotent. It has no effect if the given CP group is
     * already destroyed.
     */
    CompletionStage<Void> forceDestroyCPGroup(String groupName);

    /**
     * Returns the current list of CP members
     */
    CompletionStage<Collection<CPMember>> getCPMembers();

    /**
     * Promotes the local Hazelcast member to the CP role.
     * <p>
     * This method is idempotent.
     * If the local member is already in the active CP members list, i.e.,
     * it is already a CP member, then this method has no effect. When
     * the local member is promoted to the CP role, its member UUID is
     * assigned as its CP member UUID.
     * <p>
     * Once the returned {@code Future} object is completed, the promoted CP
     * member has been added to CP groups that have missing CP members, i.e.,
     * whose current size is smaller than
     * {@link CPSubsystemConfig#getGroupSize()}.
     * <p>
     * If the local member is currently being removed from
     * the active CP members list, then the returned {@code Future} object
     * will throw {@link IllegalArgumentException}.
     * <p>
     * If there is an ongoing membership change in CP Subsystem when this
     * method is invoked, then the returned {@code Future} object throws
     * {@link IllegalStateException}.
     * <p>
     * If the CP discovery process has not completed yet when this method is
     * invoked, then the returned {@code Future} object throws
     * {@link IllegalStateException}.
     * <p>
     * If the local member is a lite member, the returned {@code Future} object
     * throws {@link IllegalStateException}.
     */
    CompletionStage<Void> promoteToCPMember();

    /**
     * Removes the given unreachable CP member from the active CP members list
     * and all CP groups it belongs to. If any other active CP member
     * is available, it replaces the removed CP member in its CP groups.
     * Otherwise, CP groups which the removed CP member is a member of shrinks
     * and their majority values are recalculated.
     * <p>
     * Before removing a CP member from CP Subsystem, please make sure that
     * it is declared as unreachable by Hazelcast's failure detector and removed
     * from Hazelcast's member list. The behavior is undefined when a running
     * CP member is removed from CP Subsystem.
     *
     * @throws IllegalStateException When another CP member is currently being
     *         removed from CP Subsystem
     * @throws IllegalArgumentException if the given CP member is already
     *         removed from CP Subsystem
     */
    CompletionStage<Void> removeCPMember(UUID cpMemberUuid);

    /**
     * Wipes and resets the whole CP Subsystem state and initializes it
     * as if the Hazelcast cluster is starting up initially.
     * This method must be used only when the METADATA CP group loses
     * its majority and cannot make progress anymore.
     * <p>
     * After this method is called, all CP state and data are wiped
     * and CP members start with empty state.
     * <p>
     * This method can be invoked only from the Hazelcast master member, which
     * is the first member in the Hazelcast cluster member list.
     * <p>
     * This method must not be called while there are membership changes
     * in the Hazelcast cluster. Before calling this method, please make sure
     * that there is no new member joining and all existing Hazelcast members
     * have seen the same member list.
     * <p>
     * To be able to use this method, the initial CP member count of CP
     * Subsystem, which is defined by
     * {@link CPSubsystemConfig#getCPMemberCount()}, must be satisfied. For
     * instance, if {@link CPSubsystemConfig#getCPMemberCount()} is 5 and
     * only 1 CP member is alive, when this method is called, 4 additional AP
     * Hazelcast members should exist in the cluster, or new Hazelcast members
     * must be started.
     * <p>
     * This method also deletes all data written by CP Subsystem Persistence.
     * <p>
     * This method triggers a new CP discovery process round. However, if
     * the new CP discovery round fails for any reason, Hazelcast members are
     * not terminated, because Hazelcast members are likely to contain data for
     * AP data structures and their termination can cause data loss. Hence, you
     * need to observe the cluster and check if the CP discovery process
     * completes successfully.
     * <p>
     * <strong>Use with caution:
     * This method is NOT idempotent and multiple invocations can break
     * the whole system! After calling this API, you must observe the system
     * to see if the reset process is successfully completed or failed
     * before making another call.</strong>
     *
     * @throws IllegalStateException When this method is called on
     *         a Hazelcast member that is not the Hazelcast cluster master
     * @throws IllegalStateException if current member count of the cluster
     *         is smaller than {@link CPSubsystemConfig#getCPMemberCount()}
     *
     */
    CompletionStage<Void> reset();

    /**
     * Returns whether the CP discovery process is completed or not.
     *
     * @return {@code true} if the CP discovery process completed,
     *         {@code false} otherwise
     *
     * @see #awaitUntilDiscoveryCompleted(long, TimeUnit)
     */
    boolean isDiscoveryCompleted();

    /**
     * Blocks until the CP discovery process is completed, or the given
     * timeout occurs, or the current thread is interrupted, whichever
     * happens first.
     *
     * @param timeout  maximum time to wait
     * @param timeUnit time unit of the timeout
     * @return {@code true} if CP discovery completed, {@code false} otherwise
     * @throws InterruptedException if interrupted while waiting
     * @see #isDiscoveryCompleted()
     */
    boolean awaitUntilDiscoveryCompleted(long timeout, TimeUnit timeUnit) throws InterruptedException;
}
