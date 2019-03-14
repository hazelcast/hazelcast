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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.exception.CPGroupDestroyedException;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * The public API for managing CP members and groups.
 *
 * Unlike the dynamic nature of Hazelcast clusters, the CP subsystem requires
 * manual intervention while expanding / shrinking its size, or when
 * a CP member crashes or becomes unreachable. When a CP member becomes
 * unreachable, it cannot be automatically removed from the CP subsystem
 * because it could be still alive and partitioned away.
 * <p>
 * Moreover, the current CP subsystem implementation works only in memory
 * without persisting any state to disk. It means that a crashed CP member
 * will not be able to recover by reloading its previous state. Therefore,
 * crashed CP members create a danger for gradually losing majority of
 * CP groups and eventually total loss of the availability of the CP subsystem.
 * To prevent such situations, {@link CPSubsystemManagementService} offers
 * APIs for dynamic management of CP members.
 * <p>
 * The CP subsystem relies on Hazelcast's failure detectors to test
 * reachability of CP members. Before removing a CP member from
 * the CP subsystem, please make sure that it is declared as unreachable by
 * Hazelcast's failure detector and removed from Hazelcast's member list.
 * <p>
 * CP member additions and removals are internally handled by performing
 * a single membership change at a time. When multiple CP members are shutting
 * down concurrently, their shutdown process is executed serially. First,
 * the Metadata CP group creates a membership change plan for CP groups. Then,
 * the scheduled changes are applied to the CP groups one by one.
 * After all removals are done, the shutting down CP member is removed from
 * the active CP members list and its shutdown process is completed.
 * <p>
 * When a CP member is being shut down, it is replaced with another available
 * CP member in all of its CP groups, including the Metadata group, in order to
 * not to decrease or more importantly not to lose the majority of CP groups.
 * If there is no available CP member to replace a shutting down CP member in a
 * CP group, that group's size is reduced by 1 and its majority value is
 * recalculated.
 * <p>
 * A new CP member can be added to the CP subsystem to either increase
 * the number of available CP members for new CP groups or to fulfill
 * the missing slots in the existing CP groups. After the initial Hazelcast
 * cluster startup is done, an existing Hazelcast member can be be promoted to
 * the CP member role. This new CP member automatically joins to CP groups that
 * have missing members, and the majority value of these CP groups is
 * recalculated.
 * <p>
 * A CP member may crash due to hardware problems or a defect in user code,
 * or it may become unreachable because of connection problems, such as network
 * partitions, network hardware failures, etc. If a CP member is known to be
 * alive but only has temporary communication issues, it will catch up the
 * other CP members and continue to operate normally after its communication
 * issues are resolved. If it is known to be crashed or communication issues
 * cannot be resolved in a short time, it can be preferable to remove this CP
 * member from the CP subsystem, hence from all its CP groups. In this case,
 * the unreachable CP member should be terminated to prevent any accidental
 * communication with the rest of the CP subsystem.
 * <p>
 * When the majority of a CP group is lost for any reason, that CP group cannot
 * make progress anymore. Even a new CP member cannot join to this CP group,
 * because membership changes also go through the Raft consensus algorithm.
 * For this reason, the only option is to force-destroy the CP group via the
 * {@link #forceDestroyCPGroup(String)} API. When this API is used, the CP
 * group is terminated non-gracefully, without the Raft algorithm mechanics.
 * Then, all CP data structure proxies that talk to this CP group fail with
 * {@link CPGroupDestroyedException}. However, if a new proxy is created
 * afterwards, then this CP group will be re-created from scratch with a new
 * set of CP members. Losing majority of a CP group can be likened to
 * partition-loss scenario of AP Hazelcast.
 * <p>
 * Please note that the CP groups that have lost their majority must be
 * force-destroyed immediately, because they can block the Metadata CP group
 * to perform membership changes.
 * <p>
 * Loss of the majority of the Metadata CP group is the doomsday scenario for
 * the CP subsystem. It is a fatal failure and the only solution is to reset
 * the whole CP subsystem state via the {@link #restart()} API. To be able to
 * reset the CP subsystem, the initial size of the CP subsystem must be
 * satisfied, which is defined by {@link CPSubsystemConfig#getCPMemberCount()}.
 * For instance, {@link CPSubsystemConfig#getCPMemberCount()} is 5 and only 1
 * CP member is currently alive, when {@link #restart()} is called,
 * additional 4 regular Hazelcast members should exist in the cluster.
 * New Hazelcast members can be started to satisfy
 * {@link CPSubsystemConfig#getCPMemberCount()}.
 * <p>
 * <strong>There is a subtle point about graceful shutdown of CP members.
 * If there are N CP members in the cluster, {@link HazelcastInstance#shutdown()}
 * can be called on N-2 CP members concurrently. Once these N-2 CP members
 * complete their shutdown, the remaining 2 CP members must be shut down
 * serially.
 * <p>
 * Even though the shutdown API is called concurrently on multiple members,
 * the Metadata CP group handles shutdown requests serially. Therefore,
 * it would be simpler to shut down CP members one by one, by calling
 * {@link HazelcastInstance#shutdown()} on the next CP member once the current
 * CP member completes its shutdown.
 * <p>
 * The reason behind this limitation is, each shutdown request internally
 * requires a Raft commit to the Metadata CP group. A CP member proceeds to
 * shutdown after it receives a response of its commit to the Metadata CP
 * group. To be able to perform a Raft commit, the Metadata CP group must have
 * its majority available. When there are only 2 CP members left after graceful
 * shutdowns, the majority of the Metadata CP group becomes 2. If the last 2 CP
 * members shut down concurrently, one of them is likely to perform its Raft
 * commit faster than the other one and leave the cluster before the other CP
 * member completes its Raft commit. In this case, the last CP member waits for
 * a response of its commit attempt on the Metadata group, and times out
 * eventually. This situation causes an unnecessary delay on shutdown process
 * of the last CP member. On the other hand, when the last 2 CP members shut
 * down serially, the N-1th member receives response of its commit after its
 * shutdown request is committed also on the last CP member. Then, the last CP
 * member checks its local data to notice that it is the last CP member alive,
 * and proceeds its shutdown without attempting a Raft commit on the Metadata
 * CP group.</strong>
 *
 * @see CPMember
 * @see CPSubsystemConfig
 */
public interface CPSubsystemManagementService {

    /**
     * Returns the local CP member if this Hazelcast member is part of
     * the CP subsystem, returns null otherwise.
     * <p>
     * This field is initialized when the local Hazelcast member is one of
     * the first {@link CPSubsystemConfig#getCPMemberCount()} members
     * in the cluster and the CP subsystem discovery process is completed.
     * <p></p>
     * This method fails with {@link HazelcastException} if the CP subsystem
     * is not enabled.
     *
     * @return local CP member if available, null otherwise
     * @throws HazelcastException if the CP subsystem is not enabled
     * @see #isDiscoveryCompleted()
     * @see #awaitUntilDiscoveryCompleted(long, TimeUnit)
     */
    CPMember getLocalCPMember();

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
     * or removal, are done via the Raft consensus algorithm. However, when
     * a CP group loses its majority, it will not be able to commit any new
     * operation. Therefore, this method ungracefully terminates the remaining
     * members of the given CP group. It also performs a Raft commit to
     * the Metadata CP group in order to update status of the destroyed group.
     * Once a CP group id is destroyed, all CP data structure proxies created
     * before the destroy fails with {@link CPGroupDestroyedException}.
     * <p>
     * Once a CP group is destroyed, it can be created again with a new set of
     * CP members.
     * <p>
     * This method is idempotent. It has no effect if the given CP group is
     * already destroyed.
     */
    ICompletableFuture<Void> forceDestroyCPGroup(String groupName);

    /**
     * Returns the current list of CP members
     */
    ICompletableFuture<Collection<CPMember>> getCPMembers();

    /**
     * Promotes the local Hazelcast member to a CP member.
     * <p>
     * This method is idempotent.
     * If the local member is already in the active CP members list, then this
     * method has no effect. When the current member is promoted to a CP
     * member, its member UUID is assigned as CP member UUID.
     * <p>
     * Once the returned {@code Future} object is completed, the promoted CP
     * member has been added to the CP groups that have missing members, i.e.,
     * whose size is smaller than {@link CPSubsystemConfig#getGroupSize()}.
     * <p>
     * If the local member is currently being removed from
     * the active CP members list, then the returned {@code Future} object
     * will throw {@link IllegalArgumentException}.
     * <p>
     * If there is an ongoing membership change in the CP subsystem when this
     * method is invoked, then the returned {@code Future} object throws
     * {@link IllegalStateException}
     * <p>
     * If the CP subsystem initial discovery process has not completed when
     * this method is invoked, then the returned {@code Future} object throws
     * {@link IllegalStateException}
     *
     * @throws IllegalArgumentException If the local member is currently being
     *         removed from the active CP members list
     * @throws IllegalStateException If there is an ongoing membership change
     *         in the CP subsystem
     */
    ICompletableFuture<Void> promoteToCPMember();

    /**
     * Removes the given unreachable CP member from the active CP members list
     * and all CP groups it belongs to. If any other active CP member
     * is available, it will replace the removed CP member in its CP groups.
     * Otherwise, CP groups which the removed CP member is a member of will
     * shrink and their majority values will be recalculated.
     * <p>
     * Before removing a CP member from the CP subsystem, please make sure that
     * it is declared as unreachable by Hazelcast's failure detector and removed
     * from Hazelcast's member list. The behavior is undefined when a running
     * CP member is removed from the CP subsystem.
     *
     * @throws IllegalStateException When another CP member is being removed
     *         from the CP subsystem
     * @throws IllegalArgumentException if the given CP member is already
     *         removed from the CP member list
     */
    ICompletableFuture<Void> removeCPMember(String cpMemberUuid);

    /**
     * Wipes and resets the whole CP subsystem and initializes it
     * as if the Hazelcast cluster is starting up initially.
     * This method must be used only when the Metadata CP group loses
     * its majority and cannot make progress anymore.
     * <p>
     * After this method is called, all CP state and data are wiped
     * and the CP members start with empty state.
     * <p>
     * This method can be invoked only from the Hazelcast master member.
     * <p>
     * This method must not be called while there are membership changes
     * in the cluster. Before calling this method, please make sure that
     * there is no new member joining and all existing Hazelcast members
     * have seen the same member list.
     * <p>
     * <strong>Use with caution:
     * This method is NOT idempotent and multiple invocations can break
     * the whole system! After calling this API, you must observe the system
     * to see if the restart process is successfully completed or failed
     * before making another call.</strong>
     *
     * @throws IllegalStateException When this method is called on
     *         a Hazelcast member that is not the Hazelcast cluster master
     * @throws IllegalStateException if current member count of the cluster
     *         is smaller than {@link CPSubsystemConfig#getCPMemberCount()}
     *
     */
    ICompletableFuture<Void> restart();

    /**
     * Returns whether CP discovery process is completed or not.
     *
     * @return {@code true} if CP discovery completed, {@code false} otherwise
     * @see #awaitUntilDiscoveryCompleted(long, TimeUnit)
     */
    boolean isDiscoveryCompleted();

    /**
     * Blocks until CP discovery process is completed, or the timeout occurs,
     * or the current thread is interrupted, whichever happens first.
     *
     * @param timeout  maximum time to wait
     * @param timeUnit time unit of the timeout
     * @return {@code true} if CP discovery completed, {@code false} otherwise
     * @throws InterruptedException if interrupted while waiting
     * @see #isDiscoveryCompleted()
     */
    boolean awaitUntilDiscoveryCompleted(long timeout, TimeUnit timeUnit) throws InterruptedException;
}
