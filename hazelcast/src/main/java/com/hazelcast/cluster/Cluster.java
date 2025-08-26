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

package com.hazelcast.cluster;

import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.persistence.PersistenceService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.version.Version;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.UUID;

/**
 * Hazelcast cluster interface. It provides access to the members in the cluster and one can register for changes in the
 * cluster members.
 * <p>
 * All the methods on the Cluster are thread-safe.
 */
public interface Cluster {

    /**
     * Adds MembershipListener to listen for membership updates.
     * <p>
     * The addMembershipListener method returns a registration ID. This ID is needed to remove the MembershipListener using the
     * {@link #removeMembershipListener(UUID)} method.
     * <p>
     * If the MembershipListener implements the {@link InitialMembershipListener} interface, it will also receive
     * the {@link InitialMembershipEvent}.
     * <p>
     * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
     * The listener doesn't notify when a lite member is promoted to a data member.
     *
     * @param listener membership listener
     * @return the registration ID
     * @throws java.lang.NullPointerException if listener is null
     * @see #removeMembershipListener(UUID)
     */
    @Nonnull
    UUID addMembershipListener(@Nonnull MembershipListener listener);

    /**
     * Removes the specified MembershipListener.
     * <p>
     * If the same MembershipListener is registered multiple times, it needs to be removed multiple times.
     *
     * This method can safely be called multiple times for the same registration ID; subsequent calls are ignored.
     *
     * @param registrationId the registrationId of MembershipListener to remove
     * @return true if the registration is removed, false otherwise
     * @throws java.lang.NullPointerException if the registration ID is null
     * @see #addMembershipListener(MembershipListener)
     */
    boolean removeMembershipListener(@Nonnull UUID registrationId);

    /**
     * Set of the current members in the cluster. The returned set is an immutable set; it can't be modified.
     * <p>
     * The returned set is backed by an ordered set. Every member in the cluster returns the 'members' in the same order.
     * To obtain the oldest member (the master) in the cluster, you can retrieve the first item in the set using
     * 'getMembers().iterator().next()'.
     *
     * @return current members in the cluster
     */
    @Nonnull
    Set<Member> getMembers();

    /**
     * Returns this Hazelcast instance member.
     * <p>
     * The returned value will never be null, but it may change when local lite member is promoted to a data member
     * via {@link #promoteLocalLiteMember()}
     * or when this member merges to a new cluster after split-brain detected. Returned value should not be
     * cached but instead this method should be called each time when local member is needed.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @return this Hazelcast instance member
     */
    @Nonnull
    Member getLocalMember();

    /**
     * Promotes the local lite member to a data member.
     * When this method returns, both {@link #getLocalMember()} and {@link #getMembers()}
     * reflect the promotion.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @throws IllegalStateException when member is not a lite member or mastership claim is in progress
     *                               or local member cannot be identified as a member of the cluster
     *                               or cluster state doesn't allow migrations/repartitioning
     * @since 3.9
     */
    void promoteLocalLiteMember();

    /**
     * Demotes the local data member to a lite member.
     * When this method returns, both {@link #getLocalMember()} and {@link #getMembers()}
     * reflect the demotion.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @throws IllegalStateException when member is not a data member or mastership claim is in progress
     *                               or local member cannot be identified as a member of the cluster
     *                               or cluster state doesn't allow migrations/repartitioning
     *                               or the member is the only data member in the cluster.
     * @since 5.4
     */
    void demoteLocalDataMember();

    /**
     * Returns the cluster-wide time in milliseconds.
     * <p>
     * Cluster tries to keep a cluster-wide time which might be different than the member's own system time.
     * Cluster-wide time is -almost- the same on all members of the cluster.
     *
     * @return cluster-wide time
     */
    long getClusterTime();

    /**
     * Returns the state of the cluster.
     * <p>
     * If cluster state change is in process, {@link ClusterState#IN_TRANSITION} will be returned.
     * <p>
     * This is a local operation, state will be read directly from local member.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @return state of the cluster
     * @since 3.6
     */
    @Nonnull ClusterState getClusterState();

    /**
     * Changes state of the cluster to the given state transactionally. Transaction will be
     * {@code TWO_PHASE} and will have 1 durability by default. If you want to override
     * transaction options, use {@link #changeClusterState(ClusterState, TransactionOptions)}.
     * <p>
     * If the given state is already same as
     * current state of the cluster, then this method will have no effect.
     * <p>
     * If there's an ongoing state change transaction in the cluster, this method will fail
     * immediately with a {@code TransactionException}.
     * <p>
     * If a membership change occurs in the cluster during state change, a new member joins or
     * an existing member leaves, then this method will fail with an {@code IllegalStateException}.
     * <p>
     * If there are ongoing/pending migration/replication operations, because of re-balancing due to
     * member join or leave, then trying to change from {@code ACTIVE} to {@code FROZEN}
     * or {@code PASSIVE} will fail with an {@code IllegalStateException}.
     * <p>
     * If transaction timeouts during state change, then this method will fail with a {@code TransactionException}.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @param newState new state of the cluster
     * @throws NullPointerException     if newState is null
     * @throws IllegalArgumentException if newState is {@link ClusterState#IN_TRANSITION}
     * @throws IllegalStateException    if member-list changes during the transaction
     *                                  or there are ongoing/pending migration operations
     * @throws TransactionException     if there's already an ongoing transaction
     *                                  or this transaction fails
     *                                  or this transaction timeouts
     * @since 3.6
     */
    void changeClusterState(@Nonnull ClusterState newState);

    /**
     * Changes state of the cluster to the given state transactionally. Transaction must be a
     * {@code TWO_PHASE} transaction.
     * <p>
     * If the given state is already same as
     * current state of the cluster, then this method will have no effect.
     * <p>
     * If there's an ongoing state change transaction in the cluster, this method will fail
     * immediately with a {@code TransactionException}.
     * <p>
     * If a membership change occurs in the cluster during state change, a new member joins or
     * an existing member leaves, then this method will fail with an {@code IllegalStateException}.
     * <p>
     * If there are ongoing/pending migration/replication operations, because of re-balancing due to
     * member join or leave, then trying to change from {@code ACTIVE} to {@code FROZEN}
     * or {@code PASSIVE} will fail with an {@code IllegalStateException}.
     * <p>
     * If transaction timeouts during state change, then this method will fail with a {@code TransactionException}.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @param newState           new state of the cluster
     * @param transactionOptions transaction options
     * @throws NullPointerException     if newState is null
     * @throws IllegalArgumentException if newState is {@link ClusterState#IN_TRANSITION}
     *                                  or transaction type is not {@code TWO_PHASE}
     * @throws IllegalStateException    if member-list changes during the transaction
     *                                  or there are ongoing/pending migration operations
     * @throws TransactionException     if there's already an ongoing transaction
     *                                  or this transaction fails
     *                                  or this transaction timeouts
     * @since 3.6
     */
    void changeClusterState(@Nonnull ClusterState newState, @Nonnull TransactionOptions transactionOptions);

    /**
     * The cluster version indicates the operating version of the cluster. It is separate from each node's codebase version,
     * as it may be acceptable for a node to operate at a different compatibility version than its codebase version. This method
     * may return Version.UNKNOWN if invoked after the ClusterService is constructed and before the node forms a cluster, either
     * by joining existing members or becoming master of its standalone cluster if it is the first node on the cluster.
     * Importantly, this is the time during which a lifecycle event with state
     * {@link com.hazelcast.core.LifecycleEvent.LifecycleState#STARTING} is triggered.
     * <p>
     * For example, consider a cluster comprised of nodes running on {@code hazelcast-3.8.0.jar}. Each node's codebase version
     * is 3.8.0 and on startup the cluster version is 3.8. After a while, another node joins, running on
     * {@code hazelcast-3.9.jar}; this node's codebase version is 3.9.0. If deemed compatible, it is allowed to join the cluster.
     * At this point, the cluster version is still 3.8 and the 3.9.0 member should be able to adapt its behaviour to be compatible
     * with the other 3.8.0 members. Once all 3.8.0 members have been shutdown and replaced by other members on codebase
     * version 3.9.0, still the cluster version will be 3.8. At this point, it is possible to update the cluster version to
     * 3.9, since all cluster members will be compatible with the new cluster version. Once cluster version
     * is updated to 3.9, further communication among members will take place in 3.9 and all new features and functionality
     * of version 3.9 will be available.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @return the version at which this cluster operates.
     * @since 3.8
     */
    @Nonnull
    Version getClusterVersion();

    /**
     * Use {@link Cluster#getPersistenceService()} instead.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @deprecated
     */
    @Deprecated(since = "5.0")
    HotRestartService getHotRestartService();

    /**
     * Returns the public persistence service for interacting with Persistence
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @return the persistence service
     * @throws UnsupportedOperationException if the persistence service is not
     * supported on this instance (e.g. on client)
     * @since 5.0
     */
    @Nonnull
    PersistenceService getPersistenceService();

    /**
     * Changes state of the cluster to the {@link ClusterState#PASSIVE} transactionally,
     * then triggers the shutdown process on each node. Transaction will be {@code TWO_PHASE}
     * and will have 1 durability by default. If you want to override transaction options,
     * use {@link #shutdown(TransactionOptions)}.
     * <p>
     * If the cluster is already in {@link ClusterState#PASSIVE}, shutdown process begins immediately.
     * All the node join / leave rules described in {@link ClusterState#PASSIVE} state also applies here.
     * <p>
     * Any node can start the shutdown process. A shutdown command is sent to other nodes periodically until
     * either all other nodes leave the cluster or a configurable timeout occurs
     * (see {@link ClusterProperty#CLUSTER_SHUTDOWN_TIMEOUT_SECONDS}). If some of the nodes do not
     * shutdown before the timeout duration, shutdown can be also invoked on them.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @throws IllegalStateException if member-list changes during the transaction
     *                               or there are ongoing/pending migration operations
     *                               or shutdown process times out
     * @throws TransactionException  if there's already an ongoing transaction
     *                               or this transaction fails
     *                               or this transaction timeouts
     * @see ClusterProperty#CLUSTER_SHUTDOWN_TIMEOUT_SECONDS
     * @see #changeClusterState(ClusterState)
     * @see ClusterState#PASSIVE
     * @since 3.6
     */
    void shutdown();

    /**
     * Changes state of the cluster to the {@link ClusterState#PASSIVE} transactionally, then
     * triggers the shutdown process on each node. Transaction must be a {@code TWO_PHASE} transaction.
     * <p>
     * If the cluster is already in {@link ClusterState#PASSIVE}, shutdown process begins immediately.
     * All the node join / leave rules described in {@link ClusterState#PASSIVE} state also applies here.
     * <p>
     * Any node can start the shutdown process. A shutdown command is sent to other nodes periodically until
     * either all other nodes leave the cluster or a configurable timeout occurs
     * (see {@link ClusterProperty#CLUSTER_SHUTDOWN_TIMEOUT_SECONDS}). If some of the nodes do not
     * shutdown before the timeout duration, shutdown can be also invoked on them.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @param transactionOptions transaction options
     * @throws IllegalStateException if member-list changes during the transaction
     *                               or there are ongoing/pending migration operations
     *                               or shutdown process times out
     * @throws TransactionException  if there's already an ongoing transaction
     *                               or this transaction fails
     *                               or this transaction timeouts
     * @see ClusterProperty#CLUSTER_SHUTDOWN_TIMEOUT_SECONDS
     * @see #changeClusterState(ClusterState)
     * @see ClusterState#PASSIVE
     * @since 3.6
     */
    void shutdown(@Nullable TransactionOptions transactionOptions);

    /**
     * Changes the cluster version transactionally. Internally this method uses the same transaction infrastructure as
     * {@link #changeClusterState(ClusterState)} and the transaction defaults are the same in this case as well
     * ({@code TWO_PHASE} transaction with durability 1 by default).
     * <p>
     * If the requested cluster version is same as the current one, nothing happens.
     * <p>
     * If a member of the cluster is not compatible with the given cluster {@code version}, as implemented by
     * {@link NodeExtension#isNodeVersionCompatibleWith(Version)}, then a
     * {@link com.hazelcast.internal.cluster.impl.VersionMismatchException} is thrown.
     * <p>
     * If an invalid version transition is requested, for example changing to a different major version, an
     * {@link IllegalArgumentException} is thrown.
     * <p>
     * If a membership change occurs in the cluster during locking phase, a new member joins or
     * an existing member leaves, then this method will fail with an {@code IllegalStateException}.
     * <p>
     * Likewise, once locking phase is completed successfully, {@link Cluster#getClusterState()}
     * will report being {@link ClusterState#IN_TRANSITION}, disallowing membership changes until the new cluster version is
     * committed.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @param version new version of the cluster
     * @since 3.8
     */
    void changeClusterVersion(@Nonnull Version version);

    /**
     * Changes the cluster version transactionally, with the transaction options provided. Internally this method uses the same
     * transaction infrastructure as {@link #changeClusterState(ClusterState, TransactionOptions)}. The transaction
     * options must specify a {@code TWO_PHASE} transaction.
     * <p>
     * If the requested cluster version is same as the current one, nothing happens.
     * <p>
     * If a member of the cluster is not compatible with the given cluster {@code version}, as implemented by
     * {@link NodeExtension#isNodeVersionCompatibleWith(Version)}, then a
     * {@link com.hazelcast.internal.cluster.impl.VersionMismatchException} is thrown.
     * <p>
     * If an invalid version transition is requested, for example changing to a different major version, an
     * {@link IllegalArgumentException} is thrown.
     * <p>
     * If a membership change occurs in the cluster during locking phase, a new member joins or
     * an existing member leaves, then this method will fail with an {@code IllegalStateException}.
     * <p>
     * Likewise, once locking phase is completed successfully, {@link Cluster#getClusterState()}
     * will report being {@link ClusterState#IN_TRANSITION}, disallowing membership changes until the new cluster version is
     * committed.
     * <p>
     * Supported only for members of the cluster, clients will throw a {@code UnsupportedOperationException}.
     *
     * @param version new version of the cluster
     * @param options options by which to execute the transaction
     * @since 3.8
     */
    void changeClusterVersion(@Nonnull Version version,
                              @Nonnull TransactionOptions options);
}
