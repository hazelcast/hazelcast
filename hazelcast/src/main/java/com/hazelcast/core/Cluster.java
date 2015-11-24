/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;

import java.util.Set;

/**
 * Hazelcast cluster interface. It provides access to the members in the cluster and one can register for changes in the
 * cluster members.
 * <p/>
 * All the methods on the Cluster are thread-safe.
 */
public interface Cluster {

    /**
     * Adds MembershipListener to listen for membership updates.
     * <p/>
     * The addMembershipListener method returns a register-id. This id is needed to remove the MembershipListener using the
     * {@link #removeMembershipListener(String)} method.
     * <p/>
     * If the MembershipListener implements the {@link InitialMembershipListener} interface, it will also receive
     * the {@link InitialMembershipEvent}.
     * <p/>
     * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
     *
     * @param listener membership listener
     * @return the registration id.
     * @throws java.lang.NullPointerException if listener is null.
     * @see #removeMembershipListener(String)
     */
    String addMembershipListener(MembershipListener listener);

    /**
     * Removes the specified MembershipListener.
     * <p/>
     * If the same MembershipListener is registered multiple times, it needs to be removed multiple times.
     *
     * This method can safely be called multiple times for the same registration-id; subsequent calls are ignored.
     *
     * @param registrationId the registrationId of MembershipListener to remove.
     * @return true if the registration is removed, false otherwise.
     * @throws java.lang.NullPointerException if the registration id is null.
     * @see #addMembershipListener(MembershipListener)
     */
    boolean removeMembershipListener(String registrationId);

    /**
     * Set of the current members in the cluster. The returned set is an immutable set; it can't be modified.
     * <p/>
     * The returned set is backed by an ordered set. Every member in the cluster returns the 'members' in the same order.
     * To obtain the oldest member (the master) in the cluster, you can retrieve the first item in the set using
     * 'getMembers().iterator().next()'.
     *
     * @return current members in the cluster
     */
    Set<Member> getMembers();

    /**
     * Returns this Hazelcast instance member.
     *
     * @return this Hazelcast instance member
     */
    Member getLocalMember();

    /**
     * Returns the cluster-wide time in milliseconds.
     * <p/>
     * Cluster tries to keep a cluster-wide time which might be different than the member's own system time.
     * Cluster-wide time is -almost- the same on all members of the cluster.
     *
     * @return cluster-wide time
     */
    long getClusterTime();

    /**
     * Returns the state of the cluster.
     * <p/>
     * If cluster state change is in process, {@link ClusterState#IN_TRANSITION} will be returned.
     * <p/>
     * This is a local operation, state will be read directly from local member.
     *
     * @return state of the cluster
     * @since 3.6
     */
    ClusterState getClusterState();

    /**
     * Changes state of the cluster to the given state transactionally. Transaction will be
     * {@code TWO_PHASE} and will have 1 durability by default. If you want to override
     * transaction options, use {@link #changeClusterState(ClusterState, TransactionOptions)}.
     * <p/>
     * If the given state is already same as
     * current state of the cluster, then this method will have no effect.
     * <p/>
     * If there's an ongoing state change transaction in the cluster, this method will fail
     * immediately with a {@code TransactionException}.
     * <p/>
     * If a membership change occurs in the cluster during state change, a new member joins or
     * an existing member leaves, then this method will fail with an {@code IllegalStateException}.
     * <p/>
     * If there are ongoing/pending migration/replication operations, because of re-balancing due to
     * member join or leave, then trying to change from {@code ACTIVE} to {@code FROZEN}
     * or {@code PASSIVE} will fail with an {@code IllegalStateException}.
     * <p/>
     * If transaction timeouts during state change, then this method will fail with a {@code TransactionException}.
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
    void changeClusterState(ClusterState newState);

    /**
     * Changes state of the cluster to the given state transactionally. Transaction must be a
     * {@code TWO_PHASE} transaction.
     * <p/>
     * If the given state is already same as
     * current state of the cluster, then this method will have no effect.
     * <p/>
     * If there's an ongoing state change transaction in the cluster, this method will fail
     * immediately with a {@code TransactionException}.
     * <p/>
     * If a membership change occurs in the cluster during state change, a new member joins or
     * an existing member leaves, then this method will fail with an {@code IllegalStateException}.
     * <p/>
     * If there are ongoing/pending migration/replication operations, because of re-balancing due to
     * member join or leave, then trying to change from {@code ACTIVE} to {@code FROZEN}
     * or {@code PASSIVE} will fail with an {@code IllegalStateException}.
     * <p/>
     * If transaction timeouts during state change, then this method will fail with a {@code TransactionException}.
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
    void changeClusterState(ClusterState newState, TransactionOptions transactionOptions);

    /**
     * Changes state of the cluster to the {@link ClusterState#PASSIVE} transactionally,
     * then triggers the shutdown process on each node. Transaction will be {@code TWO_PHASE}
     * and will have 1 durability by default. If you want to override transaction options,
     * use {@link #shutdown(TransactionOptions)}.
     * <p/>
     * If the cluster is already in {@link ClusterState#PASSIVE}, shutdown process begins immediately.
     * All the node join / leave rules described in {@link ClusterState#PASSIVE} state also applies here.
     * <p/>
     * Any node can start the shutdown process. A shutdown command is sent to other nodes periodically until
     * either all other nodes leave the cluster or a configurable timeout occurs. If some of the nodes do not
     * shutdown before the timeout duration, shutdown can be also invoked on them.
     * @see {@link com.hazelcast.instance.GroupProperty#CLUSTER_SHUTDOWN_TIMEOUT_SECONDS}
     *
     * @throws IllegalStateException    if member-list changes during the transaction
     *                                  or there are ongoing/pending migration operations
     * @throws TransactionException     if there's already an ongoing transaction
     *                                  or this transaction fails
     *                                  or this transaction timeouts
     *
     * @see {@link #changeClusterState(ClusterState)}
     * @see {@link ClusterState#PASSIVE}
     * @since 3.6
     */
    void shutdown();

    /**
     * Changes state of the cluster to the {@link ClusterState#PASSIVE} transactionally, then
     * triggers the shutdown process on each node. Transaction must be a {@code TWO_PHASE} transaction.
     * <p/>
     * If the cluster is already in {@link ClusterState#PASSIVE}, shutdown process begins immediately.
     * All the node join / leave rules described in {@link ClusterState#PASSIVE} state also applies here.
     * <p/>
     * Any node can start the shutdown process. A shutdown command is sent to other nodes periodically until
     * either all other nodes leave the cluster or a configurable timeout occurs. If some of the nodes do not
     * shutdown before the timeout duration, shutdown can be also invoked on them.
     * @see {@link com.hazelcast.instance.GroupProperty#CLUSTER_SHUTDOWN_TIMEOUT_SECONDS}
     *
     * @param transactionOptions transaction options
     * @throws IllegalStateException    if member-list changes during the transaction
     *                                  or there are ongoing/pending migration operations
     * @throws TransactionException     if there's already an ongoing transaction
     *                                  or this transaction fails
     *                                  or this transaction timeouts
     *
     * @see {@link #changeClusterState(ClusterState)}
     * @see {@link ClusterState#PASSIVE}
     * @since 3.6
     */
    void shutdown(TransactionOptions transactionOptions);
}
