/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.CoreService;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.version.Version;

import java.util.Collection;

/**
 * A service responsible for member related functionality. So members joining, leaving etc.
 * <p/>
 * This API is an internal API; the end user will use the {@link com.hazelcast.core.Cluster} interface.
 */
public interface ClusterService extends CoreService, Cluster {

    /**
     * Gets the member for the given address.
     *
     * @param address the address of the member to lookup.
     * @return the found member, or null if not found. If address is null, null is returned.
     */
    MemberImpl getMember(Address address);

    /**
     * Gets the member with the given uuid.
     *
     * @param uuid the uuid of the member
     * @return the found member, or null if not found. If uuid is null, null is returned.
     */
    MemberImpl getMember(String uuid);

    /**
     * Gets the collection of members.
     * <p/>
     * if we take care of the generics.
     *
     * @return the collection of member. Null will never be returned.
     */
    Collection<MemberImpl> getMemberImpls();

    /**
     * Returns a collection of the members that satisfy the given {@link com.hazelcast.core.MemberSelector}.
     *
     * @param selector {@link com.hazelcast.core.MemberSelector} instance to filter members to return
     * @return members that satisfy the given {@link com.hazelcast.core.MemberSelector}.
     */
    Collection<Member> getMembers(MemberSelector selector);

    /**
     * Returns the address of the master member.
     *
     * @return the address of the master member. Could be null if the master is not yet known.
     */
    Address getMasterAddress();

    /**
     * Checks if this member is the master.
     *
     * @return true if master, false otherwise.
     */
    boolean isMaster();

    /**
     * Gets the address of this member.
     *
     * @return the address of this member. The returned value will never be null.
     */
    Address getThisAddress();

    /**
     * Gets the local member instance.
     *
     * @return the local member instance. The returned value will never be null.
     */
    Member getLocalMember();

    /**
     * Gets the current number of members.
     *
     * @return the current number of members.
     */
    int getSize();

    /**
     * Gets the number of members that satisfy the given {@link com.hazelcast.core.MemberSelector} instance.
     * @param selector {@link com.hazelcast.core.MemberSelector} instance that filters members to be counted.
     * @return the number of members that satisfy the given {@link com.hazelcast.core.MemberSelector} instance.
     */
    int getSize(MemberSelector selector);

    /**
     * Returns the {@link ClusterClock} of the cluster.
     *
     * The returned value will never be null and will never change.
     *
     * @return the ClusterClock.
     */
    ClusterClock getClusterClock();

    /**
     * Returns UUID for the cluster.
     *
     * @return unique Id for cluster
     */
    String getClusterId();

    /**
     * Changes the cluster version transactionally. Internally this method uses the same transaction infrastructure as
     * {@link #changeClusterState(ClusterState)} and the transaction defaults are the same in this case as well
     * ({@code TWO_PHASE} transaction with durability 1 by default).
     * <p/>
     * If the requested cluster version is same as the current one, nothing happens.
     * <p/>
     * If a node of the cluster is not compatible with the given cluster {@code version}, as implemented by
     * {@link com.hazelcast.instance.NodeExtension#isNodeVersionCompatibleWith(Version)}, then a
     * {@link com.hazelcast.internal.cluster.impl.VersionMismatchException} is thrown.
     * <p/>
     * If an invalid version transition is requested, for example changing to a different major version, an
     * {@link IllegalArgumentException} is thrown.
     * <p/>
     * If a membership change occurs in the cluster during locking phase, a new member joins or
     * an existing member leaves, then this method will fail with an {@code IllegalStateException}.
     * <p/>
     * Likewise, once locking phase is completed successfully, {@link Cluster#getClusterState()}
     * will report being {@link ClusterState#IN_TRANSITION}, disallowing membership changes until the new cluster version is
     * committed.
     *
     * @param version new version of the cluster
     * @since 3.8
     */
    void changeClusterVersion(Version version);

    /**
     * Changes the cluster version transactionally, with the transaction options provided. Internally this method uses the same
     * transaction infrastructure as {@link #changeClusterState(ClusterState, TransactionOptions)}. The transaction
     * options must specify a {@code TWO_PHASE} transaction.
     * <p/>
     * If the requested cluster version is same as the current one, nothing happens.
     * <p/>
     * If a node of the cluster is not compatible with the given cluster {@code version}, as implemented by
     * {@link com.hazelcast.instance.NodeExtension#isNodeVersionCompatibleWith(Version)}, then a
     * {@link com.hazelcast.internal.cluster.impl.VersionMismatchException} is thrown.
     * <p/>
     * If an invalid version transition is requested, for example changing to a different major version, an
     * {@link IllegalArgumentException} is thrown.
     * <p/>
     * If a membership change occurs in the cluster during locking phase, a new member joins or
     * an existing member leaves, then this method will fail with an {@code IllegalStateException}.
     * <p/>
     * Likewise, once locking phase is completed successfully, {@link Cluster#getClusterState()}
     * will report being {@link ClusterState#IN_TRANSITION}, disallowing membership changes until the new cluster version is
     * committed.
     *
     * @param version new version of the cluster
     * @param options options by which to execute the transaction
     * @since 3.8
     */
    void changeClusterVersion(Version version, TransactionOptions options);
}
