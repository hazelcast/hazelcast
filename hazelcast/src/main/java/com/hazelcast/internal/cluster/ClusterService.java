/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.CoreService;

import java.util.Collection;

/**
 * A service responsible for member related functionality, e.g. members joining, leaving etc.
 * <p>
 * This API is an internal API; the end user will use the {@link com.hazelcast.core.Cluster} interface.
 */
public interface ClusterService extends CoreService, Cluster {

    /**
     * Gets the member for the given address.
     *
     * @param address the address of the member to lookup
     * @return the found member, or {@code null} if not found (if the address is {@code null}, {@code null} is returned)
     */
    MemberImpl getMember(Address address);

    /**
     * Gets the member with the given UUID.
     *
     * @param UUID the UUID of the member
     * @return the found member, or {@code null} if not found (if the UUID is {@code null}, {@code null} is returned)
     */
    MemberImpl getMember(String uuid);

    /**
     * Gets the collection of members.
     * <p>
     * If we take care of the generics.
     *
     * @return the collection of member (the returned value will never be {@code null})
     */
    Collection<MemberImpl> getMemberImpls();

    /**
     * Returns a collection of the members that satisfy the given {@link com.hazelcast.core.MemberSelector}.
     *
     * @param selector {@link com.hazelcast.core.MemberSelector} instance to filter members to return
     * @return members that satisfy the given {@link com.hazelcast.core.MemberSelector}
     */
    Collection<Member> getMembers(MemberSelector selector);

    /**
     * Returns the address of the master member.
     *
     * @return the address of the master member (can be {@code null} if the master is not yet known)
     */
    Address getMasterAddress();

    /**
     * Checks if this member is the master.
     *
     * @return {@code true} if master, {@code false} otherwise
     */
    boolean isMaster();

    /**
     * Returns whether this member joined to a cluster.
     *
     * @return {@code true} if this member is joined to a cluster, {@code false} otherwise
     */
    boolean isJoined();

    /**
     * Gets the address of this member.
     *
     * @return the address of this member (the returned value will never be {@code null})
     */
    Address getThisAddress();

    /**
     * Gets the local member instance.
     * <p>
     * The returned value will never be null, but it may change when local lite member is promoted to a data member
     * via {@link #promoteLocalLiteMember()}
     * or when this member merges to a new cluster after split-brain detected. Returned value should not be
     * cached but instead this method should be called each time when local member is needed.
     *
     * @return the local member instance (the returned value will never be {@code null})
     */
    Member getLocalMember();

    /**
     * Gets the current number of members.
     *
     * @return the current number of members
     */
    int getSize();

    /**
     * Gets the number of members that satisfy the given {@link com.hazelcast.core.MemberSelector} instance.
     *
     * @param selector {@link com.hazelcast.core.MemberSelector} instance that filters members to be counted
     * @return the number of members that satisfy the given {@link com.hazelcast.core.MemberSelector} instance
     */
    int getSize(MemberSelector selector);

    /**
     * Returns the {@link ClusterClock} of the cluster.
     * <p>
     * The returned value will never be {@code null} and will never change.
     *
     * @return the ClusterClock
     */
    ClusterClock getClusterClock();

    /**
     * Returns UUID for the cluster.
     *
     * @return unique UUID for cluster
     */
    String getClusterId();

    /**
     * Returns the current version of member list.
     *
     * @return current version of member list
     */
    int getMemberListVersion();

    /**
     * Returns the member list join version of the local member instance.
     * <p>
     * The join algorithm is specifically designed to ensure that member list join version is unique for each
     * member in the cluster, even during a network-split situation:<ul>
     *     <li>If two members join at the same time, they will appear on different version of member list
     *     <li>If a new member claims mastership, it makes a jump in the member list version based on its
     *     index in the member list multiplied by the value of the
     *     {@link com.hazelcast.spi.properties.GroupProperty#MASTERSHIP_CLAIM_MEMBER_LIST_VERSION_INCREMENT}
     *     configuration property. This is to protect against the possibility that the original master is still
     *     running in a separate network partition.
     * </ul>
     * The solution provides uniqueness guarantee of member list join version numbers with the following
     * limitations:<ul>
     *    <li>When there is a split-brain issue, the number of member list changes that can occur in the
     *    sub-clusters are capped by the abovementioned configuration parameter.
     *    <li>When there is a split-brain issue, if further splits occur in the already split sub-clusters, the
     *    uniqueness guarantee can be lost.
     * </ul>
     * The value returned from this method can be cached. Even though it can change later, both values are
     * unique.
     *
     * @throws IllegalStateException if the local instance is not joined or the cluster just upgraded to 3.10,
     *      but local member has not yet learned its join version from the master node.
     * @throws UnsupportedOperationException if the cluster version is below 3.10
     *
     * @return the member list join version of the local member instance
     */
    int getMemberListJoinVersion();
}
