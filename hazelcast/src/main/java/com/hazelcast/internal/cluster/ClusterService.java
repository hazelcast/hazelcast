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
     * @param uuid the UUID of the member
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
}
