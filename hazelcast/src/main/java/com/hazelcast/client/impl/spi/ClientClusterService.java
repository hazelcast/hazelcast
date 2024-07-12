/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi;

import com.hazelcast.client.config.ClusterRoutingConfig;
import com.hazelcast.client.impl.clientside.SubsetMembers;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.version.Version;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.EventListener;
import java.util.Map;
import java.util.UUID;

/**
 * Cluster service for Hazelcast clients.
 * <p>
 * Allows retrieving Hazelcast members of the cluster, e.g. by their {@link Address} or UUID.
 */
public interface ClientClusterService {

    /**
     * Gets the member with the given UUID.
     *
     * @param uuid The UUID of the member.
     * @return The member that was found, or null if not found. If UUID is null, null is returned.
     */
    Member getMember(@Nonnull UUID uuid);

    /**
     * Gets the collection of members.
     *
     * @return The collection of members. Null will never be returned.
     */
    Collection<Member> getMemberList();

    /**
     * <p>
     * Gets the collection of members, or an empty list if the client
     * changed the cluster and the new member list is not received yet.
     * </p>
     * <p>
     * When {@link ClusterRoutingConfig} is enabled, this method
     * returns list of members seen by {@link SubsetMembers}
     * </p>
     * @return The collection of members.
     */
    @Nonnull
    Collection<Member> getEffectiveMemberList();

    /**
     * Returns a collection of the members that satisfy the given {@link MemberSelector}.
     *
     * @param selector {@link MemberSelector} instance to filter members to return
     * @return members that satisfy the given {@link MemberSelector}.
     */
    Collection<Member> getMembers(@Nonnull MemberSelector selector);

    /**
     * Returns the address of the master member.
     *
     * @return The address of the master member. Could be null if the master is not yet known.
     */
    Member getMasterMember();

    /**
     * @param listener The listener to be registered.
     * @return The registration ID
     */
    @Nonnull
    UUID addMembershipListener(@Nonnull MembershipListener listener);

    /**
     * @param registrationId The registrationId of the listener to be removed.
     * @return true if successfully removed, false otherwise.
     */
    boolean removeMembershipListener(@Nonnull UUID registrationId);

    /**
     * Returns this client's view of member group subsets, used when the
     * {@link com.hazelcast.client.impl.connection.tcp.RoutingMode#MULTI_MEMBER}
     * routing mode is set on the client.
     *
     * @return the {@link SubsetMembers} view.
     */
    SubsetMembers getSubsetMembers();

    /**
     * @return cluster's uuid
     */
    UUID getClusterId();

    /**
     * Returns the cluster version, which may be {@link Version#UNKNOWN}.
     */
    @Nonnull
    Version getClusterVersion();

    void onClusterConnect(UUID newClusterId);

    void updateOnAuth(UUID clusterUuid, UUID authMemberUuid, Map<String, String> keyValuePairs);

    /**
     * Updates the members of the cluster with the latest list.
     * @param memberListVersion The version of the member list
     * @param memberInfos The list of members
     * @param clusterUuid The UUID of the cluster
     */
    void handleMembersViewEvent(int memberListVersion, Collection<MemberInfo> memberInfos, UUID clusterUuid);

    /**
     * Updates the cluster version with the latest one.
     */
    void handleClusterVersionEvent(Version version);

    /**
     * Starts the configured MembershipListeners.
     *
     * @param configuredListeners
     */
    void start(Collection<EventListener> configuredListeners);

    /**
     * Gets the cluster proxy.
     *
     * @return the cluster proxy.
     */
    Cluster getCluster();

    /**
     * Resets the cluster snapshot back to the initial state.
     */
    void onTryToConnectNextCluster();

    /**
     * Resets the membership version to zero.
     */
    void onClusterConnect();

    /**
     * Gets the membership version.
     *
     * @return the membership version.
     */
    int getMemberListVersion();

    /**
     * Terminates the {@link com.hazelcast.client.util.ClientConnectivityLogger} task
     * which may be in progress.
     */
    void terminateClientConnectivityLogging();
}
