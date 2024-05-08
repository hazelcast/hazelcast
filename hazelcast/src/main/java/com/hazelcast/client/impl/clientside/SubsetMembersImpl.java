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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.config.SubsetRoutingConfig;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.SUBSET_MEMBER_GROUPS_INFO;
import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.checkRequiredFieldsForSubsetRoutingExist;
import static com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator.parseJson;

/**
 * <p>
 * Holds and manages a subset of members seen by a client. The client will only
 * ever connect to 1 cluster at any time, but to maintain a thread-safe view of
 * the client's subset, this implementation uses a {@link ConcurrentMap} to store
 * the current view. This allows read, modify, write semantics with minimal cons.
 * This area of code is not in the hot path, so the performance overhead of a
 * single-entry {@link ConcurrentMap} is not of concern.
 * </p>
 * <p>
 * Subset members are sent by the cluster in the form of member-groups
 * based on {@link com.hazelcast.config.PartitionGroupConfig} when using
 * the {@link com.hazelcast.client.config.RoutingStrategy#PARTITION_GROUPS}.
 * </p>
 */
public class SubsetMembersImpl implements SubsetMembers {

    private final ConcurrentMap<UUID, SubsetMembersView> subsetMembersViewByClusterUuid
            = new ConcurrentHashMap<>();
    private final SubsetRoutingConfig subsetRoutingConfig;
    private final ILogger logger;

    private volatile UUID clusterId;

    public SubsetMembersImpl(SubsetRoutingConfig subsetRoutingConfig, ILogger logger) {
        this.subsetRoutingConfig = subsetRoutingConfig;
        this.logger = logger;
    }

    @Override
    public void updateOnAuth(UUID clusterUuid, UUID authMemberUuid, Map<String, String> keyValuePairs) {
        if (!checkRequiredFieldsForSubsetRoutingExist(subsetRoutingConfig, keyValuePairs)) {
            logAsFinest("SubsetRouting is not in use");
            return;
        }

        String memberGroupsJson = keyValuePairs.get(SUBSET_MEMBER_GROUPS_INFO);
        KeyValuePairGenerator.MemberGroupsAndVersionHolder memberGroupsAndVersionHolder = parseJson(memberGroupsJson);

        Collection<Collection<UUID>> memberGroups = memberGroupsAndVersionHolder.allMemberGroups();
        int version = memberGroupsAndVersionHolder.version();

        updateInternal(memberGroups, version, clusterUuid, authMemberUuid);
        logAsFinest("On authentication [clusterUuid=%s, version=%d, memberGroups=%s, authMemberUuid=%s]",
                clusterUuid, version, memberGroups, authMemberUuid);
    }

    @Override
    public void updateOnClusterViewEvent(UUID clusterUuid,
                                         Collection<Collection<UUID>> allMemberGroups,
                                         int version) {
        updateInternal(allMemberGroups, version, clusterUuid, null);
        logAsFinest("On cluster event [clusterUuid=%s, version=%d, memberGroups=%s]",
                clusterUuid, version, allMemberGroups);
    }

    @Override
    public void onConnectionRemoved(ClientConnection clientConnection) {
        UUID clusterUuid = clientConnection.getClusterUuid();
        UUID remoteUuid = clientConnection.getRemoteUuid();
        if (remoteUuid == null) {
            return;
        }

        subsetMembersViewByClusterUuid.computeIfPresent(clusterUuid, (uuid, current) -> {
            Set<UUID> members = current.members();
            members.remove(remoteUuid);
            // when no remaining member in group remove clusterUuid
            logAsFinest("onConnectionRemoved [clusterUuid=%s, hasMemberInGroup=%s]",
                    clusterUuid, String.valueOf(members.isEmpty()));
            return members.isEmpty() ? null : current;
        });

        logAsFinest("Removed connection [clusterUuid=%s, memberUuid=%s]", clusterUuid, remoteUuid);
    }

    @Override
    public void onClusterConnect(UUID oldClusterId, UUID newClusterId) {
        if (oldClusterId != null && !oldClusterId.equals(newClusterId)) {
            subsetMembersViewByClusterUuid.remove(oldClusterId);
        }
        clusterId = newClusterId;
        logAsFinest("onClusterConnect to clusterUuid=%s", clusterId);
    }

    @Override
    @Nullable
    public SubsetMembersView getSubsetMembersView() {
        return subsetMembersViewByClusterUuid.get(clusterId);
    }

    /**
     * Central method to update a client's subset members.
     *
     * @param memberGroups all member groups from the cluster
     * @param version      version of member list in the cluster
     * @param clusterUuid  uuid of the connected cluster
     * @param memberUuid   uuid of the authenticator member, its value
     *                     is null when memberGroups info is received via cluster listener
     */
    private void updateInternal(final Collection<Collection<UUID>> memberGroups,
                                final int version,
                                final @Nonnull UUID clusterUuid,
                                final @Nullable UUID memberUuid) {
        if (!hasDataMember(memberGroups)) {
            // this means all members are lite
            logAsFinest("All members are lite [clusterUuid=%s, memberGroups=%s]",
                    clusterUuid, memberGroups);
            return;
        }

        assert version > 0;

        subsetMembersViewByClusterUuid.compute(clusterUuid,
                (uuid, current) -> pickSubset(memberGroups, version, clusterUuid, memberUuid, current));
    }

    /**
     * Picks which subset of the cluster members this client will be connected.
     *
     * @return picked members subset view
     */
    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    private static SubsetMembersView pickSubset(Collection<Collection<UUID>> memberGroups,
                                                int version,
                                                UUID clusterUuid,
                                                @Nullable UUID memberUuid,
                                                @Nullable SubsetMembersView current) {

        // 0. Decide when current view should be deemed as null
        if (current == null
                || current.members().isEmpty()
                || !current.clusterUuid().equals(clusterUuid)) {
            current = null;
        }

        // 1. Try to pick authenticator-member's group
        if (memberUuid != null && current == null) {
            for (Collection<UUID> memberGroup : memberGroups) {
                if (memberGroup.contains(memberUuid)) {
                    return new SubsetMembersView(clusterUuid, new HashSet<>(memberGroup), version);
                }
            }
        }

        Collection<UUID> pickedMemberGroup = null;

        // 2. Compare member-list-versions and try to pick the
        // group with more shared members with current one
        if (current != null) {
            if (current.version() <= version) {
                int pickedGroupsSharedMemberCountWithExistingSubset = 0;
                for (Collection<UUID> examinedMemberGroup : memberGroups) {
                    int sharedMemberCountWithExistingSubset = 0;
                    for (UUID member : current.members()) {
                        if (examinedMemberGroup.contains(member)) {
                            sharedMemberCountWithExistingSubset++;
                        }
                    }

                    if (pickedGroupsSharedMemberCountWithExistingSubset < sharedMemberCountWithExistingSubset) {
                        pickedGroupsSharedMemberCountWithExistingSubset = sharedMemberCountWithExistingSubset;
                        pickedMemberGroup = examinedMemberGroup;
                    }
                }

                if (pickedMemberGroup != null) {
                    return new SubsetMembersView(clusterUuid, new HashSet<>(pickedMemberGroup), version);
                }
            } else {
                // we have received a stale member-list-version
                // because `current.version() > version`, let's
                // stick with current members view we have
                return current;
            }
        }

        // 3. Try to pick the member-group which includes more member
        for (Collection<UUID> examinedMemberGroup : memberGroups) {
            if (pickedMemberGroup == null) {
                pickedMemberGroup = examinedMemberGroup;
                continue;
            }

            if (pickedMemberGroup.size() < examinedMemberGroup.size()) {
                pickedMemberGroup = examinedMemberGroup;
            }
        }

        assert pickedMemberGroup != null;
        return new SubsetMembersView(clusterUuid, new HashSet<>(pickedMemberGroup), version);
    }

    private static boolean hasDataMember(Collection<Collection<UUID>> memberGroups) {
        for (Collection<UUID> memberGroup : memberGroups) {
            if (!memberGroup.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private void logAsFinest(String s, Object... params) {
        if (!logger.isFinestEnabled()) {
            return;
        }

        logger.finest(String.format(s, params));
    }

    // only used for testing purposes
    public int memberCountInSubset() {
        UUID clusterUuid = this.clusterId;
        if (clusterUuid == null) {
            return 0;
        }
        SubsetMembersView subsetMembersView = subsetMembersViewByClusterUuid.get(clusterUuid);
        if (subsetMembersView == null) {
            return 0;
        }
        return subsetMembersView.members().size();
    }

    @Override
    public String toString() {
        return "ClientMemberGroupsView{"
                + "memberGroupsViewByClusterUuid=" + subsetMembersViewByClusterUuid
                + ", clusterId=" + clusterId
                + '}';
    }
}
