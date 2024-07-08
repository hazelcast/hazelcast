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

import com.hazelcast.client.impl.connection.ClientConnection;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a subset of cluster members, used when
 * {@link com.hazelcast.client.impl.connection.tcp.RoutingMode#MULTI_MEMBER}
 * is used as the client routing mode.
 */
public interface SubsetMembers {

    SubsetMembers NOOP = new NoopSubsetMembers();

    /**
     * Called by {@link com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager}
     * to pass latest state of connected server member's group info to client side.
     *
     * @param clusterUuid    uuid of the cluster connected
     * @param authMemberUuid uuid of the authenticator server.
     * @param keyValuePairs  keyValuePairs map returned from connected
     *                       server, required fields must be read from this generic map
     */
    void updateOnAuth(UUID clusterUuid, UUID authMemberUuid, Map<String, String> keyValuePairs);

    /**
     * Called by {@link
     * com.hazelcast.client.impl.spi.impl.listener.ClientClusterViewListenerService.ClusterViewListenerHandler}
     * to pass latest state of whole cluster member groups to client side.
     *
     * @param clusterUuid     uuid of the cluster connected by a client
     * @param allMemberGroups all member-groups as collection of memberUuid-collections
     * @param version         version of server members' list
     */
    void updateOnClusterViewEvent(UUID clusterUuid,
                                  Collection<Collection<UUID>> allMemberGroups,
                                  int version);

    /**
     * Removes disconnected member from subset.
     */
    void onConnectionRemoved(ClientConnection clientConnection);

    /**
     * Updates the current cluster ID used for tracking the {@link SubsetMembersView}.
     */
    void onClusterConnect(UUID oldClusterId, UUID newClusterId);

    /**
     * Retrieves the client's current {@link SubsetMembersView} if one exists.
     * It is possible that one does not exist if the client is not currently
     * connected to a cluster.
     *
     * @return the {@link SubsetMembersView} if it exists, or {@code null} otherwise.
     */
    @Nullable
    SubsetMembersView getSubsetMembersView();
}
