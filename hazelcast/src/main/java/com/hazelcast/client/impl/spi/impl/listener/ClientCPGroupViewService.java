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

package com.hazelcast.client.impl.spi.impl.listener;

import com.hazelcast.client.impl.CPGroupViewListenerService;
import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.internal.nio.ConnectionListener;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;

/**
 * Client-side component of {@link CPGroupViewListenerService}.
 * <p>
 * Adds a CP group listener to one of the connections. If that connection is removed,
 * it registers a new listener to any other connection.
 * <p>
 * Also maintains a view of CP groups based on received events to be referenced elsewhere.
 */
public interface ClientCPGroupViewService extends ConnectionListener {

    /**
     * Internal method called to initialize the service.
     */
    void start();

    /**
     * Retrieves the {@link UUID} of the last known Leader of the provided {@link CPGroupId}
     * @param groupId the {@link CPGroupId} to identify the last known leader of
     * @return the {@link UUID} of the last known leader for this group if it exists, or {@code null} otherwise
     */
    @Nullable
    UUID getLastKnownLeader(CPGroupId groupId);

    /**
     * Updates the internal mapping to set the provided {@link UUID} as
     * the leader of the provided {@link CPGroupId}. Primarily used
     * for testing purposes.
     *
     * @param groupId    the {@link CPGroupId} to update the leader for
     * @param leaderUuid the {@link UUID} of the leader for this group
     */
    void setLastKnownLeader(CPGroupId groupId, UUID leaderUuid);

    /**
     * Retrieves a {@link Map} of {@link CPGroupId}s to the last known leader {@link UUID}
     * for that group.
     *
     * @return the {@link Map} of all known CP group leaders
     */
    Map<CPGroupId, UUID> getAllKnownLeaders();

    /**
     * Initializes the known CP group leaders map with information received within the
     * {@link com.hazelcast.client.impl.connection.tcp.AuthenticationResponse} after
     * connecting to a new member of the cluster.
     *
     * @param providerUuid              The UUID of the member providing this data
     * @param providerAddress           The Address of the member providing this data
     * @param authResponseKeyValuePairs The full key-value bag passed to the client
     */
    void initializeKnownLeaders(UUID providerUuid, Address providerAddress,
                                Map<String, String> authResponseKeyValuePairs);

    /**
     * Convenience method to easily check if the client is configured for CP
     * direct-to-leader operation routing.
     *
     * @return {@code true} if direct-to-leader routing is enabled, else {@code false}
     */
    boolean isDirectToLeaderEnabled();
}
