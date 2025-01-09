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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.config.ClusterRoutingConfig;
import com.hazelcast.client.impl.connection.ClientConnection;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Used instead of null when {@link ClusterRoutingConfig#getRoutingMode()} is not
 * set to {@link com.hazelcast.client.config.RoutingMode#MULTI_MEMBER}.
 */
class NoopSubsetMembers implements SubsetMembers {

    @Override
    public void updateOnAuth(UUID clusterUuid, UUID authMemberUuid, Map<String, String> keyValuePairs) {

    }

    @Override
    public void updateOnClusterViewEvent(UUID clusterUuid, Collection<Collection<UUID>> allMemberGroups, int version) {

    }

    @Override
    public void onConnectionRemoved(ClientConnection clientConnection) {

    }

    @Override
    public void onClusterConnect(UUID oldClusterId, UUID newClusterId) {

    }

    @Override
    public SubsetMembersView getSubsetMembersView() {
        return null;
    }
}
