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

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.internal.nio.Connection;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * No-op implementation of {@link ClientCPGroupViewService} provided when
 * CP direct-to-leader routing is disabled.
 */
public class NoOpClientCPGroupViewService implements ClientCPGroupViewService {

    @Override
    public void start() {

    }

    @Nullable
    @Override
    public UUID getLastKnownLeader(CPGroupId groupId) {
        return null;
    }

    @Override
    public void setLastKnownLeader(CPGroupId groupId, UUID leaderUuid) {

    }

    @Override
    public Map<CPGroupId, UUID> getAllKnownLeaders() {
        return Collections.emptyMap();
    }

    @Override
    public void initializeKnownLeaders(UUID providerUuid, Address providerAddress,
                                       Map<String, String> authResponseKeyValuePairs) {

    }

    @Override
    public void connectionAdded(Connection connection) {

    }

    @Override
    public void connectionRemoved(Connection connection) {

    }

    @Override
    public boolean isDirectToLeaderEnabled() {
        // This implementation denotes CP direct-to-leader routing is disabled
        return false;
    }
}
