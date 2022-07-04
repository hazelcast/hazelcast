/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.client.Client;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Default node service provider implementation.
 */
public class NodeServiceProviderImpl implements NodeServiceProvider {

    private final NodeEngineImpl nodeEngine;

    public NodeServiceProviderImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public UUID getLocalMemberId() {
        return nodeEngine.getClusterService().getLocalMember().getUuid();
    }

    @Override
    public Collection<UUID> getDataMemberIds() {
        Set<UUID> res = new HashSet<>();

        for (Member member : nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR)) {
            res.add(member.getUuid());
        }

        return res;
    }

    @Override
    public Set<UUID> getClientIds() {
        Set<UUID> res = new HashSet<>();

        for (Client client : nodeEngine.getHazelcastInstance().getClientService().getConnectedClients()) {
            res.add(client.getUuid());
        }

        return res;
    }

    @Override
    public Connection getConnection(UUID memberId) {
        MemberImpl member = nodeEngine.getClusterService().getMember(memberId);

        if (member == null) {
            return null;
        }

        return nodeEngine.getNode()
                .getServer()
                .getConnectionManager(EndpointQualifier.MEMBER)
                .getOrConnect(member.getAddress());
    }

    @Override
    public MapContainer getMap(String name) {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);

        return mapService.getMapServiceContext().getMapContainers().get(name);
    }

    @Override
    public ILogger getLogger(Class<?> clazz) {
        return nodeEngine.getLogger(clazz);
    }
}
