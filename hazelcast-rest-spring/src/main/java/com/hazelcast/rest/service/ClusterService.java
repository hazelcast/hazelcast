/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.rest.service;

import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.rest.model.ClusterStatusModel;
import com.hazelcast.rest.model.MemberDetailModel;
import com.hazelcast.rest.util.NodeEngineImplHolder;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;

@Service
public class ClusterService {
    private final NodeEngineImplHolder nodeEngineImplHolder;

    public ClusterService(NodeEngineImplHolder nodeEngineImplHolder) {
        this.nodeEngineImplHolder = nodeEngineImplHolder;
    }

    public ClusterStatusModel getClusterStatus() {
        List<MemberDetailModel> members = new ArrayList<>();
        nodeEngineImplHolder.getNodeEngine()
                .getHazelcastInstance()
                .getCluster().getMembers().stream().map(m -> new MemberDetailModel.MemberDetailModelBuilder()
                        .address(m.getAddress().toString())
                        .liteMember(m.isLiteMember())
                        .localMember(m.localMember())
                        .uuid(m.getUuid().toString())
                        .build())
                .forEach(members::add);
        Server server = nodeEngineImplHolder.getNodeEngine().getNode().getServer();
        ServerConnectionManager cm = server.getConnectionManager(CLIENT);
        int clientCount = cm == null ? 0 : cm.connectionCount(ServerConnection::isClient);
        return new ClusterStatusModel.ClusterStatusModelBuilder()
                .members(members)
                .connectionCount(clientCount)
                .allConnectionCount(server.connectionCount())
                .build();
    }
}
