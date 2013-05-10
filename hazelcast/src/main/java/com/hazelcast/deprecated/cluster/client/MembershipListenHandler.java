/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.deprecated.cluster.client;

import com.hazelcast.deprecated.client.ClientCommandHandler;
import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.Node;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.deprecated.nio.protocol.Command;

public class MembershipListenHandler extends ClientCommandHandler {
    private final ClusterServiceImpl clusterService;

    public MembershipListenHandler(ClusterServiceImpl clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public Protocol processCall(final Node node, Protocol protocol) {
        final Cluster cluster = clusterService.getClusterProxy();
        final TcpIpConnection connection = protocol.conn;
        cluster.addMembershipListener(new MembershipListener() {
            public void memberAdded(MembershipEvent membershipEvent) {
                sendEvent(membershipEvent);
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                sendEvent(membershipEvent);
            }

            void sendEvent(MembershipEvent event) {
                if (connection.live()) {
                    String eventType;
                    if (MembershipEvent.MEMBER_ADDED == event.getEventType()) {
                        eventType = "ADDED";
                    } else eventType = "REMOVED";
                    String hostName = event.getMember().getInetSocketAddress().getHostName();
                    int port = event.getMember().getInetSocketAddress().getPort();
                    Protocol response = new Protocol(connection, Command.EVENT, new String[]{eventType, hostName, String.valueOf(port)});
                    sendResponse(node, response, connection);
                } else
                    cluster.removeMembershipListener(this);
            }
        });
        return null;
    }
}
