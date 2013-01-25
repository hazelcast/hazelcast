/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster.client;

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Protocol;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;

public class GetMembersHandler extends ClientCommandHandler {

    public GetMembersHandler(NodeEngine nodeService) {
        super(nodeService);
    }

    public Protocol processCall(Node node, Protocol protocol) {
        Collection<Member> collection = node.nodeEngine.getClusterService().getMembers();
        String[] args = new String[collection.size()];
        int i = 0;
        for (Member member : collection) {
            MemberImpl m = (MemberImpl) member;
            args[i++] = m.getAddress().getHost() + ":" + m.getAddress().getPort();
        }
        return protocol.success(args);
    }
}
