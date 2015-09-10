/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;

import java.security.Permission;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GetPartitionsMessageTask
        extends AbstractCallableMessageTask<ClientGetPartitionsCodec.RequestParameters> {

    public GetPartitionsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected Object call() {
        InternalPartitionService service = getService(InternalPartitionService.SERVICE_NAME);
        service.firstArrangement();
        ClusterService clusterService = getService(ClusterServiceImpl.SERVICE_NAME);
        Collection<Member> memberList = clusterService.getMembers();

        Map<Address, Set<Integer>> partitionsMap = new HashMap<Address, Set<Integer>>(memberList.size());
        for (Member member : memberList) {
            Address address = member.getAddress();
            partitionsMap.put(address, new HashSet<Integer>());
        }

        for (InternalPartition partition : service.getPartitions()) {
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                partitionsMap.clear();
                return ClientGetPartitionsCodec.encodeResponse(partitionsMap);
            }
            Set<Integer> indexes = partitionsMap.get(owner);
            indexes.add(partition.getPartitionId());
        }
        return ClientGetPartitionsCodec.encodeResponse(partitionsMap);
    }

    @Override
    protected ClientGetPartitionsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientGetPartitionsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return (ClientMessage) response;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

}
