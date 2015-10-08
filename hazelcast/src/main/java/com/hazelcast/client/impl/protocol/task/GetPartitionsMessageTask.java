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
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;

import java.security.Permission;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GetPartitionsMessageTask extends AbstractCallableMessageTask<ClientGetPartitionsCodec.RequestParameters> {

    public GetPartitionsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        InternalPartitionService service = getService(InternalPartitionService.SERVICE_NAME);
        service.firstArrangement();

        InternalPartition[] partitions = service.getPartitions();
        int[] indexes = new int[partitions.length];
        Map<Address, Set<Integer>> partitionsMap = new HashMap<Address, Set<Integer>>();

        for (InternalPartition partition : partitions) {
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                return ClientGetPartitionsCodec.encodeResponse(new Address[0], new int[0]);
            }
            Set<Integer> ownedPartitions = partitionsMap.get(owner);
            if (ownedPartitions == null) {
                ownedPartitions = new HashSet<Integer>();
                partitionsMap.put(owner, ownedPartitions);
            }
            ownedPartitions.add(partition.getPartitionId());
        }

        Address[] addresses = new Address[partitionsMap.size()];

        int k = 0;
        for (Address owner : partitionsMap.keySet()) {
            addresses[k++] = owner;
        }

        for (k = 0; k < addresses.length; k++) {
            Set<Integer> ownedPartitions = partitionsMap.get(addresses[k]);
            for (int partitionId : ownedPartitions) {
                indexes[partitionId] = k;
            }
        }

        return ClientGetPartitionsCodec.encodeResponse(addresses, indexes);
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
