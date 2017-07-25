/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddPartitionListenerCodec;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientPartitionListenerService {

    private final Map<ClientEndpoint, Long> partitionListeningEndpoints = new ConcurrentHashMap<ClientEndpoint, Long>();
    private final NodeEngineImpl nodeEngine;

    ClientPartitionListenerService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void onPartitionStateChange() {

        for (Map.Entry<ClientEndpoint, Long> entry : partitionListeningEndpoints.entrySet()) {
            ClientMessage clientMessage = getPartitionsMessage();
            Long correlationId = entry.getValue();
            clientMessage.setCorrelationId(correlationId);

            ClientEndpoint clientEndpoint = entry.getKey();
            Connection connection = clientEndpoint.getConnection();
            connection.write(clientMessage);
        }
    }

    private ClientMessage getPartitionsMessage() {
        Collection<Map.Entry<Address, List<Integer>>> partitions = getPartitions();
        ClientMessage clientMessage = ClientAddPartitionListenerCodec.encodePartitionsEvent(partitions);
        clientMessage.setCorrelationId(clientMessage.getCorrelationId());
        clientMessage.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
        clientMessage.setVersion(ClientMessage.VERSION);
        return clientMessage;
    }

    public void registerPartitionListener(ClientEndpoint clientEndpoint, long correlationId) {
        partitionListeningEndpoints.put(clientEndpoint, correlationId);

        ClientMessage clientMessage = getPartitionsMessage();
        clientEndpoint.getConnection().write(ClientMessage.createForEncode(clientMessage.buffer(), 0));
    }

    public void deregisterPartitionListener(ClientEndpoint clientEndpoint) {
        partitionListeningEndpoints.remove(clientEndpoint);
    }

    public Collection<Map.Entry<Address, List<Integer>>> getPartitions() {

        Map<Address, List<Integer>> partitionsMap = new HashMap<Address, List<Integer>>();

        for (IPartition partition : nodeEngine.getPartitionService().getPartitions()) {
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                partitionsMap.clear();
                return partitionsMap.entrySet();
            }
            List<Integer> indexes = partitionsMap.get(owner);
            if (indexes == null) {
                indexes = new LinkedList<Integer>();
                partitionsMap.put(owner, indexes);
            }
            indexes.add(partition.getPartitionId());
        }
        return partitionsMap.entrySet();
    }
}
