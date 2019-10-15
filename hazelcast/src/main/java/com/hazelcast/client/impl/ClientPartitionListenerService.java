/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddPartitionListenerCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;

public class ClientPartitionListenerService {

    private final Map<ClientEndpoint, Long> partitionListeningEndpoints = new ConcurrentHashMap<ClientEndpoint, Long>();
    private final NodeEngineImpl nodeEngine;
    private final boolean advancedNetworkConfigEnabled;

    ClientPartitionListenerService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.advancedNetworkConfigEnabled = nodeEngine.getConfig().getAdvancedNetworkConfig().isEnabled();
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
        PartitionTableView partitionTableView = nodeEngine.getPartitionService().createPartitionTableView();
        Map<Address, List<Integer>> partitions = getPartitions(partitionTableView);
        int partitionStateVersion = partitionTableView.getVersion();
        return ClientAddPartitionListenerCodec.encodePartitionsEvent(partitions.entrySet(), partitionStateVersion);
    }

    public void registerPartitionListener(ClientEndpoint clientEndpoint, long correlationId) {
        partitionListeningEndpoints.put(clientEndpoint, correlationId);

        ClientMessage clientMessage = getPartitionsMessage();
        clientMessage.setCorrelationId(correlationId);
        clientEndpoint.getConnection().write(clientMessage);
    }

    public void deregisterPartitionListener(ClientEndpoint clientEndpoint) {
        partitionListeningEndpoints.remove(clientEndpoint);
    }

    /**
     * If any partition does not have an owner, this method returns empty collection
     *
     * @param partitionTableView will be converted to address-&gt;partitions mapping
     * @return address-&gt;partitions mapping, where address is the client address of the member
     */
    public Map<Address, List<Integer>> getPartitions(PartitionTableView partitionTableView) {

        Map<Address, List<Integer>> partitionsMap = new HashMap<Address, List<Integer>>();

        int partitionCount = partitionTableView.getLength();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            PartitionReplica owner = partitionTableView.getReplica(partitionId, 0);
            if (owner == null) {
                partitionsMap.clear();
                return partitionsMap;
            }
            Address clientOwnerAddress = clientAddressOf(owner.address());
            if (clientOwnerAddress == null) {
                partitionsMap.clear();
                return partitionsMap;
            }
            List<Integer> indexes = partitionsMap.get(clientOwnerAddress);
            if (indexes == null) {
                indexes = new LinkedList<Integer>();
                partitionsMap.put(clientOwnerAddress, indexes);
            }
            indexes.add(partitionId);
        }
        return partitionsMap;
    }

    private Address clientAddressOf(Address memberAddress) {
        if (!advancedNetworkConfigEnabled) {
            return memberAddress;
        }
        Member member = nodeEngine.getClusterService().getMember(memberAddress);
        if (member != null) {
            return member.getAddressMap().get(CLIENT);
        } else {
            // partition table contains stale entries for members which are not in the member list
            return null;
        }
    }
}
