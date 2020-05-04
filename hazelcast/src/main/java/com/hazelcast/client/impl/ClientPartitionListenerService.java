/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Member;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.scheduler.CoalescingDelayedTrigger;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;

public class ClientPartitionListenerService {

    private static final long UPDATE_DELAY_MS = 100;
    private static final long UPDATE_MAX_DELAY_MS = 500;

    private final Map<ClientEndpoint, Long> partitionListeningEndpoints = new ConcurrentHashMap<ClientEndpoint, Long>();
    private final NodeEngineImpl nodeEngine;
    private final boolean advancedNetworkConfigEnabled;
    private final CoalescingDelayedTrigger delayedPartitionUpdateTrigger;

    ClientPartitionListenerService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.advancedNetworkConfigEnabled = nodeEngine.getConfig().getAdvancedNetworkConfig().isEnabled();
        this.delayedPartitionUpdateTrigger = new CoalescingDelayedTrigger(nodeEngine.getExecutionService(),
                UPDATE_DELAY_MS, UPDATE_MAX_DELAY_MS, new PushPartitionTableUpdate());
    }

    public void onPartitionStateChange() {
        delayedPartitionUpdateTrigger.executeWithDelay();
    }

    private void pushPartitionStateChange() {
        PartitionTableView partitionTableView = nodeEngine.getPartitionService().createPartitionTableView();
        Collection<Entry<Address, List<Integer>>> partitions = getPartitions(partitionTableView);
        int partitionStateVersion = partitionTableView.getVersion();

        for (Entry<ClientEndpoint, Long> entry : partitionListeningEndpoints.entrySet()) {
            ClientMessage clientMessage = getPartitionsMessage(partitions, partitionStateVersion);
            Long correlationId = entry.getValue();
            clientMessage.setCorrelationId(correlationId);

            ClientEndpoint clientEndpoint = entry.getKey();
            Connection connection = clientEndpoint.getConnection();
            connection.write(clientMessage);
        }
    }

    private ClientMessage getPartitionsMessage(Collection<Entry<Address, List<Integer>>> partitions, int partitionStateVersion) {
        ClientMessage clientMessage = ClientAddPartitionListenerCodec.encodePartitionsEvent(partitions, partitionStateVersion);
        clientMessage.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
        clientMessage.setVersion(ClientMessage.VERSION);
        return clientMessage;
    }

    public void registerPartitionListener(ClientEndpoint clientEndpoint, long correlationId) {
        partitionListeningEndpoints.put(clientEndpoint, correlationId);

        PartitionTableView partitionTableView = nodeEngine.getPartitionService().createPartitionTableView();
        Collection<Map.Entry<Address, List<Integer>>> partitions = getPartitions(partitionTableView);
        int partitionStateVersion = partitionTableView.getVersion();

        ClientMessage clientMessage = getPartitionsMessage(partitions, partitionStateVersion);
        clientMessage.setCorrelationId(correlationId);
        clientEndpoint.getConnection().write(clientMessage);
    }

    public void deregisterPartitionListener(ClientEndpoint clientEndpoint) {
        partitionListeningEndpoints.remove(clientEndpoint);
    }

    /**
     * If any partition does not have an owner, this method returns empty collection
     *
     * @param partitionTableView will be converted to address->partitions mapping
     * @return address->partitions mapping, where address is the client address of the member
     */
    public Collection<Map.Entry<Address, List<Integer>>> getPartitions(PartitionTableView partitionTableView) {

        Map<Address, List<Integer>> partitionsMap = new HashMap<Address, List<Integer>>();

        int partitionCount = partitionTableView.getLength();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            PartitionReplica owner = partitionTableView.getReplica(partitionId, 0);
            if (owner == null) {
                partitionsMap.clear();
                return partitionsMap.entrySet();
            }
            Address clientOwnerAddress = clientAddressOf(owner.address());
            if (clientOwnerAddress == null) {
                partitionsMap.clear();
                return partitionsMap.entrySet();
            }
            List<Integer> indexes = partitionsMap.get(clientOwnerAddress);
            if (indexes == null) {
                indexes = new LinkedList<Integer>();
                partitionsMap.put(clientOwnerAddress, indexes);
            }
            indexes.add(partitionId);
        }
        return partitionsMap.entrySet();
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

    //for test purpose only
    public Map<ClientEndpoint, Long> getPartitionListeningEndpoints() {
        return partitionListeningEndpoints;
    }

    private class PushPartitionTableUpdate implements Runnable {
        @Override
        public void run() {
            pushPartitionStateChange();
        }
    }
}
