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
import com.hazelcast.client.impl.protocol.codec.ClientAddClusterViewListenerCodec;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.cluster.impl.MembershipManager;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.OwnedPartitionsPerMemberInterceptor;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import javax.annotation.Nullable;
import java.nio.channels.CancelledKeyException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.client.impl.protocol.codec.ClientAddClusterViewListenerCodec.encodePartitionsViewEvent;
import static com.hazelcast.instance.EndpointQualifier.CLIENT;

public class ClusterViewListenerService {

    private static final int PUSH_PERIOD_IN_SECONDS = 30;

    private final boolean advancedNetworkConfigEnabled;
    private final NodeEngine nodeEngine;
    private final AtomicBoolean pushScheduled = new AtomicBoolean();
    private final Map<ClientEndpoint, Long> clusterListeningEndpoints = new ConcurrentHashMap<>();

    ClusterViewListenerService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.advancedNetworkConfigEnabled = nodeEngine.getConfig().getAdvancedNetworkConfig().isEnabled();
    }

    private void schedulePeriodicPush() {
        ExecutionService executor = nodeEngine.getExecutionService();
        executor.scheduleWithRepetition(this::pushView, PUSH_PERIOD_IN_SECONDS,
                PUSH_PERIOD_IN_SECONDS, TimeUnit.SECONDS);
    }

    private void pushView() {
        if (noListeningClient()) {
            return;
        }

        ClientMessage partitionViewMessage = createPartitionViewClientMessage();
        if (partitionViewMessage != null) {
            sendToListeningEndpoints(partitionViewMessage);
        }
        sendToListeningEndpoints(createMemberListViewClientMessage());
    }

    private boolean noListeningClient() {
        return clusterListeningEndpoints.isEmpty();
    }

    public void onPartitionStateChange() {
        if (noListeningClient()) {
            return;
        }

        ClientMessage partitionViewMessage = createPartitionViewClientMessage();
        if (partitionViewMessage != null) {
            sendToListeningEndpoints(partitionViewMessage);
        }
    }

    public void onMemberListChange() {
        if (noListeningClient()) {
            return;
        }

        sendToListeningEndpoints(createMemberListViewClientMessage());
    }

    private void sendToListeningEndpoints(ClientMessage clientMessage) {
        for (Map.Entry<ClientEndpoint, Long> entry : clusterListeningEndpoints.entrySet()) {
            Long correlationId = entry.getValue();
            //share the partition and membership table, copy only initial frame
            ClientMessage message = clientMessage.copyWithNewCorrelationId(correlationId);
            ClientEndpoint clientEndpoint = entry.getKey();
            Connection connection = clientEndpoint.getConnection();
            write(message, connection);
        }
    }

    private void write(ClientMessage message, Connection connection) {
        try {
            connection.write(message);
        } catch (CancelledKeyException ignored) {
            //if connection closes, while writing we can get CancelledKeyException.
            // In that case, we can safely ignore the exception
            EmptyStatement.ignore(ignored);
        }
    }

    public void registerListener(ClientEndpoint clientEndpoint, long correlationId) {
        if (pushScheduled.compareAndSet(false, true)) {
            schedulePeriodicPush();
        }
        clusterListeningEndpoints.put(clientEndpoint, correlationId);

        ClientMessage memberListViewMessage = createMemberListViewClientMessage();
        memberListViewMessage.setCorrelationId(correlationId);
        write(memberListViewMessage, clientEndpoint.getConnection());

        ClientMessage partitionViewMessage = createPartitionViewClientMessage();
        if (partitionViewMessage != null) {
            partitionViewMessage.setCorrelationId(correlationId);
            write(partitionViewMessage, clientEndpoint.getConnection());
        }
    }

    @Nullable
    private ClientMessage createPartitionViewClientMessage() {
        InternalPartitionService partitionService = (InternalPartitionService) nodeEngine.getPartitionService();
        OwnedPartitionsPerMemberInterceptor.PartitionsInfo partitionsInfo = partitionService.getPartitionsInfo();
        Map<UUID, List<Integer>> partitionIdByUuid = partitionsInfo.getPartitionIdsByMemberUuid();
        int sum = 0;
        for (List<Integer> partitions : partitionIdByUuid.values()) {
            sum += partitions.size();
        }
        return sum == partitionService.getPartitionCount()
                ? encodePartitionsViewEvent(partitionService.getPartitionStateVersion(), partitionIdByUuid.entrySet())
                : null;
    }

    private ClientMessage createMemberListViewClientMessage() {
        MembershipManager membershipManager = ((ClusterServiceImpl) nodeEngine.getClusterService()).getMembershipManager();
        MembersView membersView = membershipManager.getMembersView();

        int version = membersView.getVersion();
        List<MemberInfo> members = membersView.getMembers();

        List<MemberInfo> memberInfos = new LinkedList<>();
        for (MemberInfo member : members) {
            memberInfos.add(new MemberInfo(clientAddressOf(member.getAddress()), member.getUuid(), member.getAttributes(),
                    member.isLiteMember(), member.getVersion()));
        }

        return ClientAddClusterViewListenerCodec.encodeMembersViewEvent(version, memberInfos);
    }

    public void deregisterListener(ClientEndpoint clientEndpoint) {
        clusterListeningEndpoints.remove(clientEndpoint);
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
    public Map<ClientEndpoint, Long> getClusterListeningEndpoints() {
        return clusterListeningEndpoints;
    }
}
