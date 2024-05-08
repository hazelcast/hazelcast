/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.cluster.impl.MembershipManager;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.partition.impl.PartitionStateManagerImpl;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.scheduler.CoalescingDelayedTrigger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.partitiongroup.MemberGroup;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.nio.channels.CancelledKeyException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ClusterViewListenerService {
    private static final HazelcastProperty PUSH_PERIOD_IN_SECONDS
            = new HazelcastProperty("hazelcast.client.internal.push.period.seconds", 30, SECONDS);
    private static final long PARTITION_UPDATE_DELAY_MS = 100;
    private static final long PARTITION_UPDATE_MAX_DELAY_MS = 500;

    private final Map<ClientEndpoint, Long> clusterListeningEndpoints = new ConcurrentHashMap<>();
    private final NodeEngine nodeEngine;
    private final boolean advancedNetworkConfigEnabled;
    private final AtomicBoolean pushScheduled = new AtomicBoolean();
    private final CoalescingDelayedTrigger delayedPartitionUpdateTrigger;
    // This is an emulation of the pre-4.1 partition state version.
    // We will increment this version if a partition table change is detected
    // while sending partition table to the client.
    // Because of the client compatibility requirement, this integer version
    // wil remain until the next major version of the client protocol.
    private final AtomicInteger partitionTableVersion = new AtomicInteger();
    // Latest observed partition stamp
    private final AtomicLong latestPartitionStamp = new AtomicLong();

    ClusterViewListenerService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.advancedNetworkConfigEnabled = nodeEngine.getConfig().getAdvancedNetworkConfig().isEnabled();
        this.delayedPartitionUpdateTrigger = new CoalescingDelayedTrigger(nodeEngine.getExecutionService(),
                PARTITION_UPDATE_DELAY_MS, PARTITION_UPDATE_MAX_DELAY_MS, this::pushPartitionTableView);
    }

    private void schedulePeriodicPush() {
        ExecutionService executor = nodeEngine.getExecutionService();
        int pushPeriodInSeconds = nodeEngine.getProperties().getSeconds(PUSH_PERIOD_IN_SECONDS);
        executor.scheduleWithRepetition(this::pushView, pushPeriodInSeconds, pushPeriodInSeconds, TimeUnit.SECONDS);
    }

    private void pushView() {
        pushPartitionTableView();
        MembersView membersView = getMembersView();
        ClientMessage memberListViewMessage = getMemberListViewMessage(membersView);
        sendToListeningEndpoints(memberListViewMessage);

        Collection<Collection<UUID>> memberGroups = toMemberGroups(membersView);
        ClientMessage memberGroupsViewEvent = ClientAddClusterViewListenerCodec
                .encodeMemberGroupsViewEvent(membersView.getVersion(), memberGroups);
        sendToListeningEndpoints(memberGroupsViewEvent);
    }

    private void pushPartitionTableView() {
        ClientMessage partitionViewMessage = getPartitionViewMessageOrNull();
        if (partitionViewMessage != null) {
            sendToListeningEndpoints(partitionViewMessage);
        }
    }

    public void onPartitionStateChange() {
        delayedPartitionUpdateTrigger.executeWithDelay();
    }

    public void onMemberListChange() {
        MembersView membersView = getMembersView();
        sendToListeningEndpoints(getMemberListViewMessage(membersView));
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

        MembersView processedMembersView = getMembersView();
        ClientMessage memberListViewMessage = getMemberListViewMessage(processedMembersView);
        memberListViewMessage.setCorrelationId(correlationId);
        write(memberListViewMessage, clientEndpoint.getConnection());

        ClientMessage partitionViewMessage = getPartitionViewMessageOrNull();
        if (partitionViewMessage != null) {
            partitionViewMessage.setCorrelationId(correlationId);
            write(partitionViewMessage, clientEndpoint.getConnection());
        }

        int version = processedMembersView.getVersion();
        Collection<Collection<UUID>> memberGroups = toMemberGroups(processedMembersView);
        ClientMessage memberGroupsViewEvent = ClientAddClusterViewListenerCodec
                .encodeMemberGroupsViewEvent(version, memberGroups);
        write(memberGroupsViewEvent, clientEndpoint.getConnection());
    }

    public MembersView getMembersView() {
        MembershipManager membershipManager
                = ((ClusterServiceImpl) nodeEngine.getClusterService()).getMembershipManager();
        MembersView membersView = membershipManager.getMembersView();

        int version = membersView.getVersion();
        List<MemberInfo> members = membersView.getMembers();

        ArrayList<MemberInfo> memberInfos = new ArrayList<>();
        for (MemberInfo member : members) {
            Address address = clientAddressOf(member.getAddress());
            // Ignore any member without a CLIENT endpoint qualifier configured
            if (address == null) {
                continue;
            }
            memberInfos.add(new MemberInfo(address, member.getUuid(), member.getAttributes(),
                    member.isLiteMember(), member.getVersion(), member.getAddressMap()));
        }

        return new MembersView(version, memberInfos);
    }

    public record PartitionsView(Map<UUID, List<Integer>> partitions,
                                 int version) {
    }

    public PartitionsView getPartitionsViewOrNull() {
        InternalPartitionService partitionService = (InternalPartitionService) nodeEngine.getPartitionService();
        PartitionTableView partitionTableView = partitionService.createPartitionTableView();
        Map<UUID, List<Integer>> partitions = getPartitions(partitionTableView);
        if (partitions.isEmpty()) {
            return null;
        }

        int version;
        long currentStamp = partitionTableView.stamp();
        long latestStamp = latestPartitionStamp.get();
        if (currentStamp != latestStamp && latestPartitionStamp.compareAndSet(latestStamp, currentStamp)) {
            partitionTableVersion.incrementAndGet();
        }
        version = partitionTableVersion.get();

        return new PartitionsView(partitions, version);
    }

    /**
     * Converts server's existing view of members into collection
     * of uuid-collections based on partition grouping info.
     *
     * @param membersView   members in the cluster
     * @return member groups based on members' uuids
     */
    public Collection<Collection<UUID>> toMemberGroups(MembersView membersView) {
        if (!nodeEngine.getNode().getBuildInfo().isEnterprise()) {
            return Collections.emptyList();
        }

        List<Member> list = new ArrayList<>(membersView.size());
        List<MemberInfo> members = membersView.getMembers();
        for (MemberInfo memberInfo : members) {
            MemberImpl member = memberInfo.toMember();
            list.add(member);
        }

        PartitionStateManagerImpl partitionStateManager = getPartitionStateManager();
        Collection<MemberGroup> memberGroups = partitionStateManager.createMemberGroups(list);
        Collection<Collection<UUID>> allUuids = new ArrayList<>(memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            Collection<UUID> uuids = new ArrayList<>(memberGroup.size());

            Iterator<Member> iterator = memberGroup.iterator();
            while (iterator.hasNext()) {
                Member member = iterator.next();
                uuids.add(member.getUuid());
            }

            allUuids.add(uuids);
        }

        return allUuids;
    }

    private PartitionStateManagerImpl getPartitionStateManager() {
        return (PartitionStateManagerImpl) ((InternalPartitionService)
                nodeEngine.getPartitionService()).getPartitionStateManager();
    }

    private ClientMessage getPartitionViewMessageOrNull() {
        PartitionsView partitionsView = getPartitionsViewOrNull();
        if (partitionsView == null) {
            return null;
        }

        return ClientAddClusterViewListenerCodec.encodePartitionsViewEvent(partitionsView.version(),
                partitionsView.partitions().entrySet());
    }

    private ClientMessage getMemberListViewMessage(MembersView processedMembersView) {
        return ClientAddClusterViewListenerCodec.encodeMembersViewEvent(
                processedMembersView.getVersion(), processedMembersView.getMembers());
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

    /**
     * If any partition does not have an owner, this method returns empty collection
     *
     * @param partitionTableView will be converted to address-&gt;partitions mapping
     * @return address-&gt;partitions mapping, where address is the client address of the member
     */
    public Map<UUID, List<Integer>> getPartitions(PartitionTableView partitionTableView) {
        Map<UUID, List<Integer>> partitionsMap = new HashMap<>();

        int partitionCount = partitionTableView.length();

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            PartitionReplica owner = partitionTableView.getReplica(partitionId, 0);
            if (owner == null || owner.uuid() == null) {
                partitionsMap.clear();
                return partitionsMap;
            }
            partitionsMap.computeIfAbsent(owner.uuid(),
                    k -> new LinkedList<>()).add(partitionId);
        }
        return partitionsMap;
    }

    //for test purpose only
    public Map<ClientEndpoint, Long> getClusterListeningEndpoints() {
        return clusterListeningEndpoints;
    }
}
