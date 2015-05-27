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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientMembershipListenerCodec;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.client.ClientInitialMembershipEvent;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class ClientMembershipListener extends ClientMembershipListenerCodec.AbstractEventHandler
        implements EventHandler<ClientMessage> {

    public static final int INITIAL_MEMBERS_TIMEOUT_SECONDS = 5;
    private static final ILogger LOGGER = com.hazelcast.logging.Logger.getLogger(ClientMembershipListener.class);
    private final List<MemberImpl> members = new LinkedList<MemberImpl>();
    private final HazelcastClientInstanceImpl client;
    private final ClientClusterServiceImpl clusterService;
    private final ClientPartitionServiceImpl partitionService;
    private final ClientConnectionManagerImpl connectionManager;

    private volatile CountDownLatch initialListFetchedLatch;

    public ClientMembershipListener(HazelcastClientInstanceImpl client) {
        this.client = client;
        connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
        partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
    }

    @Override
    public void handle(Member member, int eventType) {
        switch (eventType) {
            case MembershipEvent.MEMBER_ADDED:
                memberAdded((MemberImpl) member);
                break;
            case MembershipEvent.MEMBER_REMOVED:
                memberRemoved((MemberImpl) member);
                break;
            default:
                LOGGER.warning("Unknown event type :" + eventType);
        }
        partitionService.refreshPartitions();
    }

    @Override
    public void handle(Collection<Member> members) {
        Map<String, MemberImpl> prevMembers = Collections.emptyMap();
        if (!members.isEmpty()) {
            prevMembers = new HashMap<String, MemberImpl>(members.size());
            for (Member member : members) {
                prevMembers.put(member.getUuid(), (MemberImpl) member);
            }
            members.clear();
        }
        members.addAll(members);

        final List<MembershipEvent> events = detectMembershipEvents(prevMembers);
        if (events.size() != 0) {
            applyMemberListChanges();
        }
        fireMembershipEvent(events);
        initialListFetchedLatch.countDown();
    }

    @Override
    public void handle(MemberAttributeChange memberAttributeChange) {
        Map<Address, MemberImpl> memberMap = clusterService.getMembersRef();
        if (memberMap == null) {
            return;
        }
        if (memberAttributeChange == null) {
            return;
        }
        for (MemberImpl target : memberMap.values()) {
            if (target.getUuid().equals(memberAttributeChange.getUuid())) {
                final MemberAttributeOperationType operationType = memberAttributeChange.getOperationType();
                final String key = memberAttributeChange.getKey();
                final Object value = memberAttributeChange.getValue();
                target.updateAttribute(operationType, key, value);
                MemberAttributeEvent memberAttributeEvent = new MemberAttributeEvent(client.getCluster(), target, operationType,
                        key, value);
                clusterService.fireMemberAttributeEvent(memberAttributeEvent);
                break;
            }
        }
    }

    @Override
    public void beforeListenerRegister() {

    }

    @Override
    public void onListenerRegister() {

    }

    void listenMembershipEvents(Address ownerConnectionAddress) {
        initialListFetchedLatch = new CountDownLatch(1);
        try {
            ClientMessage clientMessage = ClientMembershipListenerCodec.encodeRequest();

            Connection connection = connectionManager.getConnection(ownerConnectionAddress);
            if (connection == null) {
                System.out.println("FATAL connection null " + ownerConnectionAddress);
                throw new IllegalStateException(
                        "Can not load initial members list because owner connection is null. " + "Address "
                                + ownerConnectionAddress);
            }
            ClientInvocation invocation = new ClientInvocation(client, this, clientMessage, connection);
            invocation.invoke().get();
            waitInitialMemberListFetched();

        } catch (Exception e) {
            if (client.getLifecycleService().isRunning()) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.warning("Error while registering to cluster events! -> " + ownerConnectionAddress, e);
                } else {
                    LOGGER.warning("Error while registering to cluster events! -> " + ownerConnectionAddress + ", Error: " + e
                            .toString());
                }
            }
        }
    }

    private void waitInitialMemberListFetched()
            throws InterruptedException {
        boolean success = initialListFetchedLatch.await(INITIAL_MEMBERS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!success) {
            LOGGER.warning("Error while getting initial member list from cluster!");
        }
    }

    void initialMembers(Collection<MemberImpl> memberList) {


    }

    private void memberRemoved(MemberImpl member) {
        members.remove(member);
        final Connection connection = connectionManager.getConnection(member.getAddress());
        if (connection != null) {
            connectionManager.destroyConnection(connection);
        }
        applyMemberListChanges();
        MembershipEvent event = new MembershipEvent(client.getCluster(), member, ClientInitialMembershipEvent.MEMBER_REMOVED,
                Collections.unmodifiableSet(new LinkedHashSet<Member>(members)));
        clusterService.fireMembershipEvent(event);
    }

    private void applyMemberListChanges() {
        updateMembersRef();
        LOGGER.info(clusterService.membersString());
    }

    private void fireMembershipEvent(List<MembershipEvent> events) {

        for (MembershipEvent event : events) {
            clusterService.fireMembershipEvent(event);
        }
    }

    private List<MembershipEvent> detectMembershipEvents(Map<String, MemberImpl> prevMembers) {
        final List<MembershipEvent> events = new LinkedList<MembershipEvent>();
        final Set<Member> eventMembers = Collections.unmodifiableSet(new LinkedHashSet<Member>(members));
        for (MemberImpl member : members) {
            final MemberImpl former = prevMembers.remove(member.getUuid());
            if (former == null) {
                events.add(new MembershipEvent(client.getCluster(), member, MembershipEvent.MEMBER_ADDED, eventMembers));
            }
        }
        for (MemberImpl member : prevMembers.values()) {
            events.add(new MembershipEvent(client.getCluster(), member, MembershipEvent.MEMBER_REMOVED, eventMembers));
            if (clusterService.getMember(member.getAddress()) == null) {
                final Connection connection = connectionManager.getConnection(member.getAddress());
                if (connection != null) {
                    connectionManager.destroyConnection(connection);
                }
            }
        }
        return events;
    }

    private void memberAdded(MemberImpl member) {
        members.add(member);
        applyMemberListChanges();
        MembershipEvent event = new MembershipEvent(client.getCluster(), member, ClientInitialMembershipEvent.MEMBER_ADDED,
                Collections.unmodifiableSet(new LinkedHashSet<Member>(members)));
        clusterService.fireMembershipEvent(event);
    }

    private void updateMembersRef() {
        final Map<Address, MemberImpl> map = new LinkedHashMap<Address, MemberImpl>(members.size());
        for (MemberImpl member : members) {
            map.put(member.getAddress(), member);
        }
        clusterService.setMembersRef(Collections.unmodifiableMap(map));
    }

}
