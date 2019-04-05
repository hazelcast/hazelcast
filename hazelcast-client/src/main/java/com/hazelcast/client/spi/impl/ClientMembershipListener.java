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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddMembershipListenerCodec;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.AbstractMember;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.TargetDisconnectedException;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.unmodifiableSet;

class ClientMembershipListener extends ClientAddMembershipListenerCodec.AbstractEventHandler
        implements EventHandler<ClientMessage> {

    private static final int INITIAL_MEMBERS_TIMEOUT_SECONDS = 5;

    private final ILogger logger;
    private final HazelcastClientInstanceImpl client;
    private final ClientClusterServiceImpl clusterService;
    private final ClientPartitionServiceImpl partitionService;
    private final ClientConnectionManagerImpl connectionManager;
    private volatile Set<Member> members = new LinkedHashSet<Member>();

    private volatile CountDownLatch initialListFetchedLatch;

    ClientMembershipListener(HazelcastClientInstanceImpl client) {
        this.client = client;
        logger = client.getLoggingService().getLogger(ClientMembershipListener.class);
        connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
        partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
    }

    @Override
    public void handleMemberEventV10(Member member, int eventType) {
        switch (eventType) {
            case MembershipEvent.MEMBER_ADDED:
                memberAdded(member);
                break;
            case MembershipEvent.MEMBER_REMOVED:
                memberRemoved(member);
                break;
            default:
                logger.warning("Unknown event type: " + eventType);
        }
        partitionService.refreshPartitions();
    }

    @Override
    public void handleMemberListEventV10(Collection<Member> initialMembers) {
        Set<Member> prevMembers = Collections.emptySet();
        if (!members.isEmpty()) {
            prevMembers = new LinkedHashSet<Member>(members.size());
            prevMembers.addAll(members);
            members.clear();
        }

        members.addAll(initialMembers);

        if (prevMembers.isEmpty()) {
            //this means this is the first time client connected to cluster
            logger.info(membersString());
            clusterService.handleInitialMembershipEvent(
                    new InitialMembershipEvent(client.getCluster(), unmodifiableSet(members)));
            initialListFetchedLatch.countDown();
            return;
        }

        List<MembershipEvent> events = detectMembershipEvents(prevMembers);
        logger.info(membersString());
        fireMembershipEvent(events);
        initialListFetchedLatch.countDown();
    }

    @Override
    public void handleMemberAttributeChangeEventV10(String uuid, String key, int opType, String value) {
        Collection<Member> members = clusterService.getMemberList();
        for (Member target : members) {
            if (target.getUuid().equals(uuid)) {
                final MemberAttributeOperationType operationType = MemberAttributeOperationType.getValue(opType);
                ((AbstractMember) target).updateAttribute(operationType, key, value);
                MemberAttributeEvent memberAttributeEvent =
                        new MemberAttributeEvent(client.getCluster(), target, operationType, key, value);
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

    void listenMembershipEvents(Connection ownerConnection) throws Exception {
        initialListFetchedLatch = new CountDownLatch(1);
        ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeRequest(false);
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, null, ownerConnection);
        invocation.setEventHandler(this);
        invocation.invokeUrgent().get();
        waitInitialMemberListFetched();
    }

    private void waitInitialMemberListFetched() throws InterruptedException {
        boolean success = initialListFetchedLatch.await(INITIAL_MEMBERS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!success) {
            logger.warning("Error while getting initial member list from cluster!");
        }
    }

    private void memberRemoved(Member member) {
        members.remove(member);
        logger.info(membersString());
        final Connection connection = connectionManager.getActiveConnection(member.getAddress());
        if (connection != null) {
            connection.close(null, newTargetDisconnectedExceptionCausedByMemberLeftEvent(connection));
        }
        MembershipEvent event = new MembershipEvent(client.getCluster(), member, MembershipEvent.MEMBER_REMOVED,
                unmodifiableSet(members));
        clusterService.handleMembershipEvent(event);
    }

    private void fireMembershipEvent(List<MembershipEvent> events) {
        for (MembershipEvent event : events) {
            clusterService.handleMembershipEvent(event);
        }
    }

    private List<MembershipEvent> detectMembershipEvents(Set<Member> prevMembers) {
        List<MembershipEvent> events = new LinkedList<MembershipEvent>();
        Set<Member> eventMembers = unmodifiableSet(members);

        List<Member> newMembers = new LinkedList<Member>();
        for (Member member : members) {
            if (!prevMembers.remove(member)) {
                newMembers.add(member);
            }
        }

        // removal events should be added before added events
        for (Member member : prevMembers) {
            events.add(new MembershipEvent(client.getCluster(), member, MembershipEvent.MEMBER_REMOVED, eventMembers));
            Address address = member.getAddress();
            if (clusterService.getMember(address) == null) {
                Connection connection = connectionManager.getActiveConnection(address);
                if (connection != null) {
                    connection.close(null, newTargetDisconnectedExceptionCausedByMemberLeftEvent(connection));
                }
            }
        }
        for (Member member : newMembers) {
            events.add(new MembershipEvent(client.getCluster(), member, MembershipEvent.MEMBER_ADDED, eventMembers));
        }

        return events;
    }

    private Exception newTargetDisconnectedExceptionCausedByMemberLeftEvent(Connection connection) {
        return new TargetDisconnectedException("The client has closed the connection to this member,"
                + " after receiving a member left event from the cluster. " + connection);
    }

    private void memberAdded(Member member) {
        members.add(member);
        logger.info(membersString());
        MembershipEvent event = new MembershipEvent(client.getCluster(), member,
                MembershipEvent.MEMBER_ADDED, unmodifiableSet(members));
        clusterService.handleMembershipEvent(event);
    }

    private String membersString() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        sb.append(members.size());
        sb.append("] {");
        for (Member member : members) {
            sb.append("\n\t").append(member);
        }
        sb.append("\n}\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return "ClientMembershipListener{"
                + ", members=" + members
                + ", client=" + client
                + '}';
    }

    void clearMembers() {
        members = new LinkedHashSet<Member>();
    }
}
