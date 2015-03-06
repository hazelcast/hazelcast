package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.client.ClientInitialMembershipEvent;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ClientMembershipEventHandler implements EventHandler<ClientInitialMembershipEvent> {

    private static final ILogger LOGGER = com.hazelcast.logging.Logger.getLogger(ClientMembershipEventHandler.class);
    private final List<MemberImpl> members = new LinkedList<MemberImpl>();
    private final HazelcastClientInstanceImpl client;
    private final ClientClusterServiceImpl clusterService;
    private final ClientPartitionServiceImpl partitionService;
    private final ClientConnectionManagerImpl connectionManager;

    public ClientMembershipEventHandler(HazelcastClientInstanceImpl client) {
        this.client = client;
        connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
        partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
    }

    @Override
    public void handle(ClientInitialMembershipEvent event) {
        final MemberImpl member = (MemberImpl) event.getMember();
        if (event.getEventType() == ClientInitialMembershipEvent.INITIAL_MEMBERS) {
            initialMembers(event);
        } else if (event.getEventType() == ClientInitialMembershipEvent.MEMBER_ADDED) {
            memberAdded(member);
            partitionService.refreshPartitions();
        } else if (event.getEventType() == ClientInitialMembershipEvent.MEMBER_REMOVED) {
            memberRemoved(member);
            partitionService.refreshPartitions();
        } else if (event.getEventType() == ClientInitialMembershipEvent.MEMBER_ATTRIBUTE_CHANGED) {
            memberAttributeChanged(event);
        }
    }

    @Override
    public void beforeListenerRegister() {

    }

    @Override
    public void onListenerRegister() {

    }

    void initialMembers(ClientInitialMembershipEvent event) {

        Map<String, MemberImpl> prevMembers = Collections.emptyMap();
        if (!members.isEmpty()) {
            prevMembers = new HashMap<String, MemberImpl>(members.size());
            for (MemberImpl member : members) {
                prevMembers.put(member.getUuid(), member);
            }
            members.clear();
        }
        members.addAll(event.getMembers());


        final List<MembershipEvent> events = detectMembershipEvents(prevMembers);
        if (events.size() != 0) {
            applyMemberListChanges();
        }
        fireMembershipEvent(events);
    }


    private void memberRemoved(MemberImpl member) {
        members.remove(member);
        final Connection connection = connectionManager.getConnection(member.getAddress());
        if (connection != null) {
            connectionManager.destroyConnection(connection);
        }
        applyMemberListChanges();
        MembershipEvent event = new MembershipEvent(client.getCluster(), member,
                ClientInitialMembershipEvent.MEMBER_REMOVED,
                Collections.unmodifiableSet(new LinkedHashSet<Member>(members)));
        clusterService.fireMembershipEvent(event);
    }

    private void memberAttributeChanged(ClientInitialMembershipEvent event) {
        MemberAttributeChange memberAttributeChange = event.getMemberAttributeChange();
        Map<Address, MemberImpl> memberMap = clusterService.getMembersRef();
        if (memberMap == null) {
            return;
        }
        for (MemberImpl target : memberMap.values()) {
            if (target.getUuid().equals(memberAttributeChange.getUuid())) {
                final MemberAttributeOperationType operationType = memberAttributeChange.getOperationType();
                final String key = memberAttributeChange.getKey();
                final Object value = memberAttributeChange.getValue();
                target.updateAttribute(operationType, key, value);
                MemberAttributeEvent memberAttributeEvent = new MemberAttributeEvent(
                        client.getCluster(), target, operationType, key, value);
                clusterService.fireMemberAttributeEvent(memberAttributeEvent);
                break;
            }
        }
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
        MembershipEvent event = new MembershipEvent(client.getCluster(), member,
                ClientInitialMembershipEvent.MEMBER_ADDED,
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
