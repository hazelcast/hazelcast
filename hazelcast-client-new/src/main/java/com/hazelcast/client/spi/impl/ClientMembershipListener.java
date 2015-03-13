package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.client.ClientInitialMembershipEvent;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.cluster.client.RegisterMembershipListenerRequest;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class ClientMembershipListener implements EventHandler<ClientInitialMembershipEvent> {

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
    public void handle(ClientInitialMembershipEvent event) {
        final MemberImpl member = (MemberImpl) event.getMember();
        if (event.getEventType() == ClientInitialMembershipEvent.INITIAL_MEMBERS) {
            initialMembers(event);
            initialListFetchedLatch.countDown();
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

    void listenMembershipEvents(Address ownerConnectionAddress) {
        initialListFetchedLatch = new CountDownLatch(1);
        try {
            RegisterMembershipListenerRequest request = new RegisterMembershipListenerRequest();

            Connection connection = connectionManager.getConnection(ownerConnectionAddress);
            if (connection == null) {
                System.out.println("FATAL connection null " + ownerConnectionAddress);
                throw new IllegalStateException("Can not load initial members list because owner connection is null. "
                        + "Address " + ownerConnectionAddress);
            }
            ClientInvocation invocation = new ClientInvocation(client, this, request, connection);
            invocation.invoke().get();
            waitInitialMemberListFetched();

        } catch (Exception e) {
            if (client.getLifecycleService().isRunning()) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.warning("Error while registering to cluster events! -> " + ownerConnectionAddress, e);
                } else {
                    LOGGER.warning("Error while registering to cluster events! -> " + ownerConnectionAddress
                            + ", Error: " + e.toString());
                }
            }
        }
    }

    private void waitInitialMemberListFetched() throws InterruptedException {
        boolean success = initialListFetchedLatch.await(INITIAL_MEMBERS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (success) {
            LOGGER.warning("Error while getting initial member list from cluster!");
        }
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
