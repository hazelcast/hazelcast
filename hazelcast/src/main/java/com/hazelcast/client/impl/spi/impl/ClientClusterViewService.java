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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.Client;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientImpl;
import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddClusterViewListenerCodec;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.ClientListenerService;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberAttributeEvent;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.cluster.impl.AbstractMember;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.MemberSelectingCollection;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.exception.TargetDisconnectedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableSet;

/**
 * The {@link ClientClusterService}  and {@link ClientPartitionService} implementation.
 */
public class ClientClusterViewService implements ClientClusterService, ClientPartitionService {

    private static final int INITIAL_MEMBERS_TIMEOUT_SECONDS = 120;
    private static final ListenerMessageCodec CLUSTER_VIEW_LISTENER_CODEC = new ListenerMessageCodec() {
        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return ClientAddClusterViewListenerCodec.encodeRequest(localOnly);
        }

        @Override
        public UUID decodeAddResponse(ClientMessage clientMessage) {
            return UuidUtil.NIL_UUID;
        }

        @Override
        public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
            return null;
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return true;
        }
    };
    private static final ClusterViewSnapshot EMPTY_SNAPSHOT =
            new ClusterViewSnapshot(-1, new LinkedHashMap<>(), Collections.emptySet(), -1, new Int2ObjectHashMap<>());
    private static final long BLOCKING_GET_ONCE_SLEEP_MILLIS = 100;
    private final HazelcastClientInstanceImpl client;
    private final AtomicReference<ClusterViewSnapshot> clusterSnapshot = new AtomicReference<>(EMPTY_SNAPSHOT);
    private final ConcurrentMap<UUID, MembershipListener> listeners = new ConcurrentHashMap<>();
    private final Object clusterViewLock = new Object();
    private final Set<String> labels;
    private final ILogger logger;
    private final ClientConnectionManager connectionManager;
    private final ClientListenerService listenerService;
    private final CountDownLatch initialListFetchedLatch = new CountDownLatch(1);
    private volatile int partitionCount;
    private volatile UUID clusterViewListenerUUID;

    private static final class ClusterViewSnapshot {
        private final int version;
        private final LinkedHashMap<Address, Member> members;
        private final Set<Member> memberSet;
        private final int partitionSateVersion;
        private final Int2ObjectHashMap<Address> partitions;

        private ClusterViewSnapshot(int version, LinkedHashMap<Address, Member> members, Set<Member> memberSet,
                                    int partitionSateVersion, Int2ObjectHashMap<Address> partitions) {
            this.version = version;
            this.members = members;
            this.memberSet = memberSet;
            this.partitionSateVersion = partitionSateVersion;
            this.partitions = partitions;
        }
    }

    public ClientClusterViewService(HazelcastClientInstanceImpl client) {
        this.client = client;
        labels = unmodifiableSet(client.getClientConfig().getLabels());
        logger = client.getLoggingService().getLogger(ClientClusterService.class);
        connectionManager = client.getConnectionManager();
        listenerService = client.getListenerService();
    }

    private void handleListenerConfigs() {
        ClientConfig clientConfig = client.getClientConfig();
        List<ListenerConfig> listenerConfigs = clientConfig.getListenerConfigs();
        for (ListenerConfig listenerConfig : listenerConfigs) {
            EventListener listener = listenerConfig.getImplementation();
            if (listener == null) {
                try {
                    listener = ClassLoaderUtil.newInstance(clientConfig.getClassLoader(), listenerConfig.getClassName());
                } catch (Exception e) {
                    logger.severe(e);
                }
            }
            if (listener instanceof MembershipListener) {
                addMembershipListenerWithoutInit((MembershipListener) listener);
            }
            if (listener instanceof PartitionLostListener) {
                client.getPartitionService().addPartitionLostListener((PartitionLostListener) listener);
            }
        }
    }

    @Override
    public Member getMember(Address address) {
        return clusterSnapshot.get().members.get(address);
    }

    @Override
    public Member getMember(@Nonnull UUID uuid) {
        checkNotNull(uuid, "UUID must not be null");
        final Collection<Member> memberList = getMemberList();
        for (Member member : memberList) {
            if (uuid.equals(member.getUuid())) {
                return member;
            }
        }
        return null;
    }

    @Override
    public Collection<Member> getMemberList() {
        return clusterSnapshot.get().members.values();
    }

    @Override
    public Collection<Member> getMembers(@Nonnull MemberSelector selector) {
        checkNotNull(selector, "selector must not be null");
        return new MemberSelectingCollection<>(getMemberList(), selector);
    }

    @Override
    public Address getMasterAddress() {
        final Collection<Member> memberList = getMemberList();
        return !memberList.isEmpty() ? new Address(memberList.iterator().next().getSocketAddress()) : null;
    }

    @Override
    public int getSize() {
        return getMemberList().size();
    }

    @Override
    public int getSize(@Nonnull MemberSelector selector) {
        checkNotNull(selector, "selector must not be null");
        int size = 0;
        for (Member member : getMemberList()) {
            if (selector.select(member)) {
                size++;
            }
        }
        return size;
    }

    @Override
    public long getClusterTime() {
        return Clock.currentTimeMillis();
    }

    @Override
    public Client getLocalClient() {
        final ClientConnectionManager cm = client.getConnectionManager();
        final ClientConnection connection = (ClientConnection) cm.getRandomConnection();
        InetSocketAddress inetSocketAddress = connection != null ? connection.getLocalSocketAddress() : null;
        UUID clientUuid = cm.getClientUuid();
        return new ClientImpl(clientUuid, inetSocketAddress, client.getName(), labels);
    }

    @Nonnull
    @Override
    public UUID addMembershipListener(@Nonnull MembershipListener listener) {
        checkNotNull(listener, "Listener can't be null");

        synchronized (clusterViewLock) {
            UUID id = addMembershipListenerWithoutInit(listener);
            if (listener instanceof InitialMembershipListener) {
                Cluster cluster = client.getCluster();
                Set<Member> members = clusterSnapshot.get().memberSet;
                //if members are empty,it means initial event did not arrive yet
                //it will be redirected to listeners when it arrives see #handleInitialMembershipEvent
                if (!members.isEmpty()) {
                    InitialMembershipEvent event = new InitialMembershipEvent(cluster, members);
                    ((InitialMembershipListener) listener).init(event);
                }
            }
            return id;
        }
    }

    private UUID addMembershipListenerWithoutInit(@Nonnull MembershipListener listener) {
        UUID id = UuidUtil.newUnsecureUUID();
        listeners.put(id, listener);
        return id;
    }

    @Override
    public boolean removeMembershipListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId can't be null");
        return listeners.remove(registrationId) != null;
    }

    public void start() {
        handleListenerConfigs();
        clusterViewListenerUUID = listenerService.registerListener(CLUSTER_VIEW_LISTENER_CODEC, new ClusterViewListenerHandler());
        waitInitialMemberListFetched();
    }

    public void shutdown() {
        UUID lastListenerUUID = this.clusterViewListenerUUID;
        if (lastListenerUUID != null) {
            listenerService.deregisterListener(lastListenerUUID);
        }
    }

    private void waitInitialMemberListFetched() {
        try {
            boolean success = initialListFetchedLatch.await(INITIAL_MEMBERS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                logger.warning("Error while getting initial member list from cluster!");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void reset() {
        List<MembershipEvent> events;
        synchronized (clusterViewLock) {
            if (logger.isFineEnabled()) {
                logger.fine("Resetting the cluster snapshot");
            }
            ClusterViewSnapshot cleanSnapshot = new ClusterViewSnapshot(-1, new LinkedHashMap<>(), Collections.emptySet(),
                    -1, new Int2ObjectHashMap<>());
            events = detectMembershipEvents(clusterSnapshot.get().memberSet, Collections.emptySet());
            clusterSnapshot.set(cleanSnapshot);
        }
        fireEvents(events);
    }

    private void applyInitialState(int version, Collection<MemberInfo> memberInfos,
                                   Collection<Map.Entry<Address, List<Integer>>> partitions, int partitionStateVersion) {
        Int2ObjectHashMap<Address> map = convertToPartitionToAddressMap(partitions);
        ClusterViewSnapshot snapshot = createSnapshot(version, memberInfos, map, partitionStateVersion);
        clusterSnapshot.set(snapshot);
        logger.info(membersString(snapshot));
        Set<Member> members = snapshot.memberSet;
        InitialMembershipEvent event = new InitialMembershipEvent(client.getCluster(), members);
        for (MembershipListener listener : listeners.values()) {
            if (listener instanceof InitialMembershipListener) {
                ((InitialMembershipListener) listener).init(event);
            }
        }
        onPartitionTableUpdate();
    }

    private void onPartitionTableUpdate() {
        //  partition count is set once at the start. Even if we reset the partition table when switching cluster
        // we want to remember the partition count. That is why it is a different field.
        ClusterViewSnapshot clusterViewSnapshot = clusterSnapshot.get();
        Int2ObjectHashMap<Address> newPartitions = clusterViewSnapshot.partitions;
        if (partitionCount == 0) {
            partitionCount = newPartitions.size();
        }
        if (logger.isFineEnabled()) {
            logger.fine("Processed partition response. partitionStateVersion : " + clusterViewSnapshot.partitionSateVersion
                    + ", partitionCount :" + newPartitions.size());
        }
    }

    private void applyNewState(int memberListVersion, Collection<MemberInfo> memberInfos,
                               Collection<Map.Entry<Address, List<Integer>>> partitions,
                               int partitionStateVersion,
                               ClusterViewSnapshot oldState) {
        ClusterViewSnapshot newState;
        if (fromSameMaster(oldState.members, memberInfos)) {
            //if masters are same, we can update member list and partition table separately
            if (partitionStateVersion > oldState.partitionSateVersion) {
                //incoming partition table is more up-to-date
                Int2ObjectHashMap<Address> map = convertToPartitionToAddressMap(partitions);
                newState = createSnapshot(memberListVersion, memberInfos, map, partitionStateVersion);
            } else {
                //re-use current partition table
                newState = createSnapshot(memberListVersion, memberInfos, oldState.partitions, oldState.partitionSateVersion);
            }
        } else {
            //masters are different switch with new state (both member list and partition table) atomically
            Int2ObjectHashMap<Address> map = convertToPartitionToAddressMap(partitions);
            newState = createSnapshot(memberListVersion, memberInfos, map, partitionStateVersion);
        }
        clusterSnapshot.set(newState);
        onPartitionTableUpdate();
    }

    private boolean fromSameMaster(Map<Address, Member> currentMembers, Collection<MemberInfo> newMemberInfos) {
        Iterator<Member> iterator = currentMembers.values().iterator();
        if (!iterator.hasNext()) {
            return false;
        }
        Member masterMember = iterator.next();
        MemberInfo newMaster = newMemberInfos.iterator().next();
        return masterMember.getUuid().equals(newMaster.getUuid());
    }

    private ClusterViewSnapshot createSnapshot(int version, Collection<MemberInfo> memberInfos,
                                               Int2ObjectHashMap<Address> partitions,
                                               int partitionStateVersion) {
        LinkedHashMap<Address, Member> newMembers = new LinkedHashMap<>();
        for (MemberInfo memberInfo : memberInfos) {
            Address address = memberInfo.getAddress();
            newMembers.put(address, new MemberImpl(address, memberInfo.getVersion(), memberInfo.getUuid(),
                    memberInfo.getAttributes(), memberInfo.isLiteMember()));
        }
        Set<Member> memberSet = unmodifiableSet(new HashSet<>(newMembers.values()));
        return new ClusterViewSnapshot(version, newMembers, memberSet, partitionStateVersion, partitions);
    }

    private List<MembershipEvent> detectMembershipEvents(Set<Member> prevMembers, Set<Member> currentMembers) {
        List<Member> newMembers = new LinkedList<>();
        Set<Member> deadMembers = new HashSet<>(prevMembers);
        for (Member member : currentMembers) {
            if (!deadMembers.remove(member)) {
                newMembers.add(member);
            }
        }

        List<MembershipEvent> events = new LinkedList<>();

        // removal events should be added before added events
        for (Member member : deadMembers) {
            events.add(new MembershipEvent(client.getCluster(), member, MembershipEvent.MEMBER_REMOVED, currentMembers));
            Address address = member.getAddress();
            if (getMember(address) == null) {
                Connection connection = connectionManager.getConnection(address);
                if (connection != null) {
                    connection.close(null,
                            new TargetDisconnectedException("The client has closed the connection to this member,"
                                    + " after receiving a member left event from the cluster. " + connection));
                }
            }
        }
        for (Member member : newMembers) {
            events.add(new MembershipEvent(client.getCluster(), member, MembershipEvent.MEMBER_ADDED, currentMembers));
        }

        if (events.size() != 0) {
            logger.info(membersString(clusterSnapshot.get()));
        }
        return events;
    }

    private String membersString(ClusterViewSnapshot snapshot) {
        Collection<Member> members = snapshot.members.values();
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        sb.append(members.size());
        sb.append("] {");
        for (Member member : members) {
            sb.append("\n\t").append(member);
        }
        sb.append("\n}\n");
        return sb.toString();
    }

    private class ClusterViewListenerHandler extends ClientAddClusterViewListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        @Override
        public void beforeListenerRegister(Connection connection) {
            if (logger.isFinestEnabled()) {
                logger.finest("Register attempt of ClusterViewListenerHandler to " + connection);
            }
        }

        @Override
        public void onListenerRegister(Connection connection) {
            if (logger.isFinestEnabled()) {
                logger.finest("Registered ClusterViewListenerHandler to " + connection);
            }
        }

        @Override
        public void handleMembersViewEvent(int memberListVersion, Collection<MemberInfo> memberInfos,
                                           Collection<Map.Entry<Address, List<Integer>>> partitions,
                                           int partitionStateVersion) {
            if (logger.isFinestEnabled()) {
                Int2ObjectHashMap<Address> map = convertToPartitionToAddressMap(partitions);
                ClusterViewSnapshot snapshot = createSnapshot(memberListVersion, memberInfos, map, partitionStateVersion);
                logger.finest("Handling new snapshot with membership version: " + memberListVersion + ", partitionStateVersion: "
                        + partitionStateVersion + ", membersString " + membersString(snapshot));
            }
            ClusterViewSnapshot clusterViewSnapshot = clusterSnapshot.get();
            if (clusterViewSnapshot == EMPTY_SNAPSHOT) {
                synchronized (clusterViewLock) {
                    clusterViewSnapshot = clusterSnapshot.get();
                    if (clusterViewSnapshot == EMPTY_SNAPSHOT) {
                        //this means this is the first time client connected to cluster
                        applyInitialState(memberListVersion, memberInfos, partitions, partitionStateVersion);
                        initialListFetchedLatch.countDown();
                        return;
                    }
                }
            }

            List<MembershipEvent> events = emptyList();
            if (memberListVersion >= clusterViewSnapshot.version) {
                synchronized (clusterViewLock) {
                    clusterViewSnapshot = clusterSnapshot.get();
                    if (memberListVersion >= clusterViewSnapshot.version) {
                        Set<Member> prevMembers = clusterSnapshot.get().memberSet;
                        applyNewState(memberListVersion, memberInfos, partitions, partitionStateVersion, clusterViewSnapshot);
                        Set<Member> currentMembers = clusterSnapshot.get().memberSet;
                        events = detectMembershipEvents(prevMembers, currentMembers);
                    }
                }
            }
            fireEvents(events);
        }

        @Override
        public void handleMemberAttributeChangeEvent(Member member, String key, int operationType, @Nullable String value) {
            Set<Member> currentMembers = clusterSnapshot.get().memberSet;
            Cluster cluster = client.getCluster();
            UUID uuid = member.getUuid();
            for (Member target : currentMembers) {
                if (target.getUuid().equals(uuid)) {
                    MemberAttributeOperationType type = MemberAttributeOperationType.getValue(operationType);
                    ((AbstractMember) target).updateAttribute(type, key, value);
                    MemberAttributeEvent event = new MemberAttributeEvent(cluster, target, currentMembers, type, key, value);
                    for (MembershipListener listener : listeners.values()) {
                        listener.memberAttributeChanged(event);
                    }
                    break;
                }
            }
        }
    }

    private void fireEvents(List<MembershipEvent> events) {
        for (MembershipEvent event : events) {
            for (MembershipListener listener : listeners.values()) {
                if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
                    listener.memberAdded(event);
                } else {
                    listener.memberRemoved(event);
                }
            }
        }
    }

    private Int2ObjectHashMap<Address> convertToPartitionToAddressMap(Collection<Map.Entry<Address, List<Integer>>> partitions) {
        Int2ObjectHashMap<Address> newPartitions = new Int2ObjectHashMap<Address>();
        for (Map.Entry<Address, List<Integer>> entry : partitions) {
            Address address = entry.getKey();
            for (Integer partition : entry.getValue()) {
                newPartitions.put(partition, address);
            }
        }
        return newPartitions;
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
        return clusterSnapshot.get().partitions.get(partitionId);
    }

    @Override
    public int getPartitionId(Data key) {
        final int pc = getPartitionCount();
        if (pc <= 0) {
            return 0;
        }
        int hash = key.getPartitionHash();
        return HashUtil.hashToIndex(hash, pc);
    }

    @Override
    public int getPartitionId(Object key) {
        final Data data = client.getSerializationService().toData(key);
        return getPartitionId(data);
    }

    @Override
    public int getPartitionCount() {
        while (partitionCount == 0 && connectionManager.isAlive()) {
            Set<Member> memberList = clusterSnapshot.get().memberSet;
            if (MemberSelectingCollection.count(memberList, MemberSelectors.DATA_MEMBER_SELECTOR) == 0) {
                throw new NoDataMemberInClusterException(
                        "Partitions can't be assigned since all nodes in the cluster are lite members");
            }
            try {
                Thread.sleep(BLOCKING_GET_ONCE_SLEEP_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw ExceptionUtil.rethrow(e);
            }
        }
        return partitionCount;
    }

    @Override
    public Partition getPartition(int partitionId) {
        return new PartitionImpl(partitionId);
    }

    private final class PartitionImpl implements Partition {

        private final int partitionId;

        private PartitionImpl(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public Member getOwner() {
            Address owner = getPartitionOwner(partitionId);
            if (owner != null) {
                return getMember(owner);
            }
            return null;
        }

        @Override
        public String toString() {
            return "PartitionImpl{partitionId=" + partitionId + '}';
        }
    }

}
