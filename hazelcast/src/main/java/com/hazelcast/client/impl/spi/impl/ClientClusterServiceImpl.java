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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.Client;
import com.hazelcast.client.impl.ClientImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnection;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.MemberSelectingCollection;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetDisconnectedException;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.util.Collection;
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

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableSet;

/**
 * The {@link ClientClusterService}  and {@link ClientPartitionService} implementation.
 */
public class ClientClusterServiceImpl
        implements ClientClusterService {

    private static final int INITIAL_MEMBERS_TIMEOUT_SECONDS = 120;

    private static final MemberListSnapshot EMPTY_SNAPSHOT = new MemberListSnapshot(-1, new LinkedHashMap<>());
    private final HazelcastClientInstanceImpl client;

    private final AtomicReference<MemberListSnapshot> memberListSnapshot = new AtomicReference<>(EMPTY_SNAPSHOT);
    private final ConcurrentMap<UUID, MembershipListener> listeners = new ConcurrentHashMap<>();
    private final Set<String> labels;
    private final ILogger logger;
    private final ClientConnectionManager connectionManager;
    private final Object clusterViewLock = new Object();
    //read and written under clusterViewLock
    private CountDownLatch initialListFetchedLatch = new CountDownLatch(1);

    private static final class MemberListSnapshot {
        private final int version;
        private final LinkedHashMap<UUID, Member> members;

        private MemberListSnapshot(int version, LinkedHashMap<UUID, Member> members) {
            this.version = version;
            this.members = members;
        }
    }

    public ClientClusterServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        labels = unmodifiableSet(client.getClientConfig().getLabels());
        logger = client.getLoggingService().getLogger(ClientClusterService.class);
        connectionManager = client.getConnectionManager();
    }

    @Override
    public Member getMember(@Nonnull UUID uuid) {
        checkNotNull(uuid, "UUID must not be null");
        return memberListSnapshot.get().members.get(uuid);
    }

    @Override
    public Collection<Member> getMemberList() {
        return memberListSnapshot.get().members.values();
    }

    @Override
    public Collection<Member> getMembers(@Nonnull MemberSelector selector) {
        checkNotNull(selector, "selector must not be null");
        return new MemberSelectingCollection<>(getMemberList(), selector);
    }

    @Override
    public Member getMasterMember() {
        final Collection<Member> memberList = getMemberList();
        Iterator<Member> iterator = memberList.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public int getSize() {
        return getMemberList().size();
    }

    @Override
    public long getClusterTime() {
        return Clock.currentTimeMillis();
    }

    @Override
    public Client getLocalClient() {
        final ClientConnectionManager cm = client.getConnectionManager();
        final TcpClientConnection connection = (TcpClientConnection) cm.getRandomConnection();
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
                Collection<Member> members = memberListSnapshot.get().members.values();
                //if members are empty,it means initial event did not arrive yet
                //it will be redirected to listeners when it arrives see #handleInitialMembershipEvent
                if (!members.isEmpty()) {
                    InitialMembershipEvent event = new InitialMembershipEvent(cluster, toUnmodifiableHasSet(members));
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

    public void start(Collection<EventListener> configuredListeners) {
        configuredListeners.stream().filter(listener -> listener instanceof MembershipListener)
                .forEach(listener -> addMembershipListener((MembershipListener) listener));
    }

    public void waitInitialMemberListFetched() {
        try {
            boolean success = initialListFetchedLatch.await(INITIAL_MEMBERS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                throw new IllegalStateException("Could not get initial member list from cluster!");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void clearMemberListVersion() {
        synchronized (clusterViewLock) {
            if (logger.isFineEnabled()) {
                logger.fine("Resetting the member list version ");
            }
            MemberListSnapshot clusterViewSnapshot = memberListSnapshot.get();
            //This check is necessary so that `clearMemberListVersion` when handling auth response will not
            //intervene with client failover logic
            if (clusterViewSnapshot != EMPTY_SNAPSHOT) {
                memberListSnapshot.set(new MemberListSnapshot(0, clusterViewSnapshot.members));
            }
        }
    }

    public void reset() {
        synchronized (clusterViewLock) {
            if (logger.isFineEnabled()) {
                logger.fine("Resetting the cluster snapshot");
            }
            initialListFetchedLatch = new CountDownLatch(1);
            memberListSnapshot.set(EMPTY_SNAPSHOT);
        }
    }

    private void applyInitialState(int version, Collection<MemberInfo> memberInfos) {
        MemberListSnapshot snapshot = createSnapshot(version, memberInfos);
        memberListSnapshot.set(snapshot);
        logger.info(membersString(snapshot));
        Set<Member> members = toUnmodifiableHasSet(snapshot.members.values());
        InitialMembershipEvent event = new InitialMembershipEvent(client.getCluster(), members);
        for (MembershipListener listener : listeners.values()) {
            if (listener instanceof InitialMembershipListener) {
                ((InitialMembershipListener) listener).init(event);
            }
        }
    }

    private MemberListSnapshot createSnapshot(int memberListVersion, Collection<MemberInfo> memberInfos) {
        LinkedHashMap<UUID, Member> newMembers = new LinkedHashMap<>();
        for (MemberInfo memberInfo : memberInfos) {
            MemberImpl.Builder memberBuilder;
            Map<EndpointQualifier, Address> addressMap = memberInfo.getAddressMap();
            if (addressMap == null || addressMap.isEmpty()) {
                memberBuilder = new MemberImpl.Builder(memberInfo.getAddress());
            } else {
                memberBuilder = new MemberImpl.Builder(addressMap)
                        .address(addressMap.getOrDefault(CLIENT, addressMap.get(MEMBER)));
            }
            memberBuilder.version(memberInfo.getVersion())
                    .uuid(memberInfo.getUuid())
                    .attributes(memberInfo.getAttributes())
                    .liteMember(memberInfo.isLiteMember())
                    .memberListJoinVersion(memberInfo.getMemberListJoinVersion()).build();
            newMembers.put(memberInfo.getUuid(), memberBuilder.build());
        }
        return new MemberListSnapshot(memberListVersion, newMembers);
    }

    private Set<Member> toUnmodifiableHasSet(Collection<Member> members) {
        return unmodifiableSet(new HashSet<>(members));
    }

    private List<MembershipEvent> detectMembershipEvents(Collection<Member> prevMembers, Set<Member> currentMembers) {
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
            Connection connection = connectionManager.getConnection(member.getUuid());
            if (connection != null) {
                connection.close(null,
                        new TargetDisconnectedException("The client has closed the connection to this member,"
                                + " after receiving a member left event from the cluster. " + connection));
            }
        }
        for (Member member : newMembers) {
            events.add(new MembershipEvent(client.getCluster(), member, MembershipEvent.MEMBER_ADDED, currentMembers));
        }

        if (events.size() != 0) {
            MemberListSnapshot snapshot = memberListSnapshot.get();
            if (snapshot.members.values().size() != 0) {
                logger.info(membersString(snapshot));
            }
        }
        return events;
    }

    private String membersString(MemberListSnapshot snapshot) {
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

    public void handleMembersViewEvent(int memberListVersion, Collection<MemberInfo> memberInfos) {
        if (logger.isFinestEnabled()) {
            MemberListSnapshot snapshot = createSnapshot(memberListVersion, memberInfos);
            logger.finest("Handling new snapshot with membership version: " + memberListVersion + ", membersString "
                    + membersString(snapshot));
        }
        MemberListSnapshot clusterViewSnapshot = memberListSnapshot.get();
        if (clusterViewSnapshot == EMPTY_SNAPSHOT) {
            synchronized (clusterViewLock) {
                clusterViewSnapshot = memberListSnapshot.get();
                if (clusterViewSnapshot == EMPTY_SNAPSHOT) {
                    //this means this is the first time client connected to cluster
                    applyInitialState(memberListVersion, memberInfos);
                    initialListFetchedLatch.countDown();
                    return;
                }
            }
        }

        List<MembershipEvent> events = emptyList();
        if (memberListVersion >= clusterViewSnapshot.version) {
            synchronized (clusterViewLock) {
                clusterViewSnapshot = memberListSnapshot.get();
                if (memberListVersion >= clusterViewSnapshot.version) {
                    Collection<Member> prevMembers = clusterViewSnapshot.members.values();
                    MemberListSnapshot snapshot = createSnapshot(memberListVersion, memberInfos);
                    memberListSnapshot.set(snapshot);
                    Set<Member> currentMembers = toUnmodifiableHasSet(snapshot.members.values());
                    events = detectMembershipEvents(prevMembers, currentMembers);
                }
            }
        }
        fireEvents(events);
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
}
