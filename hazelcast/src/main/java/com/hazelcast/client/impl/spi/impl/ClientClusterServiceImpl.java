/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.proxy.ClientClusterProxy;
import com.hazelcast.client.impl.spi.ClientClusterService;
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
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
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
 * Responsible for
 * - keeping track of the cluster members and serving them to other services
 * - firing membership events based on the incoming MemberListSnapshot events.
 */
public class ClientClusterServiceImpl implements ClientClusterService {
    /**
     * Initial list version is used at the start and also after cluster has changed with blue-green deployment feature.
     * In both cases, we need to fire InitialMembershipEvent.
     */
    public static final int INITIAL_MEMBER_LIST_VERSION = -1;
    private static final int INITIAL_MEMBERS_TIMEOUT_SECONDS = 120;
    private final AtomicReference<MemberListSnapshot> memberListSnapshot =
            new AtomicReference<>(new MemberListSnapshot(INITIAL_MEMBER_LIST_VERSION, new LinkedHashMap<>(), null));
    private final ConcurrentMap<UUID, MembershipListener> listeners = new ConcurrentHashMap<>();
    private final ILogger logger;
    private final Object clusterViewLock = new Object();
    //read and written under clusterViewLock
    private CountDownLatch initialListFetchedLatch = new CountDownLatch(1);

    private static final class MemberListSnapshot {
        private final int version;
        private final LinkedHashMap<UUID, Member> members;
        private final UUID clusterUuid;

        private MemberListSnapshot(int version, LinkedHashMap<UUID, Member> members, UUID clusterUuid) {
            this.version = version;
            this.members = members;
            this.clusterUuid = clusterUuid;
        }
    }

    public ClientClusterServiceImpl(ILogger logger) {
        this.logger = logger;
    }

    public Cluster getCluster() {
        return new ClientClusterProxy(this);
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

    @Nonnull
    @Override
    public UUID addMembershipListener(@Nonnull MembershipListener listener) {
        checkNotNull(listener, "Listener can't be null");

        synchronized (clusterViewLock) {
            UUID id = addMembershipListenerWithoutInit(listener);
            if (listener instanceof InitialMembershipListener) {
                Cluster cluster = getCluster();
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

    public void onClusterConnect() {
        synchronized (clusterViewLock) {
            if (logger.isFineEnabled()) {
                logger.fine("Resetting the member list version ");
            }
            MemberListSnapshot clusterViewSnapshot = memberListSnapshot.get();
            // This check is necessary so in order not to override changing cluster information when:
            // - registering cluster view listener back to the new cluster.
            // - on authentication response when cluster uuid change is detected.
            if (clusterViewSnapshot.version != INITIAL_MEMBER_LIST_VERSION) {
                memberListSnapshot.set(new MemberListSnapshot(0,
                        clusterViewSnapshot.members,
                        clusterViewSnapshot.clusterUuid));
            }
        }
    }

    public void onClusterChange() {
        synchronized (clusterViewLock) {
            if (logger.isFineEnabled()) {
                logger.fine("Resetting the cluster snapshot");
            }
            initialListFetchedLatch = new CountDownLatch(1);
            MemberListSnapshot clusterViewSnapshot = memberListSnapshot.get();
            memberListSnapshot.set(new MemberListSnapshot(INITIAL_MEMBER_LIST_VERSION,
                    clusterViewSnapshot.members,
                    clusterViewSnapshot.clusterUuid));
        }
    }

    //public for tests on enterprise
    public int getMemberListVersion() {
        return memberListSnapshot.get().version;
    }

    private void applyInitialState(int version, Collection<MemberInfo> memberInfos, UUID clusterUuid) {
        MemberListSnapshot snapshot = createSnapshot(version, memberInfos, clusterUuid);
        memberListSnapshot.set(snapshot);
        logger.info(membersString(snapshot));
        Set<Member> members = toUnmodifiableHasSet(snapshot.members.values());
        InitialMembershipEvent event = new InitialMembershipEvent(getCluster(), members);
        for (MembershipListener listener : listeners.values()) {
            if (listener instanceof InitialMembershipListener) {
                ((InitialMembershipListener) listener).init(event);
            }
        }
    }

    private MemberListSnapshot createSnapshot(int memberListVersion, Collection<MemberInfo> memberInfos, UUID clusterUuid) {
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
                    .memberListJoinVersion(memberInfo.getMemberListJoinVersion());
            newMembers.put(memberInfo.getUuid(), memberBuilder.build());
        }
        return new MemberListSnapshot(memberListVersion, newMembers, clusterUuid);
    }

    private Set<Member> toUnmodifiableHasSet(Collection<Member> members) {
        return unmodifiableSet(new HashSet<>(members));
    }

    private List<MembershipEvent> detectMembershipEvents(Collection<Member> prevMembers,
                                                         Set<Member> currentMembers,
                                                         UUID clusterUuid) {
        List<Member> newMembers = new LinkedList<>();
        Set<Member> deadMembers = new HashSet<>(prevMembers);

        if (clusterUuid.equals(memberListSnapshot.get().clusterUuid)) {
            for (Member member : currentMembers) {
                if (!deadMembers.remove(member)) {
                    newMembers.add(member);
                }
            }
        } else {
            // if cluster uuid is not same, then we will not try to match the current members to previous members
            // As a result all previous members are dead and all current members are new members
            newMembers.addAll(currentMembers);
        }

        List<MembershipEvent> events = new LinkedList<>();

        for (Member member : deadMembers) {
            events.add(new MembershipEvent(getCluster(), member, MembershipEvent.MEMBER_REMOVED, currentMembers));
        }
        for (Member member : newMembers) {
            events.add(new MembershipEvent(getCluster(), member, MembershipEvent.MEMBER_ADDED, currentMembers));
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

    public void handleMembersViewEvent(int memberListVersion, Collection<MemberInfo> memberInfos, UUID clusterUuid) {
        if (logger.isFinestEnabled()) {
            MemberListSnapshot snapshot = createSnapshot(memberListVersion, memberInfos, clusterUuid);
            logger.finest("Handling new snapshot with membership version: " + memberListVersion + ", membersString "
                    + membersString(snapshot));
        }
        MemberListSnapshot clusterViewSnapshot = memberListSnapshot.get();
        if (clusterViewSnapshot.version == INITIAL_MEMBER_LIST_VERSION) {
            synchronized (clusterViewLock) {
                clusterViewSnapshot = memberListSnapshot.get();
                if (clusterViewSnapshot.version == INITIAL_MEMBER_LIST_VERSION) {
                    //this means this is the first time client connected to cluster/cluster has changed(blue/green)
                    applyInitialState(memberListVersion, memberInfos, clusterUuid);
                    initialListFetchedLatch.countDown();
                    return;
                }
            }
        }

        List<MembershipEvent> events = emptyList();
        if (memberListVersion > clusterViewSnapshot.version) {
            synchronized (clusterViewLock) {
                clusterViewSnapshot = memberListSnapshot.get();
                if (memberListVersion > clusterViewSnapshot.version) {
                    Collection<Member> prevMembers = clusterViewSnapshot.members.values();
                    UUID previousClusterUuid = clusterViewSnapshot.clusterUuid;
                    MemberListSnapshot snapshot = createSnapshot(memberListVersion, memberInfos, clusterUuid);
                    memberListSnapshot.set(snapshot);
                    Set<Member> currentMembers = toUnmodifiableHasSet(snapshot.members.values());
                    events = detectMembershipEvents(prevMembers, currentMembers, previousClusterUuid);
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
