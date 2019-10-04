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
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberAttributeEvent;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.internal.cluster.impl.MemberSelectingCollection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.UuidUtil;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * The {@link ClientClusterService} implementation.
 */
public class ClientClusterServiceImpl implements ClientClusterService {

    protected final HazelcastClientInstanceImpl client;
    private ClientMembershipListener clientMembershipListener;
    private final AtomicReference<Map<Address, Member>> members = new AtomicReference<>();
    private final ConcurrentMap<UUID, MembershipListener> listeners = new ConcurrentHashMap<>();
    private final Object initialMembershipListenerMutex = new Object();
    private final Set<String> labels;

    public ClientClusterServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        labels = Collections.unmodifiableSet(client.getClientConfig().getLabels());
        ILogger logger = client.getLoggingService().getLogger(ClientClusterService.class);
        ClientConfig clientConfig = getClientConfig();
        List<ListenerConfig> listenerConfigs = client.getClientConfig().getListenerConfigs();
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
        }
        members.set(Collections.unmodifiableMap(new LinkedHashMap<>()));
    }

    @Override
    public Member getMember(Address address) {
        return members.get().get(address);
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
        return members.get().values();
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
        final ClientConnection connection = cm.getActiveConnections().iterator().next();
        InetSocketAddress inetSocketAddress = connection != null ? connection.getLocalSocketAddress() : null;
        UUID clientUuid = cm.getClientUuid();
        return new ClientImpl(clientUuid, inetSocketAddress, client.getName(), labels);
    }

    @Nonnull
    @Override
    public UUID addMembershipListener(@Nonnull MembershipListener listener) {
        checkNotNull(listener, "Listener can't be null");

        synchronized (initialMembershipListenerMutex) {
            UUID id = addMembershipListenerWithoutInit(listener);
            initMembershipListener(listener);
            return id;
        }
    }

    private @Nonnull
    UUID addMembershipListenerWithoutInit(@Nonnull MembershipListener listener) {
        UUID id = UuidUtil.newUnsecureUUID();
        listeners.put(id, listener);
        return id;
    }

    private void initMembershipListener(MembershipListener listener) {
        if (listener instanceof InitialMembershipListener) {
            Cluster cluster = client.getCluster();
            Collection<Member> memberCollection = members.get().values();
            if (memberCollection.isEmpty()) {
                //if members are empty,it means initial event did not arrive yet
                //it will be redirected to listeners when it arrives see #handleInitialMembershipEvent
                return;
            }
            LinkedHashSet<Member> members = new LinkedHashSet<>(memberCollection);
            InitialMembershipEvent event = new InitialMembershipEvent(cluster, members);
            ((InitialMembershipListener) listener).init(event);
        }
    }

    @Override
    public boolean removeMembershipListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId can't be null");
        return listeners.remove(registrationId) != null;
    }

    public void listenMembershipEvents(Connection ownerConnection) throws Exception {
        this.clientMembershipListener.listenMembershipEvents(ownerConnection);
    }

    public void cleanupOnDisconnect() {
        this.clientMembershipListener.cleanupOnDisconnect();
    }

    public void start() {
        this.clientMembershipListener = new ClientMembershipListener(client);
    }

    private ClientConfig getClientConfig() {
        return client.getClientConfig();
    }

    void handleInitialMembershipEvent(InitialMembershipEvent event) {
        synchronized (initialMembershipListenerMutex) {
            Set<Member> initialMembers = event.getMembers();
            LinkedHashMap<Address, Member> newMap = new LinkedHashMap<>();
            for (Member initialMember : initialMembers) {
                newMap.put(initialMember.getAddress(), initialMember);
            }
            members.set(Collections.unmodifiableMap(newMap));
            fireInitialMembershipEvent(event);
        }
    }

    void handleMembershipEvent(MembershipEvent event) {
        synchronized (initialMembershipListenerMutex) {
            Member member = event.getMember();
            LinkedHashMap<Address, Member> newMap = new LinkedHashMap<>(members.get());
            if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
                newMap.put(member.getAddress(), member);
            } else {
                newMap.remove(member.getAddress());
            }
            members.set(Collections.unmodifiableMap(newMap));
            fireMembershipEvent(event);
        }
    }

    private void fireInitialMembershipEvent(InitialMembershipEvent event) {
        for (MembershipListener listener : listeners.values()) {
            if (listener instanceof InitialMembershipListener) {
                ((InitialMembershipListener) listener).init(event);
            }
        }
    }

    private void fireMembershipEvent(MembershipEvent event) {
        for (MembershipListener listener : listeners.values()) {
            if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
                listener.memberAdded(event);
            } else {
                listener.memberRemoved(event);
            }
        }
    }

    void fireMemberAttributeEvent(final MemberAttributeEvent event) {
        for (MembershipListener listener : listeners.values()) {
            listener.memberAttributeChanged(event);
        }
    }

    public void shutdown() {
    }

    public void reset() {
        clientMembershipListener.clearMembers();
        synchronized (initialMembershipListenerMutex) {
            members.set(Collections.emptyMap());
        }
    }
}
