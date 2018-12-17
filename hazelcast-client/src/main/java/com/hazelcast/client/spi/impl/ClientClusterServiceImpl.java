/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.clientside.ClientImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.internal.cluster.impl.MemberSelectingCollection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.Clock;
import com.hazelcast.util.UuidUtil;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link ClientClusterService} implementation.
 */
public class ClientClusterServiceImpl implements ClientClusterService {

    protected final HazelcastClientInstanceImpl client;
    private ClientMembershipListener clientMembershipListener;
    private final AtomicReference<Map<Address, Member>> members = new AtomicReference<Map<Address, Member>>();
    private final ConcurrentMap<String, MembershipListener> listeners = new ConcurrentHashMap<String, MembershipListener>();
    private final Object initialMembershipListenerMutex = new Object();
    private final Map<String, String> attributes;

    public ClientClusterServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        attributes = Collections.unmodifiableMap(new HashMap<String, String>(client.getClientConfig().getAttributes()));
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
        members.set(Collections.unmodifiableMap(new LinkedHashMap<Address, Member>()));
    }

    @Override
    public Member getMember(Address address) {
        return members.get().get(address);
    }

    @Override
    public Member getMember(String uuid) {
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
    public Collection<Member> getMembers(MemberSelector selector) {
        return new MemberSelectingCollection<Member>(getMemberList(), selector);
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
    public int getSize(MemberSelector selector) {
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
        final ClientConnection connection = cm.getOwnerConnection();
        InetSocketAddress inetSocketAddress = connection != null ? connection.getLocalSocketAddress() : null;
        ClientPrincipal principal = cm.getPrincipal();
        final String uuid = principal != null ? principal.getUuid() : null;
        return new ClientImpl(uuid, inetSocketAddress, client.getName(), attributes);
    }

    @Override
    public String addMembershipListener(MembershipListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        synchronized (initialMembershipListenerMutex) {
            String id = addMembershipListenerWithoutInit(listener);
            initMembershipListener(listener);
            return id;
        }
    }

    private String addMembershipListenerWithoutInit(MembershipListener listener) {
        String id = UuidUtil.newUnsecureUuidString();
        listeners.put(id, listener);
        return id;
    }

    private void initMembershipListener(MembershipListener listener) {
        if (listener instanceof InitialMembershipListener) {
            Cluster cluster = client.getCluster();
            Collection<Member> memberCollection = members.get().values();
            LinkedHashSet<Member> members = new LinkedHashSet<Member>(memberCollection);
            InitialMembershipEvent event = new InitialMembershipEvent(cluster, members);
            ((InitialMembershipListener) listener).init(event);
        }
    }

    @Override
    public boolean removeMembershipListener(String registrationId) {
        if (registrationId == null) {
            throw new NullPointerException("registrationId can't be null");
        }
        return listeners.remove(registrationId) != null;
    }

    public void listenMembershipEvents(Connection ownerConnection) throws Exception {
        this.clientMembershipListener.listenMembershipEvents(ownerConnection);
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
            LinkedHashMap<Address, Member> newMap = new LinkedHashMap<Address, Member>();
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
            LinkedHashMap<Address, Member> newMap = new LinkedHashMap<Address, Member>(members.get());
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
}
