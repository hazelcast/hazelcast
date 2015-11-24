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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.ClientImpl;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.cluster.impl.MemberSelectingCollection;
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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.util.Clock;
import com.hazelcast.util.UuidUtil;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
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
public class ClientClusterServiceImpl extends ClusterListenerSupport {

    private static final ILogger LOGGER = Logger.getLogger(ClientClusterService.class);
    private final AtomicReference<Map<Address, Member>> members = new AtomicReference<Map<Address, Member>>();
    private final ConcurrentMap<String, MembershipListener> listeners = new ConcurrentHashMap<String, MembershipListener>();
    private final Object initialMembershipListenerMutex = new Object();

    public ClientClusterServiceImpl(HazelcastClientInstanceImpl client, Collection<AddressProvider> addressProviders) {
        super(client, addressProviders);
        final ClientConfig clientConfig = getClientConfig();
        final List<ListenerConfig> listenerConfigs = client.getClientConfig().getListenerConfigs();
        for (ListenerConfig listenerConfig : listenerConfigs) {
            EventListener listener = listenerConfig.getImplementation();
            if (listener == null) {
                try {
                    listener = ClassLoaderUtil.newInstance(clientConfig.getClassLoader(),
                            listenerConfig.getClassName());
                } catch (Exception e) {
                    LOGGER.severe(e);
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

    public Collection<Member> getMembers(MemberSelector selector) {
        return new MemberSelectingCollection<Member>(getMemberList(), selector);
    }

    @Override
    public Address getMasterAddress() {
        final Collection<Member> memberList = getMemberList();
        Member member = memberList.iterator().next();
        return !memberList.isEmpty() ? member.getAddress() : null;
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
        Address address = getOwnerConnectionAddress();
        final ClientConnectionManager cm = client.getConnectionManager();
        final ClientConnection connection = (ClientConnection) cm.getConnection(address);
        InetSocketAddress inetSocketAddress = connection != null ? connection.getLocalSocketAddress() : null;
        final String uuid = getPrincipal().getUuid();
        return new ClientImpl(uuid, inetSocketAddress);
    }

    SerializationService getSerializationService() {
        return client.getSerializationService();
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

    private void initMembershipListener(MembershipListener listener) {
        if (listener instanceof InitialMembershipListener) {
            Cluster cluster = client.getCluster();
            Collection<Member> memberCollection = members.get().values();
            Set<Member> members = Collections.unmodifiableSet(new LinkedHashSet<Member>(memberCollection));
            ((InitialMembershipListener) listener).init(new InitialMembershipEvent(cluster, members));
        }
    }

    private String addMembershipListenerWithoutInit(MembershipListener listener) {
        String id = UuidUtil.newUnsecureUuidString();
        listeners.put(id, listener);
        return id;
    }

    private void initMembershipListeners() {
        synchronized (initialMembershipListenerMutex) {
            for (MembershipListener listener : listeners.values()) {
                initMembershipListener(listener);
            }
        }
    }

    @Override
    public boolean removeMembershipListener(String registrationId) {
        if (registrationId == null) {
            throw new NullPointerException("registrationId can't be null");
        }

        return listeners.remove(registrationId) != null;
    }

    public void start() throws Exception {
        init();
        connectToCluster();
        initMembershipListeners();
    }

    private ClientConfig getClientConfig() {
        return client.getClientConfig();
    }

    void handleMembershipEvent(MembershipEvent event) {
        synchronized (initialMembershipListenerMutex) {
            Member member = event.getMember();
            if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
                LinkedHashMap<Address, Member> newMap = new LinkedHashMap<Address, Member>(members.get());
                newMap.put(member.getAddress(), member);
                members.set(Collections.unmodifiableMap(newMap));
            } else {
                LinkedHashMap<Address, Member> newMap = new LinkedHashMap<Address, Member>(members.get());
                newMap.remove(member.getAddress());
                members.set(Collections.unmodifiableMap(newMap));
            }

            fireMembershipEvent(event);
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


}
