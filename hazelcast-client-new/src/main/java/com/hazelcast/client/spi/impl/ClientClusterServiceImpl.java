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
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.ClientImpl;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.UuidUtil;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link ClientClusterService} implementation.
 */
public class ClientClusterServiceImpl extends ClusterListenerSupport {

    private static final ILogger LOGGER = Logger.getLogger(ClientClusterService.class);
    private final AtomicReference<Map<Address, Member>> membersRef = new AtomicReference<Map<Address, Member>>();
    private final ConcurrentMap<String, MembershipListener> listeners = new ConcurrentHashMap<String, MembershipListener>();

    public ClientClusterServiceImpl(HazelcastClientInstanceImpl client) {
        super(client);
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
    }

    @Override
    public Member getMember(Address address) {
        final Map<Address, Member> members = membersRef.get();
        return members != null ? members.get(address) : null;
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
        final Map<Address, Member> members = membersRef.get();
        return members != null ? members.values() : Collections.<Member>emptySet();
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

        final String id = UuidUtil.buildRandomUuidString();
        listeners.put(id, listener);
        if (listener instanceof InitialMembershipListener) {
            // TODO: needs sync with membership events...
            final Cluster cluster = client.getCluster();
            ((InitialMembershipListener) listener).init(new InitialMembershipEvent(cluster, cluster.getMembers()));
        }
        return id;
    }

    private String addMembershipListenerWithoutInit(MembershipListener listener) {
        final String id = UUID.randomUUID().toString();
        listeners.put(id, listener);
        return id;
    }

    private void initMembershipListener() {
        for (MembershipListener membershipListener : listeners.values()) {
            if (membershipListener instanceof InitialMembershipListener) {
                // TODO: needs sync with membership events...
                Cluster cluster = client.getCluster();
                InitialMembershipEvent event = new InitialMembershipEvent(cluster, cluster.getMembers());
                ((InitialMembershipListener) membershipListener).init(event);
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
        initMembershipListener();
    }

    private ClientConfig getClientConfig() {
        return client.getClientConfig();
    }

    String membersString() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        final Collection<Member> members = getMemberList();
        sb.append(members != null ? members.size() : 0);
        sb.append("] {");
        if (members != null) {
            for (Member member : members) {
                sb.append("\n\t").append(member);
            }
        }
        sb.append("\n}\n");
        return sb.toString();
    }

    void fireMembershipEvent(final MembershipEvent event) {
        ClientExecutionService clientExecutionService = client.getClientExecutionService();
        clientExecutionService.execute(new Runnable() {
            @Override
            public void run() {
                for (MembershipListener listener : listeners.values()) {
                    if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
                        listener.memberAdded(event);
                    } else {
                        listener.memberRemoved(event);
                    }
                }
            }
        });
    }

    void fireMemberAttributeEvent(final MemberAttributeEvent event) {
        ClientExecutionService clientExecutionService = client.getClientExecutionService();
        clientExecutionService.execute(new Runnable() {
            @Override
            public void run() {
                for (MembershipListener listener : listeners.values()) {
                    listener.memberAttributeChanged(event);
                }
            }
        });
    }

    Map<Address, Member> getMembersRef() {
        return membersRef.get();
    }

    void setMembersRef(Map<Address, Member> map) {
        membersRef.set(map);
    }
}
