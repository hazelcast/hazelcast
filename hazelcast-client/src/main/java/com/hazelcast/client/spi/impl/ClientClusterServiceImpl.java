/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientImpl;
import com.hazelcast.client.ClientPrincipal;
import com.hazelcast.client.ClientResponse;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LifecycleServiceImpl;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.client.AddMembershipListenerRequest;
import com.hazelcast.cluster.client.ClientMembershipEvent;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.Clock;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.core.LifecycleEvent.LifecycleState;


/**
 * @author mdogan 5/15/13
 */
public final class ClientClusterServiceImpl implements ClientClusterService {

    private static final ILogger logger = Logger.getLogger(ClientClusterService.class);

    private final HazelcastClient client;
    private final ClientConnectionManagerImpl connectionManager;
    private final ClusterListenerThread clusterThread;
    private final AtomicReference<Map<Address, MemberImpl>> membersRef = new AtomicReference<Map<Address, MemberImpl>>();
    private final ConcurrentMap<String, MembershipListener> listeners = new ConcurrentHashMap<String, MembershipListener>();

    public ClientClusterServiceImpl(HazelcastClient client) {
        this.client = client;
        this.connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
        clusterThread = new ClusterListenerThread(client.getThreadGroup(), client.getName() + ".cluster-listener");
        final ClientConfig clientConfig = getClientConfig();
        final List<ListenerConfig> listenerConfigs = client.getClientConfig().getListenerConfigs();
        if (listenerConfigs != null && !listenerConfigs.isEmpty()) {
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
        }
    }

    public MemberImpl getMember(Address address) {
        final Map<Address, MemberImpl> members = membersRef.get();
        return members != null ? members.get(address) : null;
    }

    public MemberImpl getMember(String uuid) {
        final Collection<MemberImpl> memberList = getMemberList();
        for (MemberImpl member : memberList) {
            if (uuid.equals(member.getUuid())) {
                return member;
            }
        }
        return null;
    }

    public Collection<MemberImpl> getMemberList() {
        final Map<Address, MemberImpl> members = membersRef.get();
        return members != null ? members.values() : Collections.<MemberImpl>emptySet();
    }

    public Address getMasterAddress() {
        final Collection<MemberImpl> memberList = getMemberList();
        return !memberList.isEmpty() ? memberList.iterator().next().getAddress() : null;
    }

    public int getSize() {
        return getMemberList().size();
    }

    public long getClusterTime() {
        return Clock.currentTimeMillis();
    }

    public Client getLocalClient() {
        ClientPrincipal cp = connectionManager.getPrincipal();
        ClientConnection conn = clusterThread.conn;
        return new ClientImpl(cp != null ? cp.getUuid() : null, conn != null ? conn.getLocalSocketAddress() : null);
    }

    private SerializationService getSerializationService() {
        return client.getSerializationService();
    }

    public String addMembershipListenerWithInit(MembershipListener listener) {
        final String id = UuidUtil.buildRandomUuidString();
        listeners.put(id, listener);
        if (listener instanceof InitialMembershipListener) {
            // TODO: needs sync with membership events...
            final Cluster cluster = client.getCluster();
            ((InitialMembershipListener) listener).init(new InitialMembershipEvent(cluster, cluster.getMembers()));
        }
        return id;
    }

    public String addMembershipListenerWithoutInit(MembershipListener listener) {
        final String id = UUID.randomUUID().toString();
        listeners.put(id, listener);
        return id;
    }

    private void initMembershipListener() {
        for (MembershipListener membershipListener : listeners.values()) {
            if (membershipListener instanceof InitialMembershipListener) {
                // TODO: needs sync with membership events...
                final Cluster cluster = client.getCluster();
                ((InitialMembershipListener) membershipListener).init(new InitialMembershipEvent(cluster, cluster.getMembers()));
            }
        }
    }

    public boolean removeMembershipListener(String registrationId) {
        return listeners.remove(registrationId) != null;
    }

    public void start() {
        clusterThread.start();

        try {
            clusterThread.await();
        } catch (InterruptedException e) {
            throw new HazelcastException(e);
        }
        initMembershipListener();
        // started
    }

    public void stop() {
        clusterThread.shutdown();
    }

    private class ClusterListenerThread extends Thread {

        private ClusterListenerThread(ThreadGroup group, String name) {
            super(group, name);
        }

        private volatile ClientConnection conn;
        private final List<MemberImpl> members = new LinkedList<MemberImpl>();
        private final CountDownLatch latch = new CountDownLatch(1);

        public void await() throws InterruptedException {
            latch.await();
        }

        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (conn == null) {
                        try {
                            conn = connectToOne();
                        } catch (Exception e) {
                            logger.severe("Error while connecting to cluster!", e);
                            client.getLifecycleService().shutdown();
                            latch.countDown();
                            return;
                        }
                    }
                    getInvocationService().triggerFailedListeners();
                    loadInitialMemberList();
                    listenMembershipEvents();
                } catch (Exception e) {
                    if (client.getLifecycleService().isRunning()) {
                        if (logger.isFinestEnabled()) {
                            logger.warning("Error while listening cluster events! -> " + conn, e);
                        } else {
                            logger.warning("Error while listening cluster events! -> " + conn + ", Error: " + e.toString());
                        }
                    }

                    connectionManager.markOwnerAddressAsClosed();
                    IOUtil.closeResource(conn);
                    conn = null;
                    fireConnectionEvent(true);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    latch.countDown();
                    break;
                }
            }
        }

        private ClientInvocationServiceImpl getInvocationService() {
            return (ClientInvocationServiceImpl) client.getInvocationService();
        }

        protected Collection<InetSocketAddress> getSocketAddresses() {
            final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
            if (!members.isEmpty()) {
                for (MemberImpl member : members) {
                    socketAddresses.add(member.getInetSocketAddress());
                }
                Collections.shuffle(socketAddresses);
            }
            socketAddresses.addAll(getConfigAddresses());
            return socketAddresses;
        }

        private void loadInitialMemberList() throws Exception {
            final SerializationService serializationService = getSerializationService();
            final AddMembershipListenerRequest request = new AddMembershipListenerRequest();
            final SerializableCollection coll = (SerializableCollection) connectionManager.sendAndReceive(request, conn);

            Map<String, MemberImpl> prevMembers = Collections.emptyMap();
            if (!members.isEmpty()) {
                prevMembers = new HashMap<String, MemberImpl>(members.size());
                for (MemberImpl member : members) {
                    prevMembers.put(member.getUuid(), member);
                }
                members.clear();
            }
            for (Data data : coll) {
                members.add((MemberImpl) serializationService.toObject(data));
            }
            updateMembersRef();
            logger.info(membersString());
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
            }
            for (MembershipEvent event : events) {
                fireMembershipEvent(event);
            }
            latch.countDown();
        }

        private void listenMembershipEvents() throws IOException {
            final SerializationService serializationService = getSerializationService();
            while (!Thread.currentThread().isInterrupted()) {
                final Data clientResponseData = conn.read();
                final ClientResponse clientResponse = serializationService.toObject(clientResponseData);
                final Object eventObject = serializationService.toObject(clientResponse.getResponse());
                final ClientMembershipEvent event = (ClientMembershipEvent) eventObject;
                final MemberImpl member = (MemberImpl) event.getMember();
                boolean membersUpdated = false;
                if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
                    members.add(member);
                    membersUpdated = true;
                } else if (event.getEventType() == ClientMembershipEvent.MEMBER_REMOVED) {
                    members.remove(member);
                    membersUpdated = true;
//                    getConnectionManager().removeConnectionPool(member.getAddress()); //TODO
                } else if (event.getEventType() == ClientMembershipEvent.MEMBER_ATTRIBUTE_CHANGED) {
                    MemberAttributeChange memberAttributeChange = event.getMemberAttributeChange();
                    Map<Address, MemberImpl> memberMap = membersRef.get();
                    if (memberMap != null) {
                        for (MemberImpl target : memberMap.values()) {
                            if (target.getUuid().equals(memberAttributeChange.getUuid())) {
                                final MemberAttributeOperationType operationType = memberAttributeChange.getOperationType();
                                final String key = memberAttributeChange.getKey();
                                final Object value = memberAttributeChange.getValue();
                                target.updateAttribute(operationType, key, value);
                                MemberAttributeEvent memberAttributeEvent = new MemberAttributeEvent(
                                        client.getCluster(), target, operationType, key, value);
                                fireMemberAttributeEvent(memberAttributeEvent);
                                break;
                            }
                        }
                    }
                }

                if (membersUpdated) {
                    ((ClientPartitionServiceImpl) client.getClientPartitionService()).refreshPartitions();
                    updateMembersRef();
                    logger.info(membersString());
                    fireMembershipEvent(new MembershipEvent(client.getCluster(), member, event.getEventType(),
                            Collections.unmodifiableSet(new LinkedHashSet<Member>(members))));
                }
            }
        }

        private void fireMembershipEvent(final MembershipEvent event) {
            client.getClientExecutionService().execute(new Runnable() {
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

        private void fireMemberAttributeEvent(final MemberAttributeEvent event) {
            client.getClientExecutionService().execute(new Runnable() {
                @Override
                public void run() {
                    for (MembershipListener listener : listeners.values()) {
                        listener.memberAttributeChanged(event);
                    }
                }
            });
        }

        private void updateMembersRef() {
            final Map<Address, MemberImpl> map = new LinkedHashMap<Address, MemberImpl>(members.size());
            for (MemberImpl member : members) {
                map.put(member.getAddress(), member);
            }
            membersRef.set(Collections.unmodifiableMap(map));
        }

        void shutdown() {
            interrupt();
            final ClientConnection c = conn;
            if (c != null) {
                c.close();
            }
        }

        private ClientConnection connectToOne() throws Exception {
            final ClientNetworkConfig networkConfig = getClientConfig().getNetworkConfig();
            final int connectionAttemptLimit = networkConfig.getConnectionAttemptLimit();
            final int connectionAttemptPeriod = networkConfig.getConnectionAttemptPeriod();
            int attempt = 0;
            Throwable lastError = null;
            while (true) {
                final long nextTry = Clock.currentTimeMillis() + connectionAttemptPeriod;
                final Collection<InetSocketAddress> socketAddresses = getSocketAddresses();
                for (InetSocketAddress isa : socketAddresses) {
                    Address address = new Address(isa);
                    try {
                        final ClientConnection connection = connectionManager.ownerConnection(address);
                        fireConnectionEvent(false);
                        return connection;
                    } catch (IOException e) {
                        lastError = e;
                        logger.finest("IO error during initial connection...", e);
                    } catch (AuthenticationException e) {
                        lastError = e;
                        logger.warning("Authentication error on " + address, e);
                    }
                }
                if (attempt++ >= connectionAttemptLimit) {
                    break;
                }
                final long remainingTime = nextTry - Clock.currentTimeMillis();
                logger.warning(
                        String.format("Unable to get alive cluster connection," +
                                " try in %d ms later, attempt %d of %d.",
                                Math.max(0, remainingTime), attempt, connectionAttemptLimit));

                if (remainingTime > 0) {
                    try {
                        Thread.sleep(remainingTime);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
            throw new IllegalStateException("Unable to connect to any address in the config!", lastError);
        }
    }


    private void fireConnectionEvent(boolean disconnected) {
        final LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) client.getLifecycleService();
        final LifecycleState state = disconnected ? LifecycleState.CLIENT_DISCONNECTED : LifecycleState.CLIENT_CONNECTED;
        lifecycleService.fireLifecycleEvent(state);
    }

    private Collection<InetSocketAddress> getConfigAddresses() {
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
        final List<String> addresses = getClientConfig().getAddresses();
        Collections.shuffle(addresses);
        for (String address : addresses) {
            socketAddresses.addAll(AddressHelper.getSocketAddresses(address));
        }
        return socketAddresses;
    }

    private ClientConfig getClientConfig() {
        return client.getClientConfig();
    }

    private String membersString() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        final Collection<MemberImpl> members = getMemberList();
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

}
