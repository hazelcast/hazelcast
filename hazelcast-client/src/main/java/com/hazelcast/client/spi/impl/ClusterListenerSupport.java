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

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.AuthenticationRequest;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.client.GetMemberListRequest;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.client.ClientMembershipEvent;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.cluster.client.RegisterMembershipListenerRequest;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Level;

public class ClusterListenerSupport implements ConnectionListener, ConnectionHeartbeatListener {

    private static final ILogger LOGGER = Logger.getLogger(ClusterListenerSupport.class);

    protected final List<MemberImpl> members = new LinkedList<MemberImpl>();
    protected ClientClusterServiceImpl clusterService;

    private final Collection<AddressProvider> addressProviders;
    private final ManagerAuthenticator managerAuthenticator = new ManagerAuthenticator();
    private final boolean shuffleMemberList;

    private Credentials credentials;
    private HazelcastClientInstanceImpl client;
    private ClientConnectionManager connectionManager;
    private ClientListenerServiceImpl clientListenerService;
    private volatile Address ownerConnectionAddress;
    private volatile ClientPrincipal principal;

    public ClusterListenerSupport(Collection<AddressProvider> addressProviders, boolean shuffleMemberList) {
        this.addressProviders = addressProviders;
        this.shuffleMemberList = shuffleMemberList;
    }

    public void init(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.connectionManager = client.getConnectionManager();
        this.clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        this.clientListenerService = (ClientListenerServiceImpl) client.getListenerService();
        connectionManager.addConnectionListener(this);
        connectionManager.addConnectionHeartbeatListener(this);
        credentials = client.getCredentials();
    }

    public Address getOwnerConnectionAddress() {
        return ownerConnectionAddress;
    }

    public class ManagerAuthenticator implements Authenticator {

        @Override
        public void authenticate(ClientConnection connection) throws AuthenticationException, IOException {
            final SerializationService ss = client.getSerializationService();
            AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
            connection.init();
            auth.setOwnerConnection(true);
            //contains remoteAddress and principal
            SerializableCollection collectionWrapper;
            final ClientInvocation clientInvocation = new ClientInvocation(client, auth, connection);
            final Future<SerializableCollection> future = clientInvocation.invoke();
            try {
                collectionWrapper = ss.toObject(future.get());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e, IOException.class);
            }
            final Iterator<Data> iter = collectionWrapper.iterator();
            final Data addressData = iter.next();
            final Address address = ss.toObject(addressData);
            connection.setRemoteEndpoint(address);
            final Data principalData = iter.next();
            principal = ss.toObject(principalData);
        }
    }

    public void connectToCluster() {
        try {
            connectToOne();
        } catch (Exception e) {
            client.getLifecycleService().shutdown();
            throw ExceptionUtil.rethrow(e);
        }

        try {
            clientListenerService.triggerFailedListeners();
            loadInitialMemberList();
            listenMembershipEvents();
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

    private Collection<InetSocketAddress> getSocketAddresses() {
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
        if (!members.isEmpty()) {
            for (MemberImpl member : members) {
                socketAddresses.add(member.getInetSocketAddress());
            }
        }

        for (AddressProvider addressProvider : addressProviders) {
            socketAddresses.addAll(addressProvider.loadAddresses());
        }

        if (shuffleMemberList) {
            Collections.shuffle(socketAddresses);
        }

        return socketAddresses;
    }

    private void loadInitialMemberList() throws Exception {
        final SerializationService serializationService = clusterService.getSerializationService();
        final GetMemberListRequest request = new GetMemberListRequest();
        final Connection connection = connectionManager.getConnection(ownerConnectionAddress);
        if (connection == null) {
            throw new IllegalStateException("Can not load initial members list because owner connection is null. "
                    + "Address " + ownerConnectionAddress);
        }
        final ClientInvocation clientInvocation = new ClientInvocation(client, request, connection);
        final Future<SerializableCollection> future = clientInvocation.invoke();
        final SerializableCollection coll = serializationService.toObject(future.get());

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
        LOGGER.info(clusterService.membersString());
        fireMembershipEvent(prevMembers);
    }

    private void fireMembershipEvent(Map<String, MemberImpl> prevMembers) {
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
        for (MembershipEvent event : events) {
            clusterService.fireMembershipEvent(event);
        }
    }

    private void listenMembershipEvents() throws Exception {
        final RegisterMembershipListenerRequest request = new RegisterMembershipListenerRequest();
        final EventHandler<ClientMembershipEvent> handler = createEventHandler();
        final ClientInvocation invocation = new ClientInvocation(client, handler, request, ownerConnectionAddress);
        final Future<SerializableCollection> future = invocation.invoke();
        final SerializationService serializationService = clusterService.getSerializationService();
        final Object response = serializationService.toObject(future.get());
        if (response instanceof Exception) {
            throw (Exception) response;
        }
    }

    private EventHandler<ClientMembershipEvent> createEventHandler() {
        return new ClientMembershipEventEventHandler();
    }

    protected void updateMembersRef() {
        final Map<Address, MemberImpl> map = new LinkedHashMap<Address, MemberImpl>(members.size());
        for (MemberImpl member : members) {
            map.put(member.getAddress(), member);
        }
        clusterService.setMembersRef(Collections.unmodifiableMap(map));
    }

    public ClientPrincipal getPrincipal() {
        return principal;
    }

    private void connectToOne() throws Exception {
        final ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        final int connAttemptLimit = networkConfig.getConnectionAttemptLimit();
        final int connectionAttemptPeriod = networkConfig.getConnectionAttemptPeriod();

        final int connectionAttemptLimit = connAttemptLimit == 0 ? Integer.MAX_VALUE : connAttemptLimit;

        int attempt = 0;
        Throwable lastError = null;
        Set<InetSocketAddress> triedAddresses = new HashSet<InetSocketAddress>();
        while (attempt < connectionAttemptLimit) {
            attempt++;
            final long nextTry = Clock.currentTimeMillis() + connectionAttemptPeriod;

            final Throwable throwable = connect(triedAddresses);

            if (throwable == null) {
                return;
            }

            lastError = throwable;

            final long remainingTime = nextTry - Clock.currentTimeMillis();
            LOGGER.warning(
                    String.format("Unable to get alive cluster connection,"
                                    + " try in %d ms later, attempt %d of %d.",
                            Math.max(0, remainingTime), attempt, connectionAttemptLimit));

            if (remainingTime > 0) {
                try {
                    Thread.sleep(remainingTime);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        throw new IllegalStateException("Unable to connect to any address in the config! "
                + "The following addresses were tried:" + triedAddresses, lastError);
    }

    private Throwable connect(Set<InetSocketAddress> triedAddresses) {
        final Collection<InetSocketAddress> socketAddresses = getSocketAddresses();
        Throwable lastError = null;
        for (InetSocketAddress inetSocketAddress : socketAddresses) {
            try {
                triedAddresses.add(inetSocketAddress);
                Address address = new Address(inetSocketAddress);
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Trying to connect to " + address);
                }
                final Connection connection = connectionManager.getOrConnect(address, managerAuthenticator);
                clusterService.fireConnectionEvent(LifecycleEvent.LifecycleState.CLIENT_CONNECTED);
                ownerConnectionAddress = connection.getEndPoint();
                return null;
            } catch (Exception e) {
                lastError = e;
                Level level = e instanceof AuthenticationException ? Level.WARNING : Level.FINEST;
                LOGGER.log(level, "Exception during initial connection to " + inetSocketAddress, e);
            }
        }
        return lastError;
    }

    @Override
    public void connectionAdded(Connection connection) {

    }

    @Override
    public void connectionRemoved(Connection connection) {
        if (connection.getEndPoint().equals(ownerConnectionAddress)) {
            if (client.getLifecycleService().isRunning()) {
                client.getClientExecutionService().execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            clusterService.fireConnectionEvent(LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED);
                            clusterService.getClusterListenerSupport().connectToCluster();
                        } catch (Exception e) {
                            LOGGER.warning("Could not re-connect to cluster", e);
                        }
                    }
                });
            }
        }
    }

    @Override
    public void heartBeatStarted(Connection connection) {

    }

    @Override
    public void heartBeatStopped(Connection connection) {
        if (connection.getEndPoint().equals(ownerConnectionAddress)) {
            connectionManager.destroyConnection(connection);
        }
    }

    private class ClientMembershipEventEventHandler implements EventHandler<ClientMembershipEvent> {
        @Override
        public void handle(ClientMembershipEvent event) {
            final MemberImpl member = (MemberImpl) event.getMember();
            boolean membersUpdated = false;
            if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
                members.add(member);
                membersUpdated = true;
            } else if (event.getEventType() == ClientMembershipEvent.MEMBER_REMOVED) {
                members.remove(member);
                membersUpdated = true;
                final Connection connection = connectionManager.getConnection(member.getAddress());
                if (connection != null) {
                    connectionManager.destroyConnection(connection);
                }
            } else if (event.getEventType() == ClientMembershipEvent.MEMBER_ATTRIBUTE_CHANGED) {
                MemberAttributeChange memberAttributeChange = event.getMemberAttributeChange();
                Map<Address, MemberImpl> memberMap = clusterService.getMembersRef();
                if (memberMap != null) {
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
            }

            if (membersUpdated) {
                ((ClientPartitionServiceImpl) client.getClientPartitionService()).refreshPartitions();
                updateMembersRef();
                LOGGER.info(clusterService.membersString());
                clusterService.fireMembershipEvent(new MembershipEvent(client.getCluster(), member, event.getEventType(),
                        Collections.unmodifiableSet(new LinkedHashSet<Member>(members))));
            }
        }

        @Override
        public void beforeListenerRegister() {

        }

        @Override
        public void onListenerRegister() {

        }
    }
}

