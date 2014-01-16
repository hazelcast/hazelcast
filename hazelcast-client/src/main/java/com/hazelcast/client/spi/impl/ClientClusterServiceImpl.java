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
import com.hazelcast.client.AuthenticationRequest;
import com.hazelcast.client.ClientImpl;
import com.hazelcast.client.ClientPrincipal;
import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.ClientResponse;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LifecycleServiceImpl;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.cluster.client.AddMembershipListenerRequest;
import com.hazelcast.cluster.client.ClientMemberAttributeChangedEvent;
import com.hazelcast.cluster.client.ClientMembershipEvent;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.operation.MapOperationType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ClientPacket;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataAdapter;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.core.LifecycleEvent.LifecycleState;


/**
 * @author mdogan 5/15/13
 */
public final class ClientClusterServiceImpl implements ClientClusterService {

    private static final ILogger logger = Logger.getLogger(ClientClusterService.class);
    public static int RETRY_COUNT = 20;

    private final HazelcastClient client;
    private final ClusterListenerThread clusterThread;
    private final AtomicReference<Map<Address, MemberImpl>> membersRef = new AtomicReference<Map<Address, MemberImpl>>();
    private final ConcurrentMap<String, MembershipListener> listeners = new ConcurrentHashMap<String, MembershipListener>();

    private final boolean redoOperation;
    private final Credentials credentials;
    private volatile ClientPrincipal principal;
    private volatile boolean active = false;

    private final AtomicInteger callIdIncrementer = new AtomicInteger();
    private final ConcurrentMap<Connection, ConcurrentMap<Integer, ClientCallFuture>> connectionCallMap = new ConcurrentHashMap<Connection, ConcurrentMap<Integer, ClientCallFuture>>();
    private final ConcurrentMap<Connection, ConcurrentMap<Integer, ClientCallFuture>> connectionEventHandlerMap = new ConcurrentHashMap<Connection, ConcurrentMap<Integer, ClientCallFuture>>();

    private final ConcurrentMap<String, Integer> registrationIdMap = new ConcurrentHashMap<String, Integer>();
    private final ConcurrentMap<String, String> registrationAliasMap = new ConcurrentHashMap<String, String>();

    public ClientClusterServiceImpl(HazelcastClient client) {
        this.client = client;
        clusterThread = new ClusterListenerThread(client.getThreadGroup(), client.getName() + ".cluster-listener");
        final ClientConfig clientConfig = getClientConfig();
        redoOperation = clientConfig.isRedoOperation();
        credentials = clientConfig.getCredentials();
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
                    _addMembershipListener((MembershipListener) listener);
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

    public boolean isRedoOperation() {
        return redoOperation;
    }

    public Client getLocalClient() {
        ClientPrincipal cp = principal;
        ClientConnection conn = clusterThread.conn;
        return new ClientImpl(cp != null ? cp.getUuid() : null, conn != null ? conn.getLocalSocketAddress() : null);
    }

    private SerializationService getSerializationService() {
        return client.getSerializationService();
    }

    private ClientConnectionManager getConnectionManager() {
        return client.getConnectionManager();
    }

    public Authenticator getAuthenticator() {
        return new ClusterAuthenticator();
    }

    public String addMembershipListener(MembershipListener listener) {
        final String id = UuidUtil.buildRandomUuidString();
        listeners.put(id, listener);
        if (listener instanceof InitialMembershipListener) {
            // TODO: needs sync with membership events...
            final Cluster cluster = client.getCluster();
            ((InitialMembershipListener) listener).init(new InitialMembershipEvent(cluster, cluster.getMembers()));
        }
        return id;
    }

    public String _addMembershipListener(MembershipListener listener) {
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

        // TODO: replace with a better wait-notify
        while (membersRef.get() == null && clusterThread.isAlive()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new HazelcastException(e);
            }
        }
        initMembershipListener();
        active = true;
        // started
    }

    public void stop() {
        active = false;
        clusterThread.shutdown();
    }

    private class ClusterListenerThread extends Thread  {

        private ClusterListenerThread(ThreadGroup group, String name) {
            super(group, name);
        }

        private volatile ClientConnection conn;
        private final List<MemberImpl> members = new LinkedList<MemberImpl>();

        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (conn == null) {
                        try {
                            conn = pickConnection();
                        } catch (Exception e) {
                            logger.severe("Error while connecting to cluster!", e);
                            client.getLifecycleService().shutdown();
                            return;
                        }
                    }
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
                    IOUtil.closeResource(conn);
                    conn = null;
                    fireConnectionEvent(true);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        private ClientConnection pickConnection() throws Exception {
            final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
            if (!members.isEmpty()) {
                for (MemberImpl member : members) {
                    socketAddresses.add(member.getInetSocketAddress());
                }
                Collections.shuffle(socketAddresses);
            }
            socketAddresses.addAll(getConfigAddresses());
            return connectToOne(socketAddresses);
        }

        private void loadInitialMemberList() throws Exception {
            final SerializationService serializationService = getSerializationService();
            final AddMembershipListenerRequest request = new AddMembershipListenerRequest();
            final SerializableCollection coll = (SerializableCollection) sendAndReceive(request, conn);

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
        }

        private void listenMembershipEvents() throws IOException {
            final SerializationService serializationService = getSerializationService();
            while (!Thread.currentThread().isInterrupted()) {
                final Data clientResponseData = conn.read();
                final ClientResponse clientResponse = serializationService.toObject(clientResponseData);
                final Object eventObject = serializationService.toObject(clientResponse.getResponse());
                if (eventObject instanceof ClientMembershipEvent) {
                    final ClientMembershipEvent event = (ClientMembershipEvent) eventObject;
                    final MemberImpl member = (MemberImpl) event.getMember();
                    if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
                        members.add(member);
                    } else {
                        members.remove(member);
    //                    getConnectionManager().removeConnectionPool(member.getAddress()); //TODO
                    }
                    updateMembersRef();
                    logger.info(membersString());
                    fireMembershipEvent(new MembershipEvent(client.getCluster(), member, event.getEventType(),
                            Collections.unmodifiableSet(new LinkedHashSet<Member>(members))));
                } else if (eventObject instanceof ClientMemberAttributeChangedEvent) {
                    ClientMemberAttributeChangedEvent event = (ClientMemberAttributeChangedEvent) eventObject;
                    Map<Address, MemberImpl> memberMap = membersRef.get();
                    if (memberMap != null) {
                        for (MemberImpl member : memberMap.values()) {
                            if (member.getUuid().equals(event.getUuid())) {
                                final MapOperationType operationType = event.getOperationType();
                                final String key = event.getKey();
                                final Object value = event.getValue();
                                member.updateAttribute(operationType, key, value);
                                fireMemberAttributeEvent(new MemberAttributeEvent(client.getCluster(), member, operationType, key, value));
                                break;
                            }
                        }
                    }
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
    }

    private ClientConnection connectToOne(final Collection<InetSocketAddress> socketAddresses) throws Exception {
        final int connectionAttemptLimit = getClientConfig().getConnectionAttemptLimit();
        final ManagerAuthenticator authenticator = new ManagerAuthenticator();
        int attempt = 0;
        Throwable lastError = null;
        while (true) {
            final long nextTry = Clock.currentTimeMillis() + getClientConfig().getConnectionAttemptPeriod();
            for (InetSocketAddress isa : socketAddresses) {
                Address address = new Address(isa);
                try {
                    final ClientConnection connection = getConnectionManager().ownerConnection(address, authenticator);
                    active = true;
                    fireConnectionEvent(false);
                    return connection;
                } catch (IOException e) {
                    active = false;
                    lastError = e;
                    logger.finest("IO error during initial connection...", e);
                } catch (AuthenticationException e) {
                    active = false;
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

    private void fireConnectionEvent(boolean disconnected) {
        final LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) client.getLifecycleService();
        final LifecycleState state = disconnected ? LifecycleState.CLIENT_DISCONNECTED : LifecycleState.CLIENT_CONNECTED;
        lifecycleService.fireLifecycleEvent(state);
    }

    private Collection<InetSocketAddress> getConfigAddresses() {
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
        for (String address : getClientConfig().getAddresses()) {
            socketAddresses.addAll(AddressHelper.getSocketAddresses(address));
        }
//        Collections.shuffle(socketAddresses); //TODO
        return socketAddresses;
    }

    private ClientConfig getClientConfig() {
        return client.getClientConfig();
    }

    public class ManagerAuthenticator implements Authenticator {

        public void auth(ClientConnection connection) throws AuthenticationException, IOException {
            final Object response = authenticate(connection, credentials, principal, true, true);
            principal = (ClientPrincipal) response;
        }
    }

    private class ClusterAuthenticator implements Authenticator {
        public void auth(ClientConnection connection) throws AuthenticationException, IOException {
            authenticate(connection, credentials, principal, false, false);
        }
    }

    private Object authenticate(ClientConnection connection, Credentials credentials, ClientPrincipal principal, boolean reAuth, boolean firstConnection) throws IOException {
        final SerializationService ss = getSerializationService();
        AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
        connection.init();
        auth.setReAuth(reAuth);
        auth.setFirstConnection(firstConnection);
        SerializableCollection coll;
        try {
            coll = (SerializableCollection) sendAndReceive(auth, connection);
        } catch (Exception e) {
            throw new RetryableIOException(e);
        }
        final Iterator<Data> iter = coll.getCollection().iterator();
        if (iter.hasNext()) {
            final Data addressData = iter.next();
            final Address address = ss.toObject(addressData);
            connection.setRemoteEndpoint(address);
            if (iter.hasNext()) {
                final Data principalData = iter.next();
                return ss.toObject(principalData);
            }
        }
        throw new AuthenticationException(); //TODO
    }

    public String membersString() {
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

    //NIO

    public ICompletableFuture send(ClientRequest request) throws IOException {
        final ClientConnection connection = getOrConnect(null);
        return doSend(request, connection, null);
    }

    public ICompletableFuture send(ClientRequest request, Address target) throws IOException {
        final ClientConnection connection = getOrConnect(target);
        return doSend(request, connection, null);
    }

    public ICompletableFuture send(ClientRequest request, ClientConnection connection) throws IOException {
        return doSend(request, connection, null);
    }

    public ICompletableFuture sendAndHandle(ClientRequest request, EventHandler handler) throws IOException {
        final ClientConnection connection = getOrConnect(null);
        return doSend(request, connection, handler);
    }

    public ICompletableFuture sendAndHandle(ClientRequest request, Address target, EventHandler handler) throws IOException {
        final ClientConnection connection = getOrConnect(target);
        return doSend(request, connection, handler);
    }

    private ClientConnection getOrConnect(Address target) throws IOException {
        int count = 0;
        final ClientConnectionManager connectionManager = client.getConnectionManager();
        IOException lastError = null;
        while (count < RETRY_COUNT) {
            try {
                if (target == null || getMember(target) == null) {
                    return connectionManager.getRandomConnection();
                } else {
                    return connectionManager.getOrConnect(target);
                }
            } catch (IOException e) {
                lastError = e;
            }
            target = null;
            count++;
        }
        throw lastError;
    }

    public void registerListener(String uuid, int callId) {
        registrationAliasMap.put(uuid, uuid);
        registrationIdMap.put(uuid, callId);
    }

    public void reRegisterListener(String uuid, String alias, int callId) {
        final String oldAlias = registrationAliasMap.put(uuid, alias);
        if (oldAlias != null) {
            registrationIdMap.remove(oldAlias);
            registrationIdMap.put(alias, callId);
        }
    }

    public boolean deRegisterListener(String uuid) {
        final String alias = registrationAliasMap.remove(uuid);
        if (alias != null) {
            final Integer callId = registrationIdMap.remove(alias);
            for (ConcurrentMap<Integer, ClientCallFuture> eventHandlerMap : connectionEventHandlerMap.values()) {
                if (eventHandlerMap.remove(callId) != null) {
                    return true;
                }
            }
        }
        return false;
    }

    private Object sendAndReceive(ClientRequest request, ClientConnection connection) throws Exception {
        final SerializationService ss = getSerializationService();
        connection.write(ss.toData(request));
        final Data data = connection.read();
        ClientResponse clientResponse = ss.toObject(data);
        Object response = ss.toObject(clientResponse.getResponse());
        if (response instanceof Throwable) {
            Throwable t = (Throwable) response;
            ExceptionUtil.fixRemoteStackTrace(t, Thread.currentThread().getStackTrace());
            throw new Exception(t);
        }
        return response;
    }

    public Future reSend(ClientCallFuture future) throws IOException {
        final ClientConnection connection = getOrConnect(null);
        _send(future, connection);
        return future;
    }

    private ICompletableFuture doSend(ClientRequest request, ClientConnection connection, EventHandler handler) {
        final ClientCallFuture future = new ClientCallFuture(client, connection, request, handler);
        _send(future, connection);
        return future;
    }

    public void _send(ClientCallFuture future, ClientConnection connection) {
        registerCall(future, connection);
        final SerializationService ss = getSerializationService();
        final Data data = ss.toData(future.getRequest());
        if (!connection.write(new DataAdapter(data))) {
            future.notify(new TargetNotMemberException("Address : " + connection.getRemoteEndpoint()));
            connectionCallMap.remove(connection);
        }
    }

    private void registerCall(ClientCallFuture future, ClientConnection connection) {
        final int callId = callIdIncrementer.incrementAndGet();
        future.getRequest().setCallId(callId);
        ConcurrentMap<Integer, ClientCallFuture> callIdMap = connectionCallMap.get(connection);
        if (callIdMap == null) {
            callIdMap = new ConcurrentHashMap<Integer, ClientCallFuture>();
            final ConcurrentMap<Integer, ClientCallFuture> current = connectionCallMap.putIfAbsent(connection, callIdMap);
            if (current != null) {
                callIdMap = current;
            }
        }
        callIdMap.put(callId, future);
        if (future.getHandler() != null) {
            registerEventHandler(future, connection);
        }
    }

    private void registerEventHandler(ClientCallFuture future, ClientConnection connection) {
        ConcurrentMap<Integer, ClientCallFuture> eventHandlerMap = connectionEventHandlerMap.get(connection);
        if (eventHandlerMap == null) {
            eventHandlerMap = new ConcurrentHashMap<Integer, ClientCallFuture>();
            final ConcurrentMap<Integer, ClientCallFuture> current = connectionEventHandlerMap.putIfAbsent(connection, eventHandlerMap);
            if (current != null) {
                eventHandlerMap = current;
            }
        }
        eventHandlerMap.put(future.getRequest().getCallId(), future);
    }

    private ClientCallFuture deRegisterCall(ClientConnection connection, int callId) {
        final ConcurrentMap<Integer, ClientCallFuture> callIdMap = connectionCallMap.get(connection);
        if (callIdMap == null) {
            return null;
        }
        return callIdMap.remove(callId);
    }

    public void removeConnectionCalls(ClientConnection connection) {
        final HazelcastException response;
        if (active) {
            ((ClientPartitionServiceImpl) client.getClientPartitionService()).refreshPartitions();
            response = new TargetDisconnectedException(connection.getRemoteEndpoint());
        } else {
            response = new HazelcastException("Client is shutting down!!!");
        }
        final ConcurrentMap<Integer, ClientCallFuture> callIdMap = connectionCallMap.remove(connection);
        final ConcurrentMap<Integer, ClientCallFuture> eventHandlerMap = connectionEventHandlerMap.remove(connection);
        if (callIdMap != null) {
            for (Map.Entry<Integer, ClientCallFuture> entry : callIdMap.entrySet()) {
                if (eventHandlerMap != null) {
                    eventHandlerMap.remove(entry.getKey());
                }
                entry.getValue().notify(response);
            }
            callIdMap.clear();
        }
        if (eventHandlerMap != null) {
            for (ClientCallFuture future : eventHandlerMap.values()) {
                future.notify(response);
            }
            eventHandlerMap.clear();
        }
    }

    public void handlePacket(ClientPacket packet) {
        client.getClientExecutionService().execute(new ClientPacketProcessor(packet));
    }

    class ClientPacketProcessor implements Runnable {

        ClientPacket packet;

        ClientPacketProcessor(ClientPacket packet) {
            this.packet = packet;
        }

        public void run() {
            final ClientConnection conn = (ClientConnection) packet.getConn();
            final ClientResponse clientResponse = getSerializationService().toObject(packet.getData());
            final int callId = clientResponse.getCallId();
            final Object response = clientResponse.getResponse();
            final boolean event = clientResponse.isEvent();
            if (event) {
                handleEvent(response, callId, conn);
            } else {
                handlePacket(response, callId, conn);
            }
        }

        private void handlePacket(Object response, int callId, ClientConnection conn) {
            final ClientCallFuture future = deRegisterCall(conn, callId);
            if (future == null) {
                logger.warning("No call for callId: " + callId + ", response: " + response);
                return;
            }
            future.notify(response);
        }

        private void handleEvent(Object event, int callId, ClientConnection conn) {
            final ConcurrentMap<Integer, ClientCallFuture> eventHandlerMap = connectionEventHandlerMap.get(conn);
            if (eventHandlerMap != null) {
                final ClientCallFuture future = eventHandlerMap.get(callId);
                if (future != null && future.getHandler() != null) {
                    future.getHandler().handle(getSerializationService().toObject(event));
                    return;
                }
            }
            logger.warning("No eventHandler for callId: " + callId + ", event: " + event + ", conn: " + conn);
        }
    }
}
