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

import com.hazelcast.client.*;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.Connection;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.ResponseHandler;
import com.hazelcast.client.spi.ResponseStream;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.client.util.ErrorHandler;
import com.hazelcast.cluster.client.AddMembershipListenerRequest;
import com.hazelcast.cluster.client.ClientMembershipEvent;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.*;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataAdapter;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.core.LifecycleEvent.LifecycleState;


/**
 * @author mdogan 5/15/13
 */
public final class ClientClusterServiceImpl implements ClientClusterService {

    private static final ILogger logger = Logger.getLogger(ClientClusterService.class);
    private static int RETRY_COUNT = 20;
    private static int RETRY_WAIT_TIME = 500;

    private final HazelcastClient client;
    private final ClusterListenerThread clusterThread;
    private final AtomicReference<Map<Address, MemberImpl>> membersRef = new AtomicReference<Map<Address, MemberImpl>>();
    private final ConcurrentMap<String, MembershipListener> listeners = new ConcurrentHashMap<String, MembershipListener>();

    private final boolean redoOperation;
    private final Credentials credentials;
    private volatile ClientPrincipal principal;
    private volatile boolean active = false;

    private final AtomicLong callIdIncrementer = new AtomicLong();
    private final ConcurrentMap<Long, ClientCallFuture> callMap = new ConcurrentHashMap<Long, ClientCallFuture>();
    private final ConcurrentMap<Long, EventHandler> eventHandlerMap = new ConcurrentHashMap<Long, EventHandler>();
    private final ConcurrentMap<String, Long> registrationIdMap = new ConcurrentHashMap<String, Long>();

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
                    addMembershipListener((MembershipListener) listener);
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
        ClientPrincipal cp = principal;
        ClientConnection conn = clusterThread.conn;
        return new ClientImpl(cp != null ? cp.getUuid() : null, conn != null ? conn.getLocalSocketAddress() : null);
    }

    private interface ConnectionFactory {
        Connection create() throws IOException;
    }

    private <T> T _sendAndReceive(ConnectionFactory connectionFactory, Object obj) throws IOException {
        while (active) {
            Connection conn = null;
            boolean release = true;
            try {
                conn = connectionFactory.create();
                final SerializationService serializationService = getSerializationService();
                final Data request = serializationService.toData(obj);
                conn.write(request);
                final Data response = conn.read();
                final Object result = serializationService.toObject(response);
                return ErrorHandler.returnResultOrThrowException(result);
            } catch (Exception e) {
                if (e instanceof IOException) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Error on connection... conn: " + conn + ", error: " + e);
                    }
                    IOUtil.closeResource(conn);
                    release = false;
                }
                if (ErrorHandler.isRetryable(e)) {
                    if (redoOperation || obj instanceof RetryableRequest) {
                        if (logger.isFinestEnabled()) {
                            logger.finest("Retrying " + obj + ", last-conn: " + conn + ", last-error: " + e);
                        }
                        beforeRetry();
                        continue;
                    }
                }
                if (e instanceof IOException && !active) {
                    continue;
                }
                throw ExceptionUtil.rethrow(e, IOException.class);
            } finally {
                if (release && conn != null) {
                    conn.release();
                }
            }
        }
        throw new HazelcastInstanceNotActiveException();
    }

    public <T> T sendAndReceiveFixedConnection(Connection conn, Object obj) throws IOException {
        final SerializationService serializationService = getSerializationService();
        final Data request = serializationService.toData(obj);
        conn.write(request);
        final Data response = conn.read();
        final Object result = serializationService.toObject(response);
        return ErrorHandler.returnResultOrThrowException(result);
    }

    private SerializationService getSerializationService() {
        return client.getSerializationService();
    }

    private ClientConnectionManager getConnectionManager() {
        return client.getConnectionManager();
    }

    private Connection getRandomConnection() throws IOException {
        return getConnection(null);
    }

    private Connection getConnection(Address address) throws IOException {
        if (!client.getLifecycleService().isRunning()) {
            throw new HazelcastInstanceNotActiveException();
        }
        Connection connection = null;
//        int retryCount = RETRY_COUNT;
//        while (connection == null && retryCount > 0) {
//            if (address != null) {
//                connection = client.getConnectionManager().getConnection(address);
//                address = null;
//            } else {
//                connection = client.getConnectionManager().getRandomConnection();
//            }
//            if (connection == null) {
//                retryCount--;
//                beforeRetry();
//            }
//        }
//        if (connection == null) {
//            throw new IOException("Unable to connect to " + address);
//        }
        return connection;
    }

    private void beforeRetry() {
        try {
            Thread.sleep(RETRY_WAIT_TIME);
            ((ClientPartitionServiceImpl) client.getClientPartitionService()).refreshPartitions();
        } catch (InterruptedException ignored) {
        }
    }

    private void _sendAndHandle(ConnectionFactory connectionFactory, Object obj, ResponseHandler handler) throws IOException {
        ResponseStream stream = null;
        while (stream == null) {
            if (!active) {
                throw new HazelcastInstanceNotActiveException();
            }
            Connection conn = null;
            try {
                conn = connectionFactory.create();
                final SerializationService serializationService = getSerializationService();
                final Data request = serializationService.toData(obj);
                conn.write(request);
                stream = new ResponseStreamImpl(serializationService, conn);
            } catch (Exception e) {
                if (e instanceof IOException) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Error on connection... conn: " + conn + ", error: " + e);
                    }
                }
                if (conn != null) {
                    IOUtil.closeResource(conn);
                }
                if (ErrorHandler.isRetryable(e)) {
                    if (redoOperation || obj instanceof RetryableRequest) {
                        if (logger.isFinestEnabled()) {
                            logger.finest("Retrying " + obj + ", last-conn: " + conn + ", last-error: " + e);
                        }
                        beforeRetry();
                        continue;
                    }
                }
                if (e instanceof IOException && !active) {
                    continue;
                }
                throw ExceptionUtil.rethrow(e, IOException.class);
            }
        }

        try {
            handler.handle(stream);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e, IOException.class);
        } finally {
            stream.end();
        }
    }

    public Authenticator getAuthenticator() {
        return new ClusterAuthenticator();
    }

    public String addMembershipListener(MembershipListener listener) {
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

    private class ClusterListenerThread extends Thread implements EventHandler<ClientMembershipEvent> {

        private ClusterListenerThread(ThreadGroup group, String name) {
            super(group, name);
        }

        private volatile ClientConnection conn;
        private final List<MemberImpl> members = new LinkedList<MemberImpl>();

        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                String registrationId = null;
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
                    registrationId = loadInitialMemberList();
                } catch (Exception e) {
                    if (client.getLifecycleService().isRunning()) {
                        if (logger.isFinestEnabled()) {
                            logger.warning("Error while listening cluster events! -> " + conn, e);
                        } else {
                            logger.warning("Error while listening cluster events! -> " + conn + ", Error: " + e.toString());
                        }
                    }
                    if (registrationId != null) {
                        deRegisterListener(registrationId);
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

        private String loadInitialMemberList() throws IOException, ExecutionException, InterruptedException {
            final SerializationService serializationService = getSerializationService();
            final AddMembershipListenerRequest request = new AddMembershipListenerRequest();

            final ClientCallFuture future = innerSendAndHandle(request, conn, this);
            Object response = future.get();
            SerializableCollection coll = ErrorHandler.returnResultOrThrowException(response);

            Map<String, MemberImpl> prevMembers = Collections.emptyMap();
            if (!members.isEmpty()) {
                prevMembers = new HashMap<String, MemberImpl>(members.size());
                for (MemberImpl member : members) {
                    prevMembers.put(member.getUuid(), member);
                }
                members.clear();
            }
            final Iterator<Data> iter = coll.getCollection().iterator();
            String registrationId = null;
            if (iter.hasNext()) {
                registrationId = serializationService.toObject(iter.next());
            }
            while (iter.hasNext()) {
                members.add((MemberImpl) serializationService.toObject(iter.next()));
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
            return registrationId;
        }

        public void handle(ClientMembershipEvent event) {
            final MemberImpl member = (MemberImpl) event.getMember();
            if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
                members.add(member);
            } else {
                members.remove(member);
//                    getConnectionManager().removeConnectionPool(member.getAddress());// TODO
            }
            updateMembersRef();
            logger.info(membersString());
            fireMembershipEvent(new MembershipEvent(client.getCluster(), member, event.getEventType(),
                    Collections.unmodifiableSet(new LinkedHashSet<Member>(members))));
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
                    final ClientConnection connection = getConnectionManager().firstConnection(address, authenticator);
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
//        Collections.shuffle(socketAddresses);
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
        auth.setReAuth(reAuth);
        auth.setFirstConnection(firstConnection);
        final ClientCallFuture future = innerSend(auth, connection);

        Object result;
        try {
            result = future.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
        final SerializableCollection coll = ErrorHandler.returnResultOrThrowException(result);
        final Iterator<Data> iter = coll.getCollection().iterator();
        if (iter.hasNext()) {
            final Data addressData = iter.next();
            final Address address = (Address) ss.toObject(addressData);
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

    public long registerCall(ClientCallFuture future) {
        final long callId = callIdIncrementer.incrementAndGet();
        callMap.put(callId, future);
        return callId;
    }

    public boolean deRegisterCall(long callId) {
        return callMap.remove(callId) != null;
    }

    public boolean send(ClientRequest request) throws IOException {
        final ClientConnection connection = client.getConnectionManager().getRandomConnection();
        return _send(request, connection);
    }

    public boolean send(ClientRequest request, Address target) throws IOException {
        final ClientConnection connection = client.getConnectionManager().getOrConnect(target);
        return _send(request, connection);
    }

    public boolean sendAndHandle(ClientRequest request, EventHandler handler) throws IOException {
        final ClientConnection connection = client.getConnectionManager().getRandomConnection();
        return _sendAndHandle(request, connection, handler);
    }

    public boolean sendAndHandle(ClientRequest request, Address target, EventHandler handler) throws IOException {
        final ClientConnection connection = client.getConnectionManager().getOrConnect(target);
        return _sendAndHandle(request, connection, handler);
    }

    public void registerListener(String uuid, long callId) {
        registrationIdMap.put(uuid, callId);
    }

    public boolean deRegisterListener(String uuid) {
        return registrationIdMap.remove(uuid) != null;
    }

    private boolean _send(ClientRequest request, ClientConnection connection) {
        final SerializationService ss = getSerializationService();
        final Data data = ss.toData(request);
        return connection.write(new DataAdapter(data)); //TODO serContext?
    }

    private boolean _sendAndHandle(ClientRequest request, ClientConnection connection, EventHandler handler) {
        final SerializationService ss = getSerializationService();
        final Data data = ss.toData(request);
        eventHandlerMap.put(request.getCallId(), handler);
        return connection.write(new DataAdapter(data)); //TODO serContext?
    }

    private ClientCallFuture innerSend(ClientRequest request, ClientConnection connection) {
        final ClientCallFuture future = new ClientCallFuture();
        final long callId = registerCall(future);
        request.setCallId(callId);
        _send(request, connection);
        return future;
    }

    private ClientCallFuture innerSendAndHandle(ClientRequest request, ClientConnection connection, EventHandler handler) {
        final ClientCallFuture future = new ClientCallFuture();
        final long callId = registerCall(future);
        request.setCallId(callId);
        _sendAndHandle(request, connection, handler);
        return future;
    }

    public void handlePacket(DataAdapter packet) {
        client.getClientExecutionService().execute(new ClientPacketProcessor(packet));
    }

    class ClientPacketProcessor implements Runnable {

        DataAdapter packet;

        ClientPacketProcessor(DataAdapter packet) {
            this.packet = packet;
        }

        public void run() {
            final ClientResponse clientResponse = getSerializationService().toObject(packet.getData());
            final long callId = clientResponse.getCallId();
            final Object response = clientResponse.getResponse();
            final boolean event = clientResponse.isEvent();
            if (event) {
                handleEvent(response, callId);
            } else {
                handlePacket(response, callId);
            }
        }

        private void handlePacket(Object response, long callId){
            final ClientCallFuture future = callMap.remove(callId);
            if (future == null) {
                logger.warning("No call for callId: " + callId + ", response: " + response);
                return;
            }
            future.setResponse(response);
        }

        private void handleEvent(Object event, long callId){
            final EventHandler eventHandler = eventHandlerMap.get(callId);
            if (eventHandler == null) {
                logger.warning("No eventHandler for callId: " + callId + ", event: " + event);
                return;
            }
            eventHandler.handle(event);
        }
    }
}
