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
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ResponseHandler;
import com.hazelcast.client.spi.ResponseStream;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.client.util.ErrorHandler;
import com.hazelcast.cluster.client.AddMembershipListenerRequest;
import com.hazelcast.cluster.client.ClientMembershipEvent;
import com.hazelcast.core.*;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.util.ExceptionUtil.fixRemoteStackTrace;

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

    public ClientClusterServiceImpl(HazelcastClient client) {
        this.client = client;
        clusterThread = new ClusterListenerThread(client.getThreadGroup(), client.getName() + ".cluster-listener");
        final ClientConfig clientConfig = getClientConfig();
        redoOperation = clientConfig.isRedoOperation();
        credentials = clientConfig.getCredentials();
        final Collection<EventListener> listenersList = client.getClientConfig().getListeners();
        if (listenersList != null && !listenersList.isEmpty()) {
            for (EventListener listener : listenersList) {
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

    <T> T sendAndReceive(Object obj) throws IOException {
        return _sendAndReceive(randomConnectionFactory, obj);
    }

    <T> T sendAndReceive(final Address address, Object obj) throws IOException {
        return _sendAndReceive(new TargetConnectionFactory(address), obj);
    }

    private interface ConnectionFactory {
        Connection create() throws IOException;
    }

    private final ConnectionFactory randomConnectionFactory = new ConnectionFactory() {
        public Connection create() throws IOException {
            return getRandomConnection();
        }
    };

    private class TargetConnectionFactory implements ConnectionFactory {
        final Address target;

        private TargetConnectionFactory(Address target) {
            this.target = target;
        }

        public Connection create() throws IOException {
            return getConnection(target);
        }
    }

    private <T> T _sendAndReceive(ConnectionFactory connectionFactory, Object obj) throws IOException {
        while (true) {
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
                        logger.finest( "Error on connection... conn: " + conn + ", error: " + e);
                    }
                    IOUtil.closeResource(conn);
                    release = false;
                }
                if (ErrorHandler.isRetryable(e)) {
                    if (redoOperation || obj instanceof RetryableRequest) {
                        if (logger.isFinestEnabled()) {
                            logger.finest( "Retrying " + obj + ", last-conn: " + conn + ", last-error: " + e);
                        }
                        beforeRetry();
                        continue;
                    }
                }
                throw ExceptionUtil.rethrow(e, IOException.class);
            } finally {
                if (release && conn != null) {
                    conn.release();
                }
            }
        }
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
        int retryCount = RETRY_COUNT;
        while (connection == null && retryCount > 0) {
            if (address != null) {
                connection = client.getConnectionManager().getConnection(address);
                address = null;
            } else {
                connection = client.getConnectionManager().getRandomConnection();
            }
            if (connection == null) {
                retryCount--;
                beforeRetry();
            }
        }
        if (connection == null) {
            throw new IOException("Unable to connect to " + address);
        }
        return connection;
    }

    private void beforeRetry() {
        try {
            Thread.sleep(RETRY_WAIT_TIME);
            ((ClientPartitionServiceImpl) client.getClientPartitionService()).refreshPartitions();
        } catch (InterruptedException ignored) {
        }
    }

    void sendAndHandle(final Address address, Object obj, ResponseHandler handler) throws IOException {
        _sendAndHandle(new TargetConnectionFactory(address), obj, handler);
    }

    void sendAndHandle(Object obj, ResponseHandler handler) throws IOException {
        _sendAndHandle(randomConnectionFactory, obj, handler);
    }

    private void _sendAndHandle(ConnectionFactory connectionFactory, Object obj, ResponseHandler handler) throws IOException {
        ResponseStream stream = null;
        while (stream == null) {
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
                        logger.finest( "Error on connection... conn: " + conn + ", error: " + e);
                    }
                }
                if (conn != null) {
                    IOUtil.closeResource(conn);
                }
                if (ErrorHandler.isRetryable(e)) {
                    if (redoOperation || obj instanceof RetryableRequest) {
                        if (logger.isFinestEnabled()) {
                            logger.finest( "Retrying " + obj + ", last-conn: " + conn + ", last-error: " + e);
                        }
                        beforeRetry();
                        continue;
                    }
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
        if (listener instanceof InitialMembershipListener) {
            // TODO: needs sync with membership events...
            final Cluster cluster = client.getCluster();
            ((InitialMembershipListener) listener).init(new InitialMembershipEvent(cluster, cluster.getMembers()));
        }
        listeners.put(id, listener);
        return id;
    }

    public boolean removeMembershipListener(String registrationId) {
        return listeners.remove(registrationId) != null;
    }

    public void start() {
        final Future<Connection> f = client.getClientExecutionService().submit(new InitialConnectionCall());
        try {
            final Connection connection = f.get(30, TimeUnit.SECONDS);
            clusterThread.setInitialConn(connection);
        } catch (Throwable e) {
            if (e instanceof ExecutionException && e.getCause() != null) {
                e = e.getCause();
                fixRemoteStackTrace(e, Thread.currentThread().getStackTrace());
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new HazelcastException(e);
        }
        clusterThread.start();

        // TODO: replace with a better wait-notify
        while (membersRef.get() == null) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new HazelcastException(e);
            }
        }
        // started
    }

    public void stop() {
        clusterThread.shutdown();
    }


    private class InitialConnectionCall implements Callable<Connection> {

        public Connection call() throws Exception {
            return connectToOne(getConfigAddresses());
        }
    }

    private class ClusterListenerThread extends Thread {

        private ClusterListenerThread(ThreadGroup group, String name) {
            super(group, name);
        }

        private volatile Connection conn;
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
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        private Connection pickConnection() throws Exception {
            final Collection<InetSocketAddress> addresses = new HashSet<InetSocketAddress>();
            if (!members.isEmpty()) {
                addresses.addAll(getClusterAddresses());
            }
            addresses.addAll(getConfigAddresses());
            return connectToOne(addresses);
        }

        private void loadInitialMemberList() throws IOException {
            final SerializationService serializationService = getSerializationService();
            final Data request = serializationService.toData(new AddMembershipListenerRequest());
            conn.write(request);
            final Data response = conn.read();
            SerializableCollection coll = ErrorHandler.returnResultOrThrowException(serializationService.toObject(response));

            Map<String, MemberImpl> prevMembers = Collections.emptyMap();
            if (!members.isEmpty()) {
                prevMembers = new HashMap<String, MemberImpl>(members.size());
                for (MemberImpl member : members) {
                    prevMembers.put(member.getUuid(), member);
                }
                members.clear();
            }
            for (Data d : coll.getCollection()) {
                members.add((MemberImpl) serializationService.toObject(d));
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
                final Data eventData = conn.read();
                final ClientMembershipEvent event = (ClientMembershipEvent) serializationService.toObject(eventData);
                final MemberImpl member = (MemberImpl) event.getMember();
                if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
                    members.add(member);
                } else {
                    members.remove(member);
                    getConnectionManager().removeConnectionPool(member.getAddress());
                }
                updateMembersRef();
                logger.info(membersString());
                fireMembershipEvent(new MembershipEvent(client.getCluster(), member, event.getEventType(),
                        Collections.unmodifiableSet(new LinkedHashSet<Member>(members))));
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

        private void updateMembersRef() {
            final Map<Address, MemberImpl> map = new LinkedHashMap<Address, MemberImpl>(members.size());
            for (MemberImpl member : members) {
                map.put(member.getAddress(), member);
            }
            membersRef.set(Collections.unmodifiableMap(map));
        }

        private Collection<InetSocketAddress> getClusterAddresses() {
            final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
            for (MemberImpl member : members) {
                socketAddresses.add(member.getInetSocketAddress());
            }
            Collections.shuffle(socketAddresses);
            return socketAddresses;
        }

        void setInitialConn(Connection conn) {
            this.conn = conn;
        }

        void shutdown() {
            interrupt();
            final Connection c = conn;
            if (c != null) {
                try {
                    c.close();
                } catch (IOException e) {
                    logger.warning("Error while closing connection!", e);
                }
            }
        }
    }

    private Connection connectToOne(final Collection<InetSocketAddress> socketAddresses) throws Exception {
        final int connectionAttemptLimit = getClientConfig().getConnectionAttemptLimit();
        final ManagerAuthenticator authenticator = new ManagerAuthenticator();
        int attempt = 0;
        Throwable lastError = null;
        while (true) {
            final long nextTry = Clock.currentTimeMillis() + getClientConfig().getConnectionAttemptPeriod();
            for (InetSocketAddress isa : socketAddresses) {
                Address address = new Address(isa);
                try {
                    return getConnectionManager().firstConnection(address, authenticator);
                } catch (IOException e) {
                    lastError = e;
                    logger.finest( "IO error during initial connection...", e);
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

    private Collection<InetSocketAddress> getConfigAddresses() {
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
        for (String address : getClientConfig().getAddressList()) {
            socketAddresses.addAll(AddressHelper.getSocketAddresses(address));
        }
        Collections.shuffle(socketAddresses);
        return socketAddresses;
    }

    private ClientConfig getClientConfig() {
        return client.getClientConfig();
    }

    private class ManagerAuthenticator implements Authenticator {
        public void auth(Connection connection) throws AuthenticationException, IOException {
            final Object response = authenticate(connection, credentials, principal, true, true);
            principal = (ClientPrincipal) response;
        }
    }

    private class ClusterAuthenticator implements Authenticator {
        public void auth(Connection connection) throws AuthenticationException, IOException {
            authenticate(connection, credentials, principal, false, false);
        }
    }

    private Object authenticate(Connection connection, Credentials credentials, ClientPrincipal principal, boolean reAuth, boolean firstConnection) throws IOException {
        AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
        auth.setReAuth(reAuth);
        auth.setFirstConnection(firstConnection);
        final SerializationService serializationService = getSerializationService();
        connection.write(serializationService.toData(auth));
        final Data addressData = connection.read();
        Address address = ErrorHandler.returnResultOrThrowException(serializationService.toObject(addressData));
        connection.setEndpoint(address);

        final Data data = connection.read();
        return ErrorHandler.returnResultOrThrowException(serializationService.toObject(data));
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

}
