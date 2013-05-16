package com.hazelcast.client.spi.impl;

import com.hazelcast.client.AuthenticationRequest;
import com.hazelcast.client.ClientPrincipal;
import com.hazelcast.client.GenericError;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.Connection;
import com.hazelcast.client.exception.AuthenticationException;
import com.hazelcast.client.exception.ClusterClientException;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ResponseStream;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.cluster.client.AddMembershipListenerRequest;
import com.hazelcast.cluster.client.ClientMembershipEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @mdogan 5/15/13
 */
public final class ClientClusterServiceImpl implements ClientClusterService {

    private final HazelcastClient client;
    private final ClusterListenerThread clusterThread;
    private final AtomicReference<Map<Address, MemberImpl>> membersRef = new AtomicReference<Map<Address, MemberImpl>>();
    private final ConcurrentMap<String, MembershipListener> listeners = new ConcurrentHashMap<String, MembershipListener>();

    private final Credentials credentials;
    private volatile ClientPrincipal principal;

    public ClientClusterServiceImpl(HazelcastClient client) {
        this.client = client;
        clusterThread = new ClusterListenerThread(client.getThreadGroup(), client.getName() + ".cluster-listener");
        credentials = getClientConfig().getCredentials();
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

    public Set<Member> getMembers() {
        final Map<Address, MemberImpl> members = membersRef.get();
        return members != null ? new LinkedHashSet<Member>(members.values()) : Collections.<Member>emptySet();
    }

    public Address getMasterAddress() {
        final Collection<MemberImpl> memberList = getMemberList();
        return !memberList.isEmpty() ? memberList.iterator().next().getAddress() : null;
    }

    public boolean isMaster() {
        return false;
    }

    public Address getThisAddress() {
        throw new UnsupportedOperationException();
    }

    public int getSize() {
        return getMemberList().size();
    }

    public long getClusterTime() {
        return Clock.currentTimeMillis();
    }

    public <T> T sendAndReceive(Object obj) throws IOException {
        final Connection conn = getConnectionManager().getRandomConnection();
        try {
            return sendAndReceive(conn, obj);
        } finally {
            conn.close();
        }
    }

    public <T> T sendAndReceive(Address address, Object obj) throws IOException {
        final Connection conn = getConnectionManager().getConnection(address);
        try {
            return sendAndReceive(conn, obj);
        } finally {
            conn.close();
        }
    }

    private <T> T sendAndReceive(Connection conn, Object obj) throws IOException {
        final SerializationService serializationService = getSerializationService();
        final Data request = serializationService.toData(obj);
        conn.write(request);
        final Data response = conn.read();
        return (T) serializationService.toObject(response);
    }

    private SerializationService getSerializationService() {
        return client.getSerializationService();
    }

    private ClientConnectionManager getConnectionManager() {
        return client.getConnectionManager();
    }

    public ResponseStream sendAndStream(Address address, Object obj) {

        return null;
    }

    public Authenticator getAuthenticator() {
        return new ClusterAuthenticator();
    }

    public String addMembershipListener(MembershipListener listener) {
        final String id = UUID.randomUUID().toString();
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
        } catch (Exception e) {
            throw new ClusterClientException(e);
        }
        clusterThread.start();

        // TODO: replace with a better wait-notify
        while (membersRef.get() == null) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new ClusterClientException(e);
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

        private Connection conn;
        private final List<MemberImpl> members = new LinkedList<MemberImpl>();

        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (conn == null) {
                        conn = pickConnection();
                        System.err.println("Connected: " + conn);
                    }
                    loadInitialMemberList();
                    listenMembershipEvents();
                } catch (Exception e) {
                    if (client.getLifecycleService().isRunning()) {
                        e.printStackTrace();
                    }
                    System.err.println(conn + " FAILED...");
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
            final List<InetSocketAddress> addresses = new LinkedList<InetSocketAddress>();
            if (!members.isEmpty()) {
                addresses.addAll(getClusterAddresses());
            }
            addresses.addAll(getConfigAddresses());
            System.err.println("Possible addresses: " + addresses);
            Collections.shuffle(addresses);
            return connectToOne(addresses);
        }

        private void loadInitialMemberList() throws IOException {
            SerializableCollection coll = sendAndReceive(conn, new AddMembershipListenerRequest());
            final SerializationService serializationService = getSerializationService();
            members.clear();
            for (Data d : coll.getCollection()) {
                members.add((MemberImpl) serializationService.toObject(d));
            }
            System.err.println("members = " + members);
            updateMembersRef();
        }

        private void listenMembershipEvents() throws IOException {
            final SerializationService serializationService = getSerializationService();
            while (!Thread.currentThread().isInterrupted()) {
                final Data eventData = conn.read();
                final ClientMembershipEvent event = (ClientMembershipEvent) serializationService.toObject(eventData);
                System.err.println(event);
                if (event.isAdded()) {
                    members.add(event.getMember());
                } else {
                    members.remove(event.getMember());
                }
                // call membership listeners...
                updateMembersRef();
            }
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
                    e.printStackTrace();
                }
            }
        }
    }

    private Connection connectToOne(final Collection<InetSocketAddress> socketAddresses) throws Exception {
        final int connectionAttemptLimit = getClientConfig().getConnectionAttemptLimit();
        final ManagerAuthenticator authenticator = new ManagerAuthenticator();
        int attempt = 0;
        while (true) {
            final long nextTry = Clock.currentTimeMillis() + getClientConfig().getAttemptPeriod();
            for (InetSocketAddress isa : socketAddresses) {
                try {
                    Address address = new Address(isa);
                    System.err.println("Trying to connect: " + address);
                    return getConnectionManager().newConnection(address, authenticator);
                } catch (IOException ignored) {
                }
            }
            if (attempt++ >= connectionAttemptLimit) {
                break;
            }
            final long remainingTime = nextTry - Clock.currentTimeMillis();
            System.err.println(
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
        throw new IllegalStateException("Unable to connect to any address in the config!");
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
            final Object response = authenticate(connection, credentials, principal, true);
            principal = (ClientPrincipal) response;
        }
    }

    private class ClusterAuthenticator implements Authenticator {
        public void auth(Connection connection) throws AuthenticationException, IOException {
            authenticate(connection, credentials, principal, false);
        }
    }

    private Object authenticate(Connection connection, Credentials credentials, ClientPrincipal principal, boolean reAuth) throws IOException {
        AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
        auth.setReAuth(reAuth);
        final SerializationService serializationService = getSerializationService();
        connection.write(serializationService.toData(auth));
        final Data data = connection.read();
        Object response = serializationService.toObject(data);
        if (response instanceof GenericError) {
            throw new AuthenticationException(((GenericError) response).getMessage());
        }
        System.err.println("principal = " + response);
        return response;
    }

}
