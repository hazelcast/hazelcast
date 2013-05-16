package com.hazelcast.client.spi;

import com.hazelcast.client.AuthenticationRequest;
import com.hazelcast.client.ClientPrincipal;
import com.hazelcast.client.GenericError;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.Connection;
import com.hazelcast.client.connection.ConnectionManager;
import com.hazelcast.client.exception.AuthenticationException;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.cluster.client.AddMembershipListenerRequest;
import com.hazelcast.cluster.client.ClientMembershipEvent;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @mdogan 5/15/13
 */
public final class ClusterService {

    private final HazelcastClient client;
    private final ClusterThread clusterThread;
    private final Authenticator authenticator;
    private final AtomicReference<Set<Member>> membersRef = new AtomicReference<Set<Member>>();
    private final ConcurrentMap<String, MembershipListener> listeners = new ConcurrentHashMap<String, MembershipListener>();

    public ClusterService(HazelcastClient client) {
        this.client = client;
        clusterThread = new ClusterThread();
        authenticator = new ClusterAuthenticator();
    }

    public Object sendAndReceive(Address address, Object obj) throws IOException {
        final Connection conn = getConnectionManager().getConnection(address);
        try {
            return sendAndReceive(conn, obj);
        } finally {
            conn.close();
        }
    }

    public Object sendAndReceive(Connection conn, Object obj) throws IOException {
        final SerializationServiceImpl serializationService = getSerializationService();
        final Data request = serializationService.toData(obj);
        conn.write(request);
        final Data response = conn.read();
        return serializationService.toObject(response);
    }

    private SerializationServiceImpl getSerializationService() {
        return client.getSerializationService();
    }

    private ConnectionManager getConnectionManager() {
        return client.getConnectionManager();
    }

    public ResponseStream sendAndStream(Address address, Object obj) {

        return null;
    }

    public Authenticator getAuthenticator() {
        return authenticator;
    }

    public Set<Member> getMembers() {
        final Set<Member> members = membersRef.get();
        return members != null ? members : Collections.<Member>emptySet();
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

    class ClusterThread extends Thread {

        private Connection conn;
        private final List<MemberImpl> members = new LinkedList<MemberImpl>();


        public void run() {
            // TODO: improve member selection logic...
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (!members.isEmpty()) {
                        conn = connect(getClusterAddresses());
                    } else {
                        conn = connect(getConfigAddresses());
                    }

                    SerializableCollection coll = (SerializableCollection) sendAndReceive(conn, new AddMembershipListenerRequest());
                    final SerializationServiceImpl serializationService = getSerializationService();
                    members.clear();
                    for (Data d : coll.getCollection()) {
                        members.add((MemberImpl) serializationService.toObject(d));
                    }
                    System.err.println("members = " + members);
                    updateMembersRef();

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
                } catch (AuthenticationException e) {
                    throw e;
                } catch (Exception e) {
                    if (client.getLifecycleService().isRunning()) {
                        e.printStackTrace();
                    }
                    IOUtil.closeResource(conn);
                }
            }
        }

        private void updateMembersRef() {
            membersRef.set(Collections.<Member>unmodifiableSet(new LinkedHashSet<Member>(members)));
        }

        private Connection connect(final Collection<InetSocketAddress> socketAddresses) throws Exception {
            final int connectionAttemptLimit = getClientConfig().getConnectionAttemptLimit();
            int attempt = 0;
            while (true) {
                final long nextTry = Clock.currentTimeMillis() + getClientConfig().getAttemptPeriod();
                for (InetSocketAddress isa : socketAddresses) {
                    try {
                        Address address = new Address(isa);
                        return getConnectionManager().newConnection(address);
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

        private Collection<InetSocketAddress> getClusterAddresses() {
            final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
            for (MemberImpl member : members) {
                socketAddresses.add(member.getInetSocketAddress());
            }
            Collections.shuffle(socketAddresses);
            return socketAddresses;
        }

        public void shutdown() {
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

    private ClientConfig getClientConfig() {
        return client.getClientConfig();
    }

    private class ClusterAuthenticator implements Authenticator {

        private final Credentials credentials = getClientConfig().getCredentials();
        private volatile ClientPrincipal principal;

        public void auth(Connection connection) throws AuthenticationException, IOException {
            AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
            final SerializationServiceImpl serializationService = getSerializationService();
            connection.write(serializationService.toData(auth));
            final Data data = connection.read();
            Object response = serializationService.toObject(data);
            if (response instanceof GenericError) {
                throw new AuthenticationException(((GenericError) response).getMessage());
            }
            System.err.println("principal = " + response);
            principal = (ClientPrincipal) response;
        }
    }

}
