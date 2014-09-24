package com.hazelcast.client.spi.impl;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.client.AddMembershipListenerRequest;
import com.hazelcast.cluster.client.ClientMembershipEvent;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

class ClusterListenerThread extends Thread {

    private static final ILogger LOGGER = Logger.getLogger(ClusterListenerThread.class);
    private static final int SLEEP_TIME = 1000;
    protected final List<MemberImpl> members = new LinkedList<MemberImpl>();
    protected ClientClusterServiceImpl clusterService;
    private volatile ClientConnection conn;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final Collection<AddressProvider> addressProviders;
    private HazelcastClient client;
    private ClientConnectionManager connectionManager;
    private ClientListenerServiceImpl clientListenerService;


    public ClusterListenerThread(ThreadGroup group, String name, Collection<AddressProvider> addressProviders) {
        super(group, name);
        this.addressProviders = addressProviders;
    }

    public void init(HazelcastClient client) {
        this.client = client;
        this.connectionManager = client.getConnectionManager();
        this.clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        this.clientListenerService = (ClientListenerServiceImpl) client.getListenerService();
    }

    public void await() throws InterruptedException {
        latch.await();
    }

    ClientConnection getConnection() {
        return conn;
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (conn == null) {
                    try {
                        conn = connectToOne();
                    } catch (Exception e) {
                        LOGGER.severe("Error while connecting to cluster!", e);
                        client.getLifecycleService().shutdown();
                        latch.countDown();
                        return;
                    }
                }
                clientListenerService.triggerFailedListeners();
                loadInitialMemberList();
                listenMembershipEvents();
            } catch (Exception e) {
                if (client.getLifecycleService().isRunning()) {
                    if (LOGGER.isFinestEnabled()) {
                        LOGGER.warning("Error while listening cluster events! -> " + conn, e);
                    } else {
                        LOGGER.warning("Error while listening cluster events! -> " + conn + ", Error: " + e.toString());
                    }
                }

                connectionManager.onCloseOwnerConnection();
                IOUtil.closeResource(conn);
                conn = null;
                clusterService.fireConnectionEvent(true);
            }
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                latch.countDown();
                break;
            }
        }
    }

    private Collection<InetSocketAddress> getSocketAddresses() throws Exception {
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
        if (!members.isEmpty()) {
            for (MemberImpl member : members) {
                socketAddresses.add(member.getInetSocketAddress());
            }
            Collections.shuffle(socketAddresses);
        }

        for (AddressProvider addressProvider : addressProviders) {
            socketAddresses.addAll(addressProvider.loadAddresses());
        }

        return socketAddresses;
    }

    private void loadInitialMemberList() throws Exception {
        final SerializationService serializationService = clusterService.getSerializationService();
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
        LOGGER.info(clusterService.membersString());
        fireMembershipEvent(prevMembers);
        latch.countDown();
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
                connectionManager.removeEndpoint(member.getAddress());
            }
        }
        for (MembershipEvent event : events) {
            clusterService.fireMembershipEvent(event);
        }
    }

    private void listenMembershipEvents() throws IOException {
        final SerializationService serializationService = clusterService.getSerializationService();
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
                connectionManager.removeEndpoint(member.getAddress());
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
    }


    protected void updateMembersRef() {
        final Map<Address, MemberImpl> map = new LinkedHashMap<Address, MemberImpl>(members.size());
        for (MemberImpl member : members) {
            map.put(member.getAddress(), member);
        }
        clusterService.setMembersRef(Collections.unmodifiableMap(map));
    }

    void shutdown() {
        interrupt();
        final ClientConnection c = conn;
        if (c != null) {
            c.close();
        }
    }

    private ClientConnection connectToOne() throws Exception {
        final ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        final int connAttemptLimit = networkConfig.getConnectionAttemptLimit();
        final int connectionAttemptPeriod = networkConfig.getConnectionAttemptPeriod();

        final int connectionAttemptLimit = connAttemptLimit == 0 ? Integer.MAX_VALUE : connAttemptLimit;

        int attempt = 0;
        Throwable lastError = null;
        Set<Address> triedAddresses = new HashSet<Address>();
        while (true) {
            final long nextTry = Clock.currentTimeMillis() + connectionAttemptPeriod;
            final Collection<InetSocketAddress> socketAddresses = getSocketAddresses();
            for (InetSocketAddress isa : socketAddresses) {
                Address address = new Address(isa);
                triedAddresses.add(address);
                try {
                    final ClientConnection connection = connectionManager.ownerConnection(address);
                    clusterService.fireConnectionEvent(false);
                    return connection;
                } catch (IOException e) {
                    lastError = e;
                    LOGGER.finest("IO error during initial connection to " + address, e);
                } catch (AuthenticationException e) {
                    lastError = e;
                    LOGGER.warning("Authentication error on " + address, e);
                }
            }
            if (attempt++ >= connectionAttemptLimit) {
                break;
            }
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
        throw new IllegalStateException("Unable to connect to any address in the config! The following addresses were tried:"
                + triedAddresses, lastError);
    }
}

