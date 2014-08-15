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

package com.hazelcast.client;

import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.*;
import static java.text.MessageFormat.format;

public class ConnectionManager implements MembershipListener {

    private volatile Connection currentConnection;
    private final AtomicInteger connectionIdGenerator = new AtomicInteger(-1);
    private final List<InetSocketAddress> clusterMembers = new CopyOnWriteArrayList<InetSocketAddress>();
    private final List<InetSocketAddress> initialClusterMembers = new CopyOnWriteArrayList<InetSocketAddress>();
    private final ILogger logger = Logger.getLogger(getClass().getName());
    private final HazelcastClient client;
    private volatile int lastDisconnectedConnectionId = -1;
    private ClientBinder binder;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private volatile boolean lookingForLiveConnection = false;
    private volatile boolean running = true;

    private final LifecycleServiceClientImpl lifecycleService;
    final Timer heartbeatTimer = new Timer();
    private final ClientConfig config;

    public ConnectionManager(HazelcastClient client, ClientConfig config, LifecycleServiceClientImpl lifecycleService) {
        this.config = config;
        this.client = client;
        this.lifecycleService = lifecycleService;
        this.clusterMembers.addAll(config.getAddressList());
        this.initialClusterMembers.addAll(config.getAddressList());
        if (config.isShuffle()) {
            Collections.shuffle(this.clusterMembers);
            Collections.shuffle(this.initialClusterMembers);
        }
    }

    void scheduleHeartbeatTimerTask() {
        final int TIMEOUT = config.getConnectionTimeout();
        heartbeatTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                long diff = Clock.currentTimeMillis() - client.getInRunnable().lastReceived;
                try {
                    if (diff >= TIMEOUT / 5 && diff < TIMEOUT) {
                        logger.log(Level.FINEST,
                                "Being idle for some time, Doing a getMembers() call to ping the server!");
                        final CountDownLatch latch = new CountDownLatch(1);
                        executorService.execute(new Runnable() {
                            public void run() {
                                Set<Member> members = client.getCluster().getMembers();
                                if (members != null && members.size() >= 1) {
                                    latch.countDown();
                                }
                            }
                        });
                        if (!latch.await(10000, TimeUnit.MILLISECONDS)) {
                            logger.log(Level.WARNING, "Server didn't respond to client's ping call within 10 seconds!");
                        }
                    } else if (diff >= TIMEOUT) {
                        logger.log(Level.WARNING, "Server didn't respond to client's requests for " + TIMEOUT / 1000 +
                                " seconds. Assuming it is dead, closing the connection!");
                        if (currentConnection != null) {
                            currentConnection.close();
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (IOException ignored) {
                }
            }
        }, TIMEOUT / 10, TIMEOUT / 10);
    }

    public Connection getInitConnection() throws IOException {
        if (currentConnection == null) {
            synchronized (this) {
                currentConnection = lookForLiveConnection(config.getInitialConnectionAttemptLimit(),
                        config.getReConnectionTimeOut());
            }
        }
        return currentConnection;
    }

    public Connection lookForLiveConnection() throws IOException {
        return lookForLiveConnection(config.getReconnectionAttemptLimit(), config.getReConnectionTimeOut());
    }

    private Connection lookForLiveConnection(final int attemptsLimit,
                                             final int reconnectionTimeout) throws IOException {
        lookingForLiveConnection = true;
        try {
            boolean restored = false;
            int attempt = 0;
            while (currentConnection == null && running && !Thread.interrupted()) {
                final long next = Clock.currentTimeMillis() + reconnectionTimeout;
                synchronized (this) {
                    if (currentConnection == null) {
                        final Connection connection = searchForAvailableConnection();
                        restored = connection != null;
                        if (restored) {
                            try {
                                ClientConfig clientConfig = client.getClientConfig();
                                if (clientConfig != null) {
                                    SocketInterceptor socketInterceptor = clientConfig.getSocketInterceptor();
                                    if (socketInterceptor != null) {
                                        socketInterceptor.onConnect(connection.getSocket());
                                    }
                                }
                                bindConnection(connection);
                                currentConnection = connection;
                            } catch (Throwable e) {
                                closeConnection(connection);
                                logger.log(Level.WARNING, "got an exception on getConnection:" + e.getMessage(), e);
                                restored = false;
                            }
                        }
                    }
                }
                if (currentConnection != null) {
                    logger.log(Level.FINE, "Client is connecting to " + currentConnection);
                    lookingForLiveConnection = false;
                    break;
                }
                if (attempt >= attemptsLimit) {
                    break;
                }
                attempt++;
                final long t = next - Clock.currentTimeMillis();
                logger.log(Level.INFO, format("Unable to get alive cluster connection," +
                        " try in {0} ms later, attempt {1} of {2}.",
                        Math.max(0, t), attempt, attemptsLimit));
                if (t > 0) {
                    try {
                        Thread.sleep(t);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
            if (restored) {
                notifyConnectionIsRestored();
            }
        } finally {
            lookingForLiveConnection = false;
        }
        return currentConnection;
    }

    void closeConnection(final Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Throwable e) {
            logger.log(Level.INFO, "got an exception on closeConnection "
                    + connection + ":" + e.getMessage(), e);
        }
    }

    public Connection getConnection() throws IOException {
        if (currentConnection == null && running && !lookingForLiveConnection) {
            boolean restored = false;
            synchronized (this) {
                if (currentConnection == null) {
                    Connection connection = searchForAvailableConnection();
                    if (connection != null) {
                        logger.log(Level.FINE, "Client is connecting to " + connection);
                        try {
                            bindConnection(connection);
                            currentConnection = connection;
                        } catch (Throwable e) {
                            closeConnection(connection);
                            logger.log(Level.WARNING, "got an exception on getConnection:" + e.getMessage(), e);
                        }
                    }
                    restored = currentConnection != null;
                }
            }
            if (restored) {
                notifyConnectionIsRestored();
            }
        }
        return currentConnection;
    }

    void notifyConnectionIsRestored() {
        lifecycleService.fireLifecycleEvent(CLIENT_CONNECTION_OPENING);
    }

    void notifyConnectionIsOpened() {
        lifecycleService.fireLifecycleEvent(CLIENT_CONNECTION_OPENED);
    }

    void bindConnection(Connection connection) throws IOException {
        binder.bind(connection, config.getCredentials());
    }

    public void destroyConnection(final Connection connection) {
        boolean lost = false;
        synchronized (this) {
            if (currentConnection != null &&
                    connection != null &&
                    currentConnection.getVersion() == connection.getVersion()) {
                logger.log(Level.WARNING, "Connection to " + currentConnection + " is lost");
                // remove current connection's address from member list.
                // if address is IPv6 then remove all possible socket addresses (for all scopes)
                while (clusterMembers.remove(currentConnection.getAddress())) ;
                currentConnection = null;
                lost = true;
                try {
                    connection.close();
                } catch (IOException e) {
                    logger.log(Level.FINEST, e.getMessage(), e);
                }
            }
        }
        if (lost) {
            lifecycleService.fireLifecycleEvent(CLIENT_CONNECTION_LOST);
        }
    }

    private void popAndPush(List<InetSocketAddress> clusterMembers) {
        if (!clusterMembers.isEmpty()) {
            InetSocketAddress address = clusterMembers.remove(0);
            clusterMembers.add(address);
        }
    }

    private Connection searchForAvailableConnection() {
        Connection connection = null;
        popAndPush(clusterMembers);
        if (clusterMembers.isEmpty()) {
            clusterMembers.addAll(initialClusterMembers);
        }
        int counter = clusterMembers.size();
        while (counter > 0) {
            try {
                connection = getNextConnection();
                break;
            } catch (Exception e) {
                logger.log(Level.FINEST, e.getMessage(), e);
                popAndPush(clusterMembers);
                counter--;
            }
        }
        logger.log(Level.FINEST, format("searchForAvailableConnection connection:{0}", connection));
        return connection;
    }

    protected Connection getNextConnection() {
        InetSocketAddress address = clusterMembers.get(0);
        return new Connection(config.getConnectionTimeout(), address, connectionIdGenerator.incrementAndGet());
    }

    public void memberAdded(MembershipEvent membershipEvent) {
        InetSocketAddress address = membershipEvent.getMember().getInetSocketAddress();
        Collection<InetSocketAddress> addresses = AddressHelper.getPossibleSocketAddresses(address.getAddress(),
                address.getPort());
        clusterMembers.addAll(addresses);
        initialClusterMembers.addAll(addresses);
    }

    public void memberRemoved(MembershipEvent membershipEvent) {
        InetSocketAddress address = membershipEvent.getMember().getInetSocketAddress();
        Collection<InetSocketAddress> addresses = AddressHelper.getPossibleSocketAddresses(address.getAddress(),
                address.getPort());
        clusterMembers.removeAll(addresses);
    }

    public void updateMembers() {
        Set<Member> members = client.getCluster().getMembers();
        clusterMembers.clear();
        for (Member member : members) {
            InetSocketAddress address = member.getInetSocketAddress();
            Collection<InetSocketAddress> addresses = AddressHelper.getPossibleSocketAddresses(address.getAddress(),
                    address.getPort());
            clusterMembers.addAll(addresses);
        }
    }

    public boolean shouldExecuteOnDisconnect(Connection connection) {
        if (connection == null || lastDisconnectedConnectionId >= connection.getVersion()) {
            return false;
        }
        lastDisconnectedConnectionId = connection.getVersion();
        return true;
    }

    public void setBinder(ClientBinder binder) {
        this.binder = binder;
    }

    List<InetSocketAddress> getClusterMembers() {
        return clusterMembers;
    }

    public void shutdown() {
        logger.log(Level.INFO, getClass().getSimpleName() + " shutdown");
        running = false;
        heartbeatTimer.cancel();
    }
}
