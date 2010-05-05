/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.cluster.*;
import com.hazelcast.config.Config;
import com.hazelcast.config.Interfaces;
import com.hazelcast.config.Join;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.*;
import com.hazelcast.util.NoneStrictObjectPool;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.Level;

public class Node {
    private final ILogger logger;

    private volatile boolean joined = false;

    private volatile boolean active = false;

    private volatile boolean completelyShutdown = false;

    private final ClusterImpl clusterImpl;

    private final Set<Address> failedConnections = new CopyOnWriteArraySet<Address>();

    private final NodeShutdownHookThread shutdownHookThread = new NodeShutdownHookThread("hz.ShutdownThread");

    private final boolean superClient;

    private final NodeType localNodeType;

    final NodeBaseVariables baseVariables;

    public final ConcurrentMapManager concurrentMapManager;

    public final BlockingQueueManager blockingQueueManager;

    public final ClusterManager clusterManager;

    public final TopicManager topicManager;

    public final ListenerManager listenerManager;

    public final ClusterService clusterService;

    public final ExecutorManager executorManager;

    public final InSelector inSelector;

    public final OutSelector outSelector;

    public final MulticastService multicastService;

    public final ConnectionManager connectionManager;

    public final ClientService clientService;

    public final Config config;

    public final GroupProperties groupProperties;

    public final ThreadGroup threadGroup;

    final Address address;

    final MemberImpl localMember;

    volatile Address masterAddress = null;

    volatile Thread serviceThread = null;

    public final FactoryImpl factory;

    private final int buildNumber;

    public final LoggingServiceImpl loggingService;

    private final NoneStrictObjectPool<Packet> packetPool;

    public Node(FactoryImpl factory, Config config) {
        this.threadGroup = new ThreadGroup(factory.getName());
        this.factory = factory;
        this.config = config;
        this.groupProperties = new GroupProperties(config);
        this.superClient = config.isSuperClient();
        this.localNodeType = (superClient) ? NodeType.SUPER_CLIENT : NodeType.MEMBER;
        String version = "unknown";
        String build = "unknown";
        try {
            InputStream inRuntimeProperties = Node.class.getClassLoader().getResourceAsStream("hazelcast-runtime.properties");
            if (inRuntimeProperties != null) {
                Properties runtimeProperties = new Properties();
                runtimeProperties.load(inRuntimeProperties);
                version = runtimeProperties.getProperty("hazelcast.version");
                build = runtimeProperties.getProperty("hazelcast.build");
            }
        } catch (Exception ignored) {
        }
        int tmpBuildNumber = 0;
        try {
            tmpBuildNumber = Integer.getInteger("hazelcast.build", -1);
            if (tmpBuildNumber == -1) {
                tmpBuildNumber = Integer.parseInt(build);
            }
        } catch (Exception ignored) {
        }
        buildNumber = tmpBuildNumber;
        ServerSocketChannel serverSocketChannel;
        Address localAddress = null;
        try {
            final String preferIPv4Stack = System.getProperty("java.net.preferIPv4Stack");
            final String preferIPv6Address = System.getProperty("java.net.preferIPv6Addresses");
            if (preferIPv6Address == null && preferIPv4Stack == null) {
                System.setProperty("java.net.preferIPv4Stack", "true");
            }
            serverSocketChannel = ServerSocketChannel.open();
            AddressPicker addressPicker = new AddressPicker(this, serverSocketChannel);
            localAddress = addressPicker.pickAddress();
            localAddress.setThisAddress(true);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        address = localAddress;
        localMember = new MemberImpl(address, true, localNodeType);
        packetPool = new NoneStrictObjectPool<Packet>(2000) {
            @Override
            public void onRelease(Packet packet) {
                if (packet.released) {
                    throw new RuntimeException("Packet is already released!");
                }
                packet.released = true;
            }

            @Override
            public void onObtain(Packet packet) {
                packet.reset();
                packet.released = false;
            }

            public Packet createNew() {
                return new Packet();
            }
        };
        clusterImpl = new ClusterImpl(this);
        baseVariables = new NodeBaseVariables(address, localMember);
        this.loggingService = new LoggingServiceImpl(config.getGroupConfig().getName(), localMember);
        this.logger = loggingService.getLogger(Node.class.getName());
        //initialize managers..
        clusterService = new ClusterService(this);
        clusterService.start();
        inSelector = new InSelector(this, serverSocketChannel);
        outSelector = new OutSelector(this);
        connectionManager = new ConnectionManager(this);
        clientService = new ClientService(this);
        clusterManager = new ClusterManager(this);
        concurrentMapManager = new ConcurrentMapManager(this);
        blockingQueueManager = new BlockingQueueManager(this);
        listenerManager = new ListenerManager(this);
        topicManager = new TopicManager(this);
        clusterManager.addMember(localMember);
        executorManager = new ExecutorManager(this);
        ILogger systemLogger = getLogger("com.hazelcast.system");
        systemLogger.log(Level.INFO, "Hazelcast " + version + " ("
                + build + ") starting at " + address);
        systemLogger.log(Level.INFO, "Copyright (C) 2008-2010 Hazelcast.com");
        Join join = config.getNetworkConfig().getJoin();
        MulticastService mcService = null;
        try {
            if (join.getMulticastConfig().isEnabled()) {
                MulticastSocket multicastSocket = new MulticastSocket(null);
                multicastSocket.setReuseAddress(true);
                // bind to receive interface
                multicastSocket.bind(new InetSocketAddress(
                        join.getMulticastConfig().getMulticastPort()));
                multicastSocket.setTimeToLive(32);
                // set the send interface
                multicastSocket.setInterface(address.getInetAddress());
                multicastSocket.setReceiveBufferSize(1024);
                multicastSocket.setSendBufferSize(1024);
                multicastSocket.joinGroup(InetAddress
                        .getByName(join.getMulticastConfig().getMulticastGroup()));
                multicastSocket.setSoTimeout(1000);
                mcService = new MulticastService(this, multicastSocket);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        this.multicastService = mcService;
    }

    public void failedConnection(Address address) {
        failedConnections.add(address);
    }

    public ClusterImpl getClusterImpl() {
        return clusterImpl;
    }

    public final NodeType getLocalNodeType() {
        return localNodeType;
    }

    public Address getMasterAddress() {
        return masterAddress;
    }

    public Address getThisAddress() {
        return address;
    }

    public String getName() {
        return factory.getName();
    }

    public void handleInterruptedException(Thread thread, Exception e) {
        logger.log(Level.FINEST, thread.getName() + " is interrupted ", e);
    }

    public static boolean isIP(final String address) {
        if (address.indexOf('.') == -1) {
            return false;
        } else {
            final StringTokenizer st = new StringTokenizer(address, ".");
            int tokenCount = 0;
            while (st.hasMoreTokens()) {
                final String token = st.nextToken();
                tokenCount++;
                try {
                    Integer.parseInt(token);
                } catch (final Exception e) {
                    return false;
                }
            }
            if (tokenCount != 4)
                return false;
        }
        return true;
    }

    public final boolean isSuperClient() {
        return superClient;
    }

    public boolean joined() {
        return joined;
    }

    public boolean master() {
        return address != null && address.equals(masterAddress);
    }

    public void setMasterAddress(final Address master) {
        if (master != null) {
            logger.log(Level.FINE, "** setting master lockAddress to " + master.toString());
        }
        masterAddress = master;
    }

    public void shutdown() {
        logger.log(Level.FINE, "** we are being asked to shutdown when active = " + String.valueOf(active));
        if (isActive()) {
            // set the joined=false first so that
            // threads do not process unnecessary
            // events, such as remove address
            long start = System.currentTimeMillis();
            joined = false;
            setActive(false);
            logger.log(Level.FINEST, "Shutting down the NIO socket selector for input");
            inSelector.shutdown();
            logger.log(Level.FINEST, "Shutting down the NIO socket selector for output");
            outSelector.shutdown();
            logger.log(Level.FINEST, "Shutting down the cluster service");
            clusterService.stop();
            logger.log(Level.FINEST, "Shutting down the query service");
            if (multicastService != null) {
                multicastService.stop();
            }
            logger.log(Level.FINEST, "Shutting down the connection manager");
            connectionManager.shutdown();
            logger.log(Level.FINEST, "Shutting down the concurrentMapManager");
            concurrentMapManager.reset();
            logger.log(Level.FINEST, "Shutting down the clientService");
            clientService.reset();
            logger.log(Level.FINEST, "Shutting down the executorManager");
            executorManager.stop();
            masterAddress = null;
            logger.log(Level.FINEST, "Shutting down the cluster manager");
            clusterManager.stop();
            int numThreads = threadGroup.activeCount();
            Thread[] threads = new Thread[numThreads * 2];
            numThreads = threadGroup.enumerate(threads, false);
            for (int i = 0; i < numThreads; i++) {
                Thread thread = threads[i];
                logger.log(Level.FINEST, "Shutting down thread " + thread.getName());
                thread.interrupt();
            }
            logger.log(Level.INFO, "Hazelcast Shutdown is completed in " + (System.currentTimeMillis() - start) + " ms.");
            packetPool.clear();
        }
    }

    public void start() {
        logger.log(Level.FINEST, "We are asked to start and completelyShutdown is " + String.valueOf(completelyShutdown));
        if (completelyShutdown) return;
        Thread inThread = new Thread(threadGroup, inSelector, "hz.InThread");
//        inThread.setContextClassLoader(config.getClassLoader());
        inThread.setPriority(7);
        logger.log(Level.FINEST, "Starting thread " + inThread.getName());
        inThread.start();
        Thread outThread = new Thread(threadGroup, outSelector, "hz.OutThread");
//        outThread.setContextClassLoader(config.getClassLoader());
        outThread.setPriority(7);
        logger.log(Level.FINEST, "Starting thread " + outThread.getName());
        outThread.start();
        serviceThread = new Thread(threadGroup, clusterService, "hz.ServiceThread");
//        serviceThread.setContextClassLoader(config.getClassLoader());
        serviceThread.setPriority(8);
        logger.log(Level.FINEST, "Starting thread " + serviceThread.getName());
        serviceThread.start();
        if (config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
            final Thread multicastServiceThread = new Thread(threadGroup, multicastService, "hz.MulticastThread");
            multicastServiceThread.start();
//            multicastServiceThread.setContextClassLoader(config.getClassLoader());
            multicastServiceThread.setPriority(6);
        }
        setActive(true);
        if (!completelyShutdown) {
            logger.log(Level.FINEST, "Adding ShutdownHook");
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
        logger.log(Level.FINEST, "finished starting threads, calling join");
        join();
    }

    public ILogger getLogger(String name) {
        return loggingService.getLogger(name);
    }

    public NoneStrictObjectPool<Packet> getPacketPool() {
        return packetPool;
    }

    public GroupProperties getGroupProperties() {
        return groupProperties;
    }

    public class NodeShutdownHookThread extends Thread {

        NodeShutdownHookThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            try {
                if (isActive() && !completelyShutdown) {
                    completelyShutdown = true;
                    if (groupProperties.SHUTDOWNHOOK_ENABLED.getBoolean()) {
                        shutdown();
                    }
                } else {
                    logger.log(Level.FINEST, "shutdown hook - we are not --> active and not completely down so we are not calling shutdown");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void unlock() {
        joined = true;
    }

    private Address findMaster() {
        try {
            final String ip = System.getProperty("join.ip");
            if (ip == null) {
                JoinInfo joinInfo = new JoinInfo(true, address, config.getGroupConfig().getName(),
                        config.getGroupConfig().getPassword(), getLocalNodeType(), Packet.PACKET_VERSION, buildNumber);
                int tryCount = config.getNetworkConfig().getJoin().getMulticastConfig().getMulticastTimeoutSeconds() * 100;
                for (int i = 0; i < tryCount; i++) {
                    multicastService.send(joinInfo);
                    if (masterAddress == null) {
                        Thread.sleep(10);
                    } else {
                        return masterAddress;
                    }
                }
            } else {
                logger.log(Level.FINEST, "RETURNING join.ip");
                return new Address(ip, config.getPort());
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean validateJoinRequest(JoinRequest joinRequest) {
        return config.getGroupConfig().getName().equals(joinRequest.groupName) &&
                config.getGroupConfig().getPassword().equals(joinRequest.groupPassword) &&
                Packet.PACKET_VERSION == joinRequest.packetVersion &&
                buildNumber == joinRequest.buildNumber;
    }

    private Address getAddressFor(String host) {
        int port = config.getPort();
        final int indexColon = host.indexOf(':');
        if (indexColon != -1) {
            port = Integer.parseInt(host.substring(indexColon + 1));
            host = host.substring(0, indexColon);
        }
        final boolean ip = isIP(host);
        try {
            if (ip) {
                return new Address(host, port, true);
            } else {

                final InetAddress[] allAddresses = InetAddress.getAllByName(host);
                for (final InetAddress inetAddress : allAddresses) {
                    boolean shouldCheck = true;
                    Address address;
                    Interfaces interfaces = config.getNetworkConfig().getInterfaces();
                    if (interfaces.isEnabled()) {
                        address = new Address(inetAddress.getAddress(), config.getPort());
                        shouldCheck = AddressPicker.matchAddress(address.getHost(), interfaces.getInterfaces());
                    }
                    if (shouldCheck) {
                        return new Address(inetAddress.getAddress(), port);
                    }
                }
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private List<Address> getPossibleMembers() {
        Join join = config.getNetworkConfig().getJoin();
        final List<String> lsJoinMembers = join.getTcpIpConfig().getMembers();
        final List<Address> lsPossibleAddresses = new ArrayList<Address>();
        for (final String host : lsJoinMembers) {
            // check if host is hostname of ip lockAddress
            final boolean ip = isIP(host);
            try {
                if (ip) {
                    for (int i = 0; i < 3; i++) {
                        final Address addrs = new Address(host, config.getPort() + i, true);
                        if (!addrs.equals(getThisAddress())) {
                            logger.log(Level.FINEST, "adding possible member " + addrs);
                            lsPossibleAddresses.add(addrs);
                        }
                    }
                } else {
                    final InetAddress[] allAddresses = InetAddress.getAllByName(host);
                    for (final InetAddress inetAddress : allAddresses) {
                        boolean shouldCheck = true;
                        Address addrs;
                        Interfaces interfaces = config.getNetworkConfig().getInterfaces();
                        if (interfaces.isEnabled()) {
                            addrs = new Address(inetAddress.getAddress(), config.getPort());
                            shouldCheck = AddressPicker.matchAddress(addrs.getHost(), interfaces.getInterfaces());
                        }
                        if (shouldCheck) {
                            for (int i = 0; i < 3; i++) {
                                final Address addressProper = new Address(inetAddress.getAddress(),
                                        config.getPort() + i);
                                if (!addressProper.equals(getThisAddress())) {
                                    logger.log(Level.FINEST, "adding possible member " + addressProper);
                                    lsPossibleAddresses.add(addressProper);
                                }
                            }
                        }
                    }
                }
            } catch (final Exception e) {
                e.printStackTrace();
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        lsPossibleAddresses.addAll(config.getNetworkConfig().getJoin().getTcpIpConfig().getAddresses());
        return lsPossibleAddresses;
    }

    private void join() {
        if (!config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
            joinWithTCP();
        } else {
            joinWithMulticast();
        }
        clusterManager.finalizeJoin();
        if (baseVariables.lsMembers.size() == 1) {
            final StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append(clusterManager);
            logger.log(Level.INFO, sb.toString());
        }
    }

    void setAsMaster() {
        logger.log(Level.FINE, "This node is being set as the master");
        masterAddress = address;
        logger.log(Level.FINEST, "adding member myself");
        clusterManager.addMember(address, getLocalNodeType()); // add
        // myself
        clusterImpl.setMembers(baseVariables.lsMembers);
        unlock();
    }

    private void joinWithMulticast() {
        int tryCount = 0;
        while (!joined) {
            try {
                logger.log(Level.FINEST, "joining... " + masterAddress);
                if (masterAddress == null) {
                    masterAddress = findMaster();
                    if (masterAddress == null || masterAddress.equals(address)) {
                        TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
                        if (tcpIpConfig != null && tcpIpConfig.isEnabled()) {
                            masterAddress = null;
                            logger.log (Level.FINEST, "Multicast couldn't find cluster. Trying TCP/IP");
                            joinWithTCP();
                        } else {
                            setAsMaster();
                        }
                        return;
                    }
                }
                if (tryCount++ > 20) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("\n");
                    sb.append("===========================");
                    sb.append("\n");
                    sb.append("Couldn't connect to discovered master! tryCount: " + tryCount);
                    sb.append("\n");
                    sb.append("thisAddress: " + address);
                    sb.append("\n");
                    sb.append("masterAddress: " + masterAddress);
                    sb.append("\n");
                    sb.append("connection: " + connectionManager.getConnection(masterAddress));
                    sb.append("===========================");
                    sb.append("\n");
                    logger.log(Level.WARNING, sb.toString());
                    tryCount = 0;
                }
                if (!masterAddress.equals(address)) {
                    connectAndSendJoinRequest(masterAddress);
                } else {
                    masterAddress = null;
                    tryCount = 0;
                }
                Thread.sleep(500);
            } catch (final Exception e) {
                logger.log(Level.FINEST, "multicast join", e);
            }
        }
    }

    private void connectAndSendJoinRequest(Address masterAddress) throws Exception {
        if (masterAddress == null || masterAddress.equals(address)) {
            throw new IllegalArgumentException();
        }
        Connection conn = connectionManager.getOrConnect(masterAddress);
        logger.log(Level.FINEST, "Master connection " + conn);
        if (conn != null) {
            clusterManager.sendJoinRequest(masterAddress);
        }
    }

    private void joinViaPossibleMembers() {
        try {
            failedConnections.clear();
            final List<Address> lsPossibleAddresses = getPossibleMembers();
            lsPossibleAddresses.remove(address);
            for (final Address possibleAddress : lsPossibleAddresses) {
                logger.log(Level.FINEST, "connecting to " + possibleAddress);
                connectionManager.getOrConnect(possibleAddress);
            }
            boolean found = false;
            int numberOfSeconds = 0;
            final int connectionTimeoutSeconds = config.getNetworkConfig().getJoin().getTcpIpConfig().getConnectionTimeoutSeconds();
            while (!found && numberOfSeconds < connectionTimeoutSeconds) {
                lsPossibleAddresses.removeAll(failedConnections);
                if (lsPossibleAddresses.size() == 0) {
                    break;
                }
                Thread.sleep(1000);
                numberOfSeconds++;
                int numberOfJoinReq = 0;
                logger.log(Level.FINE, "we are going to try to connect to each lockAddress, but no more than five times");
                for (final Address possibleAddress : lsPossibleAddresses) {
                    logger.log(Level.FINEST, "connection attempt " + numberOfJoinReq + " to " + possibleAddress);
                    final Connection conn = connectionManager.getOrConnect(possibleAddress);
                    if (conn != null && numberOfJoinReq < 5) {
                        found = true;
                        logger.log(Level.FINEST, "found and sending join request for " + possibleAddress);
                        clusterManager.sendJoinRequest(possibleAddress);
                        numberOfJoinReq++;
                    } else {
                        logger.log(Level.FINEST, "number of join reqests is greater than 5, no join request will be sent for " + possibleAddress);
                    }
                }
            }
            logger.log(Level.FINEST, "FOUND " + found);
            if (!found) {
                logger.log(Level.FINEST, "This node will assume master role since no possible member where connected to");
                setAsMaster();
            } else {
                while (!joined) {
                    int numberOfJoinReq = 0;
                    lsPossibleAddresses.removeAll(failedConnections);
                    for (final Address possibleAddress : lsPossibleAddresses) {
                        final Connection conn = connectionManager.getOrConnect(possibleAddress);
                        if (conn != null && numberOfJoinReq < 5) {
                            logger.log(Level.FINEST, "sending join request for " + possibleAddress);
                            clusterManager.sendJoinRequest(possibleAddress);
                            numberOfJoinReq++;
                        } else {
                            logger.log(Level.FINEST, "number of join request is greater than 5, no join request will be sent for " + possibleAddress + " the second time");
                        }
                    }
                    Thread.sleep(2000);
                    if (masterAddress == null) { // no-one knows the master
                        boolean masterCandidate = true;
                        for (final Address address : lsPossibleAddresses) {
                            if (this.address.hashCode() > address.hashCode())
                                masterCandidate = false;
                        }
                        if (masterCandidate) {
                            logger.log(Level.FINEST, "I am the master candidate, setting as master");
                            setAsMaster();
                        }
                    }
                }
            }
            lsPossibleAddresses.clear();
            failedConnections.clear();
        } catch (final Exception e) {
            e.printStackTrace();
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    private void joinViaRequiredMember() {
        try {
            final Address requiredAddress = getAddressFor(config.getNetworkConfig().getJoin().getTcpIpConfig().getRequiredMember());
            logger.log(Level.FINEST, "Joining over required member " + requiredAddress);
            if (requiredAddress == null) {
                throw new RuntimeException("Invalid required member "
                        + config.getNetworkConfig().getJoin().getTcpIpConfig().getRequiredMember());
            }
            if (requiredAddress.equals(address)) {
                setAsMaster();
                return;
            }
            connectionManager.getOrConnect(requiredAddress);
            Connection conn = null;
            while (conn == null) {
                conn = connectionManager.getOrConnect(requiredAddress);
                Thread.sleep(1000);
            }
            while (!joined) {
                final Connection connection = connectionManager.getOrConnect(requiredAddress);
                if (connection == null) {
                    joinViaRequiredMember();
                }
                logger.log(Level.FINEST, "Sending joinRequest " + requiredAddress);
                clusterManager.sendJoinRequest(requiredAddress);
                Thread.sleep(2000);
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void joinWithTCP() {
        if (config.getNetworkConfig().getJoin().getTcpIpConfig().getRequiredMember() != null) {
            joinViaRequiredMember();
        } else {
            joinViaPossibleMembers();
        }
    }

    public Config getConfig() {
        return config;
    }

    public int getBuildNumber() {
        return buildNumber;
    }

    public String toString() {
        return "Node[" + getName() + "]";
    }

    /**
     * @param active the active to set
     */
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * @return the active
     */
    public boolean isActive() {
        return active;
    }
}
