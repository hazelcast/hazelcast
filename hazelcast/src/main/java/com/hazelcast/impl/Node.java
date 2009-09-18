/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterImpl;
import com.hazelcast.cluster.ClusterManager;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.config.Interfaces;
import com.hazelcast.config.Join;
import com.hazelcast.impl.MulticastService.JoinInfo;
import com.hazelcast.nio.*;
import com.hazelcast.query.QueryService;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Node {
    private final Logger logger = Logger.getLogger(Node.class.getName());

    private volatile boolean joined = false;

    volatile boolean active = false;

    private volatile boolean completelyShutdown = false;

    private final ClusterImpl clusterImpl;

    private final BlockingQueue<Address> failedConnections = new LinkedBlockingQueue<Address>();

    private final ShutdownHookThread shutdownHookThread = new ShutdownHookThread();

    private final boolean superClient;

    private final NodeType localNodeType;

    final BaseVariables baseVariables;

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

    public final QueryService queryService;

    public final ClientService clientService;

    public final Config config;

    public final ThreadGroup threadGroup;

    volatile Address address = null;

    volatile MemberImpl localMember = null;

    volatile Address masterAddress = null;

    volatile Thread serviceThread = null;

    volatile Thread queryThread = null;

    public enum NodeType {
        MEMBER(1),
        SUPER_CLIENT(2),
        JAVA_CLIENT(3),
        CSHARP_CLIENT(4);

        NodeType(int type) {
            this.value = type;
        }

        private int value;

        public int getValue() {
            return value;
        }

        public static NodeType create(int value) {
            switch (value) {
                case 1:
                    return MEMBER;
                case 2:
                    return SUPER_CLIENT;
                case 3:
                    return JAVA_CLIENT;
                case 4:
                    return CSHARP_CLIENT;
                default:
                    return null;
            }
        }
    }


    class BaseVariables {
        final LinkedList<MemberImpl> lsMembers = new LinkedList<MemberImpl>();

        final Map<Address, MemberImpl> mapMembers = new HashMap<Address, MemberImpl>(
                100);

        final Map<Long, BaseManager.Call> mapCalls = new HashMap<Long, BaseManager.Call>();

        final BaseManager.EventQueue[] eventQueues = new BaseManager.EventQueue[BaseManager.EVENT_QUEUE_COUNT];

        final Map<Long, StreamResponseHandler> mapStreams = new ConcurrentHashMap<Long, StreamResponseHandler>();

        final AtomicLong localIdGen = new AtomicLong(0);

        final Address thisAddress;

        final MemberImpl thisMember;


        BaseVariables(Address thisAddress, MemberImpl thisMember) {
            this.thisAddress = thisAddress;
            this.thisMember = thisMember;

            for (int i = 0; i < BaseManager.EVENT_QUEUE_COUNT; i++) {
                eventQueues[i] = new BaseManager.EventQueue();
            }
        }
    }

    public final FactoryImpl factory;

    public Node(FactoryImpl factory, Config config) {
        this.threadGroup = new ThreadGroup(factory.getName());
        this.factory = factory;
        this.config = config;
        superClient = config.isSuperClient();
        localNodeType = (superClient) ? NodeType.SUPER_CLIENT : NodeType.MEMBER;
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
        ServerSocketChannel serverSocketChannel;
        try {
            final String preferIPv4Stack = System.getProperty("java.net.preferIPv4Stack");
            final String preferIPv6Address = System.getProperty("java.net.preferIPv6Addresses");
            if (preferIPv6Address == null && preferIPv4Stack == null) {
                System.setProperty("java.net.preferIPv4Stack", "true");
            }
            AddressPicker addressPicker = new AddressPicker(this);
            serverSocketChannel = ServerSocketChannel.open();
            address = addressPicker.pickAddress(this, serverSocketChannel);
            address.setThisAddress(true);
            localMember = new MemberImpl(address, true, localNodeType);
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
        clusterImpl = new ClusterImpl(this);
        baseVariables = new BaseVariables(address, localMember);
        //initialize managers..
        clusterService = new ClusterService(this);
        clusterService.start();

        inSelector = new InSelector(this, serverSocketChannel);
        outSelector = new OutSelector(this);
        connectionManager = new ConnectionManager(this);

        clientService = new ClientService(this);
        queryService = new QueryService(this);
        clusterManager = new ClusterManager(this);
        concurrentMapManager = new ConcurrentMapManager(this);
        blockingQueueManager = new BlockingQueueManager(this);
        executorManager = new ExecutorManager(this);
        listenerManager = new ListenerManager(this);
        topicManager = new TopicManager(this);

        clusterManager.addMember(localMember);

        Logger systemLogger = Logger.getLogger("com.hazelcast.system");
        systemLogger.log(Level.INFO, "Hazelcast " + version + " ("
                + build + ") starting at " + address);
        systemLogger.log(Level.INFO, "Copyright (C) 2009 Hazelcast.com");
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
        }
        this.multicastService = mcService;
    }

    public void failedConnection(final Address address) {
        failedConnections.offer(address);
    }

    public ClusterImpl getClusterImpl() {
        return clusterImpl;
    }

    public MemberImpl getLocalMember() {
        return localMember;
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

    public void handleInterruptedException(final Thread thread, final Exception e) {
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

    public boolean isMaster(final Address address) {
        return (address.equals(masterAddress));
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

    public void reJoin() {
        logger.log(Level.FINEST, "REJOINING...");
        joined = false;
        masterAddress = null;
        join();
    }
    
    public void setMasterAddress(final Address master) {
        masterAddress = master;
    }

    public void shutdown() {
        if (active) {
            // set the joined=false first so that
            // threads do not process unnecessary
            // events, such as removeaddress
            joined = false;
            active = false;
            inSelector.shutdown();
            outSelector.shutdown();
            clusterService.stop();
            queryService.stop();
            if (multicastService != null) {
                multicastService.stop();
            }
            connectionManager.shutdown();
            concurrentMapManager.reset();
            clientService.reset();
            executorManager.stop();
            address = null;
            masterAddress = null;
            clusterManager.stop();
            int numThreads = threadGroup.activeCount();
            Thread[] threads = new Thread[numThreads * 2];
            numThreads = threadGroup.enumerate(threads, false);
            for (int i = 0; i < numThreads; i++) {
                Thread thread = threads[i];
                thread.interrupt();
            }
        }
    }

    public void start() {
        if (completelyShutdown) return;
        Thread inThread = new Thread(threadGroup, inSelector, "hz.InThread");
        inThread.setPriority(7);
        inThread.start();

        Thread outThread = new Thread(threadGroup, outSelector, "hz.OutThread");
        outThread.setPriority(7);
        outThread.start();

        serviceThread = new Thread(threadGroup, clusterService, "hz.ServiceThread");
        serviceThread.setPriority(8);
        serviceThread.start();

        queryThread = new Thread(threadGroup, queryService, "hz.QueryThread");
        queryThread.setPriority(6);
        queryThread.start();

        if (config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
            final Thread multicastServiceThread = new Thread(threadGroup, multicastService, "hz.MulticastThread");
            multicastServiceThread.start();
            multicastServiceThread.setPriority(6);
        }
        
        active = true;
        if (!completelyShutdown) {
            logger.log(Level.FINEST, "Adding ShutdownHook");
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
        join();
    }

    class ShutdownHookThread extends Thread {

        @Override
        public void run() {
            try {
                if (active && !completelyShutdown) {
                    completelyShutdown = true;
                    shutdown();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void unlock() {
        joined = true;
    }

    void setAsMaster() {
        masterAddress = address;
        logger.log(Level.FINEST, "adding member myself");
        clusterManager.addMember(address, getLocalNodeType()); // add
        // myself
        clusterImpl.setMembers(baseVariables.lsMembers);
        unlock();
    }

    private Address findMaster() {
        try {
            final String ip = System.getProperty("join.ip");
            if (ip == null) {
                JoinInfo joinInfo = new JoinInfo(true, address, config.getGroupName(),
                        config.getGroupPassword(), getLocalNodeType());

                for (int i = 0; i < 200; i++) {
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

    private Address getAddressFor(final String host) {
        int port = config.getPort();
        final int indexColon = host.indexOf(':');
        if (indexColon != -1) {
            port = Integer.parseInt(host.substring(indexColon + 1));
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
                        shouldCheck = AddressPicker.matchAddress(address.getHost(), interfaces.getLsInterfaces());
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
        final List<String> lsJoinMembers = join.getJoinMembers().getMembers();
        final List<Address> lsPossibleAddresses = new ArrayList<Address>();
        for (final String host : lsJoinMembers) {
            // check if host is hostname of ip address
            final boolean ip = isIP(host);
            try {
                if (ip) {
                    for (int i = 0; i < 3; i++) {
                        final Address addrs = new Address(host, config.getPort() + i, true);
                        if (!addrs.equals(getThisAddress())) {
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
                            shouldCheck = AddressPicker.matchAddress(addrs.getHost(), interfaces.getLsInterfaces());
                        }
                        if (shouldCheck) {
                            for (int i = 0; i < 3; i++) {
                                final Address addressProper = new Address(inetAddress.getAddress(),
                                        config.getPort() + i);
                                if (!addressProper.equals(getThisAddress())) {
                                    lsPossibleAddresses.add(addressProper);
                                }
                            }
                        }
                    }
                }
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
        lsPossibleAddresses.addAll(config.getNetworkConfig().getJoin().getJoinMembers().getAddresses());
        return lsPossibleAddresses;
    }


    private void join() {
        if (!config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
            joinWithTCP();
        } else {
            joinWithMulticast();
        }
        logger.log(Level.FINEST, "Join DONE");
        clusterManager.finalizeJoin();
        if (baseVariables.lsMembers.size() == 1) {
            final StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append(clusterManager);
            logger.log(Level.INFO, sb.toString());
        }
    }

    private void joinWithMulticast() {
        masterAddress = findMaster();
        logger.log(Level.FINEST, address + " master: " + masterAddress);
        if (masterAddress == null || masterAddress.equals(address)) {
            clusterManager.addMember(address, getLocalNodeType()); // add
            // myself
            masterAddress = address;
            clusterImpl.setMembers(baseVariables.lsMembers);
            unlock();
        } else {
            while (!joined) {
                try {
                    logger.log(Level.FINEST, "joining... " + masterAddress);
                    if (masterAddress == null) {
                        joinWithMulticast();
                    } else if (masterAddress.equals(address)) {
                        setAsMaster();
                    }
                    joinExisting(masterAddress);
                    Thread.sleep(500);
                } catch (final Exception e) {
                    logger.log(Level.FINEST, "multicast join", e);
                }
            }
        }
    }

    private void joinExisting(final Address masterAddress) throws Exception {
        if (masterAddress == null) return;
        if (masterAddress.equals(getThisAddress())) return;
        Connection conn = connectionManager.getOrConnect(masterAddress);
        if (conn == null)
            Thread.sleep(1000);
        conn = connectionManager.getConnection(masterAddress);
        logger.log(Level.FINEST, "Master connnection " + conn);
        if (conn != null)
            clusterManager.sendJoinRequest(masterAddress);
    }

    private void joinViaPossibleMembers() {
        try {
            final List<Address> lsPossibleAddresses = getPossibleMembers();
            lsPossibleAddresses.remove(address);
            for (final Address adrs : lsPossibleAddresses) {
                logger.log(Level.FINEST, "connecting to " + adrs);
                connectionManager.getOrConnect(adrs);
            }
            boolean found = false;
            int numberOfSeconds = 0;
            while (!found
                    && numberOfSeconds < config.getNetworkConfig().getJoin().getJoinMembers().getConnectionTimeoutSeconds()) {
                Address addressFailed;
                while ((addressFailed = failedConnections.poll()) != null) {
                    lsPossibleAddresses.remove(addressFailed);
                }
                if (lsPossibleAddresses.size() == 0)
                    break;
                Thread.sleep(1000);
                numberOfSeconds++;
                int numberOfJoinReq = 0;
                for (final Address adrs : lsPossibleAddresses) {
                    final Connection conn = connectionManager.getOrConnect(adrs);
                    logger.log(Level.FINEST, "conn " + conn);
                    if (conn != null && numberOfJoinReq < 5) {
                        found = true;
                        clusterManager.sendJoinRequest(adrs);
                        numberOfJoinReq++;
                    }
                }
            }
            logger.log(Level.FINEST, "FOUND " + found);
            if (!found) {
                setAsMaster();
            } else {
                while (!joined) {
                    int numberOfJoinReq = 0;
                    for (final Address adrs : lsPossibleAddresses) {
                        final Connection conn = connectionManager.getOrConnect(adrs);
                        if (conn != null && numberOfJoinReq < 5) {
                            clusterManager.sendJoinRequest(adrs);
                            numberOfJoinReq++;
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
                            setAsMaster();
                        }
                    }
                }

            }
            lsPossibleAddresses.clear();
            failedConnections.clear();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void joinViaRequiredMember() {
        try {
            final Address requiredAddress = getAddressFor(config.getNetworkConfig().getJoin().getJoinMembers().getRequiredMember());
            logger.log(Level.FINEST, "Joining over required member " + requiredAddress);
            if (requiredAddress == null) {
                throw new RuntimeException("Invalid required member "
                        + config.getNetworkConfig().getJoin().getJoinMembers().getRequiredMember());
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
                if (connection == null)
                    joinViaRequiredMember();
                logger.log(Level.FINEST, "Sending joinRequest " + requiredAddress);
                clusterManager.sendJoinRequest(requiredAddress);

                Thread.sleep(2000);
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void joinWithTCP() {
        if (config.getNetworkConfig().getJoin().getJoinMembers().getRequiredMember() != null) {
            joinViaRequiredMember();
        } else {
            joinViaPossibleMembers();
        }
    }

    public Config getConfig() {
        return config;
    }
}
