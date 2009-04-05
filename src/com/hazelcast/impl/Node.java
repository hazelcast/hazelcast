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

import static com.hazelcast.impl.Constants.NodeTypes.NODE_MEMBER;
import static com.hazelcast.impl.Constants.NodeTypes.NODE_SUPER_CLIENT;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hazelcast.impl.MulticastService.JoinInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.InSelector;
import com.hazelcast.nio.OutSelector;

public class Node {
    protected static Logger logger = Logger.getLogger(Node.class.getName());

    static final boolean DEBUG = Build.DEBUG;

    volatile Address address = null;

    volatile MemberImpl localMember = null;

    volatile Address masterAddress = null;

    private static Node instance = new Node();

    private volatile boolean joined = false;

    private Object joinLock = new Object();

    private ClusterImpl clusterImpl = null;

    private final CoreDump coreDump = new CoreDump();

    private Thread firstMainThread = null;

    private final List<Thread> lsThreads = new ArrayList<Thread>(3);

    private final BlockingQueue<Address> qFailedConnections = new LinkedBlockingQueue<Address>();

    private final boolean superClient;

    private final int localNodeType;

    private Node() {
        boolean sClient = false;
        final String superClientProp = System.getProperty("hazelcast.super.client");
        if (superClientProp != null) {
            if ("true".equalsIgnoreCase(superClientProp)) {
                sClient = true;
            }
        }
        superClient = sClient;
        localNodeType = (superClient) ? NODE_SUPER_CLIENT : NODE_MEMBER;
    }

    public static Node get() {
        return instance;
    }

    public void dumpCore(final Throwable ex) {
        try {
            final StringBuffer sb = new StringBuffer();
            if (ex != null) {
                exceptionToStringBuffer(ex, sb);
            }
            sb.append("Hazelcast.version : " + Build.version + "\n");
            sb.append("Hazelcast.build   : " + Build.build + "\n");
            sb.append("Hazelcast.address   : " + address + "\n");
            sb.append("joined : " + joined + "\n");
            sb.append(AddressPicker.createCoreDump());
            coreDump.getPrintWriter().write(sb.toString());
            coreDump.getPrintWriter().write("\n");
            coreDump.getPrintWriter().write("\n");
            for (final Thread thread : lsThreads) {
                thread.interrupt();
            }
            if (!joined) {
                if (firstMainThread != null) {
                    try {
                        firstMainThread.interrupt();
                    } catch (final Exception e) {
                    }
                }
            }
            String fileName = "hz-core";
            if (address != null)
                fileName += "-" + address.getHost() + "_" + address.getPort();
            fileName += ".txt";
            final FileOutputStream fos = new FileOutputStream(fileName);
            Util.writeText(coreDump.toString(), fos);
            fos.flush();
            fos.close();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void exceptionToStringBuffer(final Throwable e, final StringBuffer sb) {

        final StackTraceElement[] stEls = e.getStackTrace();
        for (final StackTraceElement stackTraceElement : stEls) {
            sb.append("\tat " + stackTraceElement + "\n");
        }
        final Throwable cause = e.getCause();
        if (cause != null) {
            sb.append("\tcaused by " + cause);
        }
    }

    public void failedConnection(final Address address) {
        qFailedConnections.offer(address);
    }

    public ClusterImpl getClusterImpl() {
        return clusterImpl;
    }

    public CoreDump getCoreDump() {
        return coreDump;
    }

    public MemberImpl getLocalMember() {
        return localMember;
    }

    public final int getLocalNodeType() {
        return localNodeType;
    }

    public Address getMasterAddress() {
        return masterAddress;
    }

    public Address getThisAddress() {
        return address;
    }

    public synchronized void handleInterruptedException(final Thread thread, final Exception e) {
        final PrintWriter pw = coreDump.getPrintWriter();
        pw.write(thread.toString());
        pw.write("\n");
        final StackTraceElement[] stEls = e.getStackTrace();
        for (final StackTraceElement stackTraceElement : stEls) {
            pw.write("\tat " + stackTraceElement + "\n");
        }
        final Throwable cause = e.getCause();
        if (cause != null) {
            pw.write("\tcaused by " + cause);
        }
    }

    public boolean isIP(final String address) {
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
        return address.equals(masterAddress);
    }

    public void reJoin() {
        logger.log(Level.FINEST, "REJOINING...");
        joined = false;
        masterAddress = null;
        join();
    }

    public void restart() {
        shutdown();
        start();
    }

    public void setMasterAddress(final Address master) {
        masterAddress = master;
    }

    public void shutdown() {
        try {
            ClusterService.get().stop();
            MulticastService.get().stop();
            ConnectionManager.get().shutdown();
            ExecutorManager.get().stop();
            InSelector.get().shutdown();
            OutSelector.get().shutdown();
            address = null;
            masterAddress = null;
            joined = false;
            FactoryImpl.inited.set(false);
            ClusterManager.get().stop();
        } catch (Throwable e) {
            logger.log(Level.FINEST, "shutdown exception", e);
        }
    }

    public void start() {
        firstMainThread = Thread.currentThread();
        clusterImpl = new ClusterImpl();
        final boolean inited = init();
        if (!inited)
            return;
        final Thread inThread = new Thread(InSelector.get(), "hz.InThread");
        inThread.start();
        inThread.setPriority(8);
        lsThreads.add(inThread);

        final Thread outThread = new Thread(OutSelector.get(), "hz.OutThread");
        outThread.start();
        outThread.setPriority(8);
        lsThreads.add(outThread);

        final Thread clusterServiceThread = new Thread(ClusterService.get(), "hz.ServiceThread");
        clusterServiceThread.start();
        clusterServiceThread.setPriority(7);
        lsThreads.add(clusterServiceThread);

        join();

        if (Config.get().join.multicastConfig.enabled) {
            startMulticastService();
        }
        firstMainThread = null;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.log(Level.FINEST, "Hazelcast ShutdownHook is shutting down!");
                shutdown();
            }
        });
    }

    public void startMulticastService() {
        final Thread multicastServiceThread = new Thread(MulticastService.get(), "hz.MulticastThread");
        multicastServiceThread.start();
        multicastServiceThread.setPriority(6);
    }

    public void unlock() {
        joined = true;
        synchronized (joinLock) {
            joinLock.notify();
        }
    }

    void setAsMaster() {
        masterAddress = address;
        if (DEBUG)
            logger.log(Level.FINEST, "adding member myself");
        ClusterManager.get().addMember(address, getLocalNodeType()); // add
        // myself
        clusterImpl.setMembers(ClusterManager.get().lsMembers);
        unlock();
    }

    private Address findMaster() {
        final Config config = Config.get();
        try {
            final String ip = System.getProperty("join.ip");
            if (ip == null) {
                JoinInfo joinInfo = new JoinInfo(true, address, config.groupName,
                        config.groupPassword, getLocalNodeType());
                for (int i = 0; i < 5; i++) {
                    MulticastService.get().send(joinInfo);
                    Thread.sleep(10);
                }
                JoinInfo respJoinInfo = null;
                boolean timedOut = false;
                while (!timedOut) {
                    respJoinInfo = MulticastService.get().receive();
                    if (respJoinInfo == null) {
                        timedOut = true;
                    } else if (!respJoinInfo.request && !respJoinInfo.address.equals(address)) {
                        timedOut = true;
                    }
                }
                if (respJoinInfo != null) {
                    masterAddress = respJoinInfo.address;
                    return masterAddress;
                } else {
                    joinInfo = new JoinInfo(false, address, config.groupName, config.groupPassword,
                            getLocalNodeType());
                    for (int i = 0; i < 5; i++) {
                        MulticastService.get().send(joinInfo);
                    }
                    return address;
                }

            } else {
                if (DEBUG)
                    logger.log(Level.FINEST, "RETURNING join.ip");
                return new Address(ip, config.port);
            }

        } catch (final Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private Address getAddressFor(final String host) {
        final Config config = Config.get();
        int port = config.port;
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
                    Address address = null;
                    if (config.interfaces.enabled) {
                        address = new Address(inetAddress.getAddress(), config.port);
                        shouldCheck = AddressPicker.matchAddress(address.getHost());
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

    private List<Address> getPossibleMembers(final List<String> lsJoinMembers) {
        final Config config = Config.get();
        final List<Address> lsPossibleAddresses = new ArrayList<Address>();
        for (final String host : lsJoinMembers) {
            // check if host is hostname of ip address
            final boolean ip = isIP(host);
            try {
                if (ip) {
                    for (int i = 0; i < 3; i++) {
                        final Address address = new Address(host, config.port + i, true);
                        lsPossibleAddresses.add(address);
                    }
                } else {
                    final InetAddress[] allAddresses = InetAddress.getAllByName(host);
                    for (final InetAddress inetAddress : allAddresses) {
                        boolean shouldCheck = true;
                        Address address = null;
                        if (config.interfaces.enabled) {
                            address = new Address(inetAddress.getAddress(), config.port);
                            shouldCheck = AddressPicker.matchAddress(address.getHost());
                        }
                        if (shouldCheck) {
                            for (int i = 0; i < 3; i++) {
                                final Address addressProper = new Address(inetAddress.getAddress(),
                                        config.port + i);
                                lsPossibleAddresses.add(addressProper);
                            }
                        }
                    }
                }
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
        return lsPossibleAddresses;
    }

    private boolean init() {
        try {
            final String preferIPv4Stack = System.getProperty("java.net.preferIPv4Stack");
            final String preferIPv6Address = System.getProperty("java.net.preferIPv6Addresses");
            if (preferIPv6Address == null && preferIPv4Stack == null) {
                System.setProperty("java.net.preferIPv4Stack", "true");
            }
            final Config config = Config.get();
            final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            address = AddressPicker.pickAddress(serverSocketChannel);
            address.setThisAddress(true);
            localMember = new MemberImpl(address, true, localNodeType);
            //initialize managers..
            ClusterService.get().start();
            ClusterManager.get();
            ConcurrentMapManager.get();
            BlockingQueueManager.get();
            ExecutorManager.get().start();
            ListenerManager.get();
            TopicManager.get();

            ClusterManager.get().addMember(localMember);
            InSelector.get().start();
            OutSelector.get().start();
            InSelector.get().setServerSocketChannel(serverSocketChannel);
            if (address == null)
                return false;
            Logger systemLogger = Logger.getLogger("com.hazelcast.system");
            systemLogger.log(Level.INFO, "Hazelcast " + Build.version + " ("
                    + Build.build + ") starting at " + address);
            systemLogger.log(Level.INFO, "Copyright (C) 2008 Hazelcast.com");

            if (config.join.multicastConfig.enabled) {
                final MulticastSocket multicastSocket = new MulticastSocket(null);
                multicastSocket.setReuseAddress(true);
                // bind to receive interface
                multicastSocket.bind(new InetSocketAddress(
                        config.join.multicastConfig.multicastPort));
                multicastSocket.setTimeToLive(32);
                // set the send interface
                multicastSocket.setInterface(address.getInetAddress());
                multicastSocket.setReceiveBufferSize(1 * 1024);
                multicastSocket.setSendBufferSize(1 * 1024);
                multicastSocket.joinGroup(InetAddress
                        .getByName(config.join.multicastConfig.multicastGroup));
                multicastSocket.setSoTimeout(1000);
                MulticastService.get().init(multicastSocket);
            }

        } catch (final Exception e) {
            dumpCore(e);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private void join() {
        final Config config = Config.get();
        if (!config.join.multicastConfig.enabled) {
            joinWithTCP();
        } else {
            joinWithMulticast();
        }
        if (DEBUG)
            logger.log(Level.FINEST, "Join DONE");
        ClusterManager.get().finalizeJoin();
        if (ClusterManager.get().lsMembers.size() == 1) {
            final StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append(ClusterManager.get());
            logger.log(Level.INFO, sb.toString());
        }
    }

    private void joinWithMulticast() {
        masterAddress = findMaster();
        if (DEBUG)
            logger.log(Level.FINEST, address + " master: " + masterAddress);
        if (masterAddress == null || masterAddress.equals(address)) {
            ClusterManager.get().addMember(address, getLocalNodeType()); // add
            // myself
            masterAddress = address;
            clusterImpl.setMembers(ClusterManager.get().lsMembers);
            unlock();
        } else {
            while (!joined) {
                try {
                    if (DEBUG)
                        logger.log(Level.FINEST, "joining... " + masterAddress);
                    synchronized (joinLock) {
                        joinExisting(masterAddress);
                        joinLock.wait(2000);
                    }
                    if (masterAddress == null) {
                        joinWithMulticast();
                    } else if (masterAddress.equals(address)) {
                        setAsMaster();
                    }
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void joinExisting(final Address masterAddress) throws Exception {
        Connection conn = ConnectionManager.get().getOrConnect(masterAddress);
        if (conn == null)
            Thread.sleep(1000);
        conn = ConnectionManager.get().getConnection(masterAddress);
        if (DEBUG) {
            logger.log(Level.FINEST, "Master connnection " + conn);
        }
        if (conn != null)
            ClusterManager.get().sendJoinRequest(masterAddress);
    }

    private void joinViaPossibleMembers() {
        final Config config = Config.get();
        try {
            final List<Address> lsPossibleAddresses = getPossibleMembers(config.join.joinMembers.lsMembers);
            lsPossibleAddresses.remove(address);
            for (final Address adrs : lsPossibleAddresses) {
                if (DEBUG)
                    logger.log(Level.FINEST, "connecting to " + adrs);
                ConnectionManager.get().getOrConnect(adrs);
            }
            boolean found = false;
            int numberOfSeconds = 0;
            connectionTimeout:
            while (!found
                    && numberOfSeconds < config.join.joinMembers.connectionTimeoutSeconds) {
                Address addressFailed = null;
                while ((addressFailed = qFailedConnections.poll()) != null) {
                    lsPossibleAddresses.remove(addressFailed);
                }
                if (lsPossibleAddresses.size() == 0)
                    break connectionTimeout;
                Thread.sleep(1000);
                numberOfSeconds++;
                int numberOfJoinReq = 0;
                for (final Address adrs : lsPossibleAddresses) {
                    final Connection conn = ConnectionManager.get().getOrConnect(adrs);
                    if (DEBUG)
                        logger.log(Level.FINEST, "conn " + conn);
                    if (conn != null && numberOfJoinReq < 5) {
                        found = true;
                        ClusterManager.get().sendJoinRequest(adrs);
                        numberOfJoinReq++;
                    }
                }
            }
            if (DEBUG)
                logger.log(Level.FINEST, "FOUND " + found);
            if (!found) {
                setAsMaster();
            } else {
                while (!joined) {
                    int numberOfJoinReq = 0;
                    for (final Address adrs : lsPossibleAddresses) {
                        final Connection conn = ConnectionManager.get().getOrConnect(adrs);
                        if (conn != null && numberOfJoinReq < 5) {
                            found = true;
                            ClusterManager.get().sendJoinRequest(adrs);
                            numberOfJoinReq++;
                        }
                    }
                    Thread.sleep(2000);
                    if (DEBUG) {
                        logger.log(Level.FINEST, masterAddress.toString());
                    }
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
            qFailedConnections.clear();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void joinViaRequiredMember() {

        try {
            final Config config = Config.get();
            final Address requiredAddress = getAddressFor(config.join.joinMembers.requiredMember);
            if (DEBUG) {
                logger.log(Level.FINEST, "Joining over required member " + requiredAddress);
            }
            if (requiredAddress == null) {
                throw new RuntimeException("Invalid required member "
                        + config.join.joinMembers.requiredMember);
            }
            if (requiredAddress.equals(address)) {
                setAsMaster();
                return;
            }
            ConnectionManager.get().getOrConnect(requiredAddress);
            Connection conn = null;
            while (conn == null) {
                conn = ConnectionManager.get().getOrConnect(requiredAddress);
                Thread.sleep(1000);
            }
            while (!joined) {
                final Connection connection = ConnectionManager.get().getOrConnect(requiredAddress);
                if (connection == null)
                    joinViaRequiredMember();
                if (DEBUG) {
                    logger.log(Level.FINEST, "Sending joinRequest " + requiredAddress);
                }
                ClusterManager.get().sendJoinRequest(requiredAddress);

                Thread.sleep(2000);
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void joinWithTCP() {
        final Config config = Config.get();
        if (config.join.joinMembers.requiredMember != null) {
            joinViaRequiredMember();
        } else {
            joinViaPossibleMembers();
        }
    }
}
