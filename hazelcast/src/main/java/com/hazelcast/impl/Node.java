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
import com.hazelcast.config.Join;
import com.hazelcast.impl.ascii.TextCommandService;
import com.hazelcast.impl.ascii.TextCommandServiceImpl;
import com.hazelcast.impl.base.CpuUtilization;
import com.hazelcast.impl.base.VersionCheck;
import com.hazelcast.impl.wan.WanReplicationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.*;
import com.hazelcast.util.NoneStrictObjectPool;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class Node {
    private final ILogger logger;

    //private volatile boolean joined = false;
    private AtomicBoolean joined = new AtomicBoolean(false);

    private volatile boolean active = false;

    private volatile boolean outOfMemory = false;

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

    public final TextCommandServiceImpl textCommandService;

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

    private final static AtomicInteger counter = new AtomicInteger();

    private final CpuUtilization cpuUtilization = new CpuUtilization();

    private final ServerSocketChannel serverSocketChannel;

    final int id;

    final WanReplicationService wanReplicationService;

    final Joiner joiner;

    public Node(FactoryImpl factory, Config config) {
        this.id = counter.incrementAndGet();
        this.threadGroup = new ThreadGroup(factory.getName());
        this.factory = factory;
        this.config = config;
        this.groupProperties = new GroupProperties(config);
        this.superClient = config.isSuperClient();
        this.localNodeType = (superClient) ? NodeType.SUPER_CLIENT : NodeType.MEMBER;
        String version = System.getProperty("hazelcast.version", "unknown");
        String build = System.getProperty("hazelcast.build", "unknown");
        if ("unknown".equals(version) || "unknown".equals(build)) {
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
        ServerSocketChannel serverSocketChannelTemp = null;
        Address localAddress = null;
        try {
            final String preferIPv4Stack = System.getProperty("java.net.preferIPv4Stack");
            final String preferIPv6Address = System.getProperty("java.net.preferIPv6Addresses");
            if (preferIPv6Address == null && preferIPv4Stack == null) {
                System.setProperty("java.net.preferIPv4Stack", "true");
            }
            serverSocketChannelTemp = ServerSocketChannel.open();
            AddressPicker addressPicker = new AddressPicker(this, serverSocketChannelTemp);
            localAddress = addressPicker.pickAddress();
            localAddress.setThisAddress(true);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        serverSocketChannel = serverSocketChannelTemp;
        address = localAddress;
        localMember = new MemberImpl(address, true, localNodeType);
        packetPool = new NoneStrictObjectPool<Packet>(2000) {
            @Override
            public Packet obtain() {
                return createNew();
            }

            @Override
            public boolean release(Packet packet) {
                return true;
            }

            @Override
            public void onObtain(Packet packet) {
            }

            @Override
            public void onRelease(Packet packet) {
            }

            public Packet createNew() {
                return new Packet();
            }
        };
        this.loggingService = new LoggingServiceImpl(config.getGroupConfig().getName(), localMember);
        this.logger = loggingService.getLogger(Node.class.getName());
        clusterImpl = new ClusterImpl(this, localMember);
        baseVariables = new NodeBaseVariables(address, localMember);
        //initialize managers..
        clusterService = new ClusterService(this);
        clusterService.start();
        inSelector = new InSelector(this, serverSocketChannel);
        outSelector = new OutSelector(this);
        connectionManager = new ConnectionManager(this);
        clusterManager = new ClusterManager(this);
        executorManager = new ExecutorManager(this);
        clientService = new ClientService(this);
        concurrentMapManager = new ConcurrentMapManager(this);
        blockingQueueManager = new BlockingQueueManager(this);
        listenerManager = new ListenerManager(this);
        topicManager = new TopicManager(this);
        textCommandService = new TextCommandServiceImpl(this);
        clusterManager.addMember(false, localMember);
        ILogger systemLogger = getLogger("com.hazelcast.system");
        systemLogger.log(Level.INFO, "Hazelcast " + version + " ("
                + build + ") starting at " + address);
        systemLogger.log(Level.INFO, "Copyright (C) 2008-2011 Hazelcast.com");
        VersionCheck.check(this, build, version);
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
                multicastSocket.setReceiveBufferSize(64 * 1024);
                multicastSocket.setSendBufferSize(64 * 1024);
                multicastSocket.joinGroup(InetAddress
                        .getByName(join.getMulticastConfig().getMulticastGroup()));
                multicastSocket.setSoTimeout(1000);
                mcService = new MulticastService(this, multicastSocket);
                mcService.addMulticastListener(new NodeMulticastListener(this));
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        this.multicastService = mcService;
        wanReplicationService = null; //new WanReplicationService(this);
        joiner = createJoiner();
    }

    public void failedConnection(Address address) {
        logger.log(Level.WARNING, getThisAddress() + "failed " + address);
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

    public String getThreadPoolNamePrefix(String poolName) {
        return "hz." + id + ".threads." + getName() + "." + poolName + ".thread-";
    }

    public void handleInterruptedException(Thread thread, Exception e) {
        logger.log(Level.FINEST, thread.getName() + " is interrupted ", e);
    }

    public void checkNodeState() {
        if (factory.restarted) {
            throw new IllegalStateException("Hazelcast Instance is restarted!");
        } else if (!isActive()) {
            throw new IllegalStateException("Hazelcast Instance is not active!");
        }
    }

    public final boolean isSuperClient() {
        return superClient;
    }

    public boolean joined() {
        return joined.get();
    }

    public boolean isMaster() {
        return address != null && address.equals(masterAddress);
    }

    public void setMasterAddress(final Address master) {
        if (master != null) {
            logger.log(Level.FINE, "** setting master address to " + master.toString());
        }
        masterAddress = master;
    }

    public void cleanupServiceThread() {
        clusterManager.checkServiceThread();
        baseVariables.qServiceThreadPacketCache.clear();
        concurrentMapManager.reset();
        clusterManager.stop();
    }

    public void shutdown() {
        shutdown(false);
    }

    public void shutdown(boolean force) {
        logger.log(Level.FINE, "** we are being asked to shutdown when active = " + String.valueOf(active));
        while (!force && isActive() && concurrentMapManager.partitionManager.hasActiveBackupTask()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        if (isActive()) {
            // set the joined=false first so that
            // threads do not process unnecessary
            // events, such as remove address
            long start = System.currentTimeMillis();
            joined.set(false);
            setActive(false);
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
            } catch (Throwable ignored) {
            }
            logger.log(Level.FINEST, "Shutting down the clientService");
            clientService.shutdown();
            logger.log(Level.FINEST, "Shutting down the NIO socket selector for input");
            inSelector.shutdown();
            logger.log(Level.FINEST, "Shutting down the NIO socket selector for output");
            outSelector.shutdown();
            logger.log(Level.FINEST, "Shutting down the cluster service");
            concurrentMapManager.shutdown();
            clusterService.stop();
            logger.log(Level.FINEST, "Shutting down the query service");
            if (multicastService != null) {
                multicastService.stop();
            }
            logger.log(Level.FINEST, "Shutting down the connection manager");
            connectionManager.shutdown();
            logger.log(Level.FINEST, "Shutting down the concurrentMapManager");
            logger.log(Level.FINEST, "Shutting down the executorManager");
            executorManager.stop();
            textCommandService.stop();
            masterAddress = null;
            packetPool.clear();
            logger.log(Level.FINEST, "Shutting down the cluster manager");
            int numThreads = threadGroup.activeCount();
            Thread[] threads = new Thread[numThreads * 2];
            numThreads = threadGroup.enumerate(threads, false);
            for (int i = 0; i < numThreads; i++) {
                Thread thread = threads[i];
                logger.log(Level.FINEST, "Shutting down thread " + thread.getName());
                thread.interrupt();
            }
            logger.log(Level.INFO, "Hazelcast Shutdown is completed in " + (System.currentTimeMillis() - start) + " ms.");
        }
    }

    public void start() {
        logger.log(Level.FINEST, "We are asked to start and completelyShutdown is " + String.valueOf(completelyShutdown));
        if (completelyShutdown) return;
        final String prefix = "hz." + this.id + ".";
        Thread inThread = new Thread(threadGroup, inSelector, prefix + "InThread");
        inThread.setPriority(groupProperties.IN_THREAD_PRIORITY.getInteger());
        logger.log(Level.FINEST, "Starting thread " + inThread.getName());
        inThread.start();
        Thread outThread = new Thread(threadGroup, outSelector, prefix + "OutThread");
        outThread.setPriority(groupProperties.OUT_THREAD_PRIORITY.getInteger());
        logger.log(Level.FINEST, "Starting thread " + outThread.getName());
        outThread.start();
        serviceThread = new Thread(threadGroup, clusterService, prefix + "ServiceThread");
        serviceThread.setPriority(groupProperties.SERVICE_THREAD_PRIORITY.getInteger());
        logger.log(Level.FINEST, "Starting thread " + serviceThread.getName());
        serviceThread.start();
        if (config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
            final Thread multicastServiceThread = new Thread(threadGroup, multicastService, prefix + "MulticastThread");
            multicastServiceThread.start();
        }
        setActive(true);
        if (!completelyShutdown) {
            logger.log(Level.FINEST, "Adding ShutdownHook");
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
        logger.log(Level.FINEST, "finished starting threads, calling join");
        join();
        int clusterSize = clusterImpl.getMembers().size();
        if (address.getPort() >= config.getPort() + clusterSize) {
            StringBuilder sb = new StringBuilder("Config seed port is ");
            sb.append(config.getPort());
            sb.append(" and cluster size is ");
            sb.append(clusterSize);
            sb.append(". Some of the ports seem occupied!");
            logger.log(Level.WARNING, sb.toString());
        }
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

    public TextCommandService getTextCommandService() {
        return textCommandService;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void onOutOfMemory(OutOfMemoryError e) {
        try {
            if (serverSocketChannel != null) {
                try {
                    serverSocketChannel.close();
                    connectionManager.shutdown();
                    shutdown(true);
                } catch (Throwable ignored) {
                }
            }
        } finally {
            active = false;
            outOfMemory = true;
            e.printStackTrace();
        }
    }

    public Set<Address> getFailedConnections() {
        return failedConnections;
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
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    public void setJoined() {
        joined.set(true);
    }

    public JoinInfo createJoinInfo() {
        return new JoinInfo(this.getLogger(JoinInfo.class.getName()), true, address, config, getLocalNodeType(),
                Packet.PACKET_VERSION, buildNumber, clusterImpl.getMembers().size());
    }

    public boolean validateJoinRequest(JoinRequest joinRequest) throws Exception {
        boolean valid = Packet.PACKET_VERSION == joinRequest.packetVersion &&
                buildNumber == joinRequest.buildNumber;
        if (valid) {
            try {
                valid = config.isCompatible(joinRequest.config);
            } catch (Exception e) {
                logger.log(Level.FINEST, "Invalid join request, reason:" + e.getMessage());
                throw e;
            }
        }
        return valid;
    }

    void rejoin() {
        masterAddress = null;
        joined.set(false);
        clusterImpl.reset();
        failedConnections.clear();
        join();
    }

    void join() {
        try {
            if (joiner == null) {
                logger.log(Level.WARNING, "No join method is enabled! Starting standalone.");
                setAsMaster();
            } else {
                joiner.join(joined);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage());
            factory.lifecycleService.restart();
        }
    }

    Joiner getJoiner() {
        return joiner;
    }

    Joiner createJoiner() {
        Join join = config.getNetworkConfig().getJoin();
        if (join.getMulticastConfig().isEnabled() && multicastService != null) {
            return new MulticastJoiner(this);
        } else if (join.getTcpIpConfig().isEnabled()) {
            return new TcpIpJoiner(this);
        } else if (join.getAwsConfig().isEnabled()) {
            try {
                Class clazz = Class.forName("com.hazelcast.impl.TcpIpJoinerOverAWS");
                Constructor constructor = clazz.getConstructor(Node.class);
                return (Joiner) constructor.newInstance(this);
            } catch (Exception e) {
                logger.log(Level.WARNING, e.getMessage());
                return null;
            }
        }
        return null;
    }

    void setAsMaster() {
        logger.log(Level.FINEST, "This node is being set as the master");
        masterAddress = address;
        logger.log(Level.FINEST, "adding member myself");
        clusterManager.enqueueAndWait(new Processable() {
            public void process() {
                clusterManager.addMember(address, getLocalNodeType()); // add
                // myself
                clusterImpl.setMembers(baseVariables.lsMembers);
            }
        }, 5);
        setJoined();
    }

    public Config getConfig() {
        return config;
    }

    public ExecutorManager getExecutorManager() {
        return executorManager;
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

    public boolean isOutOfMemory() {
        return outOfMemory;
    }

    public CpuUtilization getCpuUtilization() {
        return cpuUtilization;
    }

    public String toString() {
        return "Node[" + getName() + "]";
    }
}
