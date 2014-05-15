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

package com.hazelcast.impl;

import com.hazelcast.cluster.*;
import com.hazelcast.config.*;
import com.hazelcast.core.InstanceListener;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.impl.ascii.TextCommandService;
import com.hazelcast.impl.ascii.TextCommandServiceImpl;
import com.hazelcast.impl.base.*;
import com.hazelcast.impl.management.ManagementCenterService;
import com.hazelcast.impl.wan.WanReplicationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.*;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.util.*;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public class Node {
    private final ILogger logger;

    //private volatile boolean joined = false;
    private AtomicBoolean joined = new AtomicBoolean(false);

    private volatile boolean active = false;

    private volatile boolean outOfMemory = false;

    private volatile boolean completelyShutdown = false;

    private final ClusterImpl clusterImpl;

    private final Set<Address> failedConnections = new ConcurrentHashSet<Address>();

    private final NodeShutdownHookThread shutdownHookThread = new NodeShutdownHookThread("hz.ShutdownThread");

    private final boolean liteMember;

    private final NodeType localNodeType;

    final NodeBaseVariables baseVariables;

    public final ConcurrentMapManager concurrentMapManager;

    public final BlockingQueueManager blockingQueueManager;

    public final ClusterManager clusterManager;

    public final TopicManager topicManager;

    public final ListenerManager listenerManager;

    public final ClusterService clusterService;

    public final ExecutorManager executorManager;

    public final MulticastService multicastService;

    public final ConnectionManager connectionManager;

    public final ClientHandlerService clientHandlerService;

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

    public final ClientServiceImpl clientService;

    private final CpuUtilization cpuUtilization = new CpuUtilization();

    private final SystemLogService systemLogService;

    final SimpleBoundedQueue<Packet> serviceThreadPacketQueue = new SimpleBoundedQueue<Packet>(1000);

    final WanReplicationService wanReplicationService;

    final Joiner joiner;

    public final NodeInitializer initializer;

    private ManagementCenterService managementCenterService;

    public final SecurityContext securityContext;

    private final VersionCheck versionCheck = new VersionCheck();

    public Node(FactoryImpl factory, Config config) {
        ThreadContext.get().setCurrentFactory(factory);
        this.threadGroup = new ThreadGroup(factory.getName());
        this.factory = factory;
        this.config = config;
        this.groupProperties = new GroupProperties(config);
        this.liteMember = config.isLiteMember();
        this.localNodeType = (liteMember) ? NodeType.LITE_MEMBER : NodeType.MEMBER;
        systemLogService = new SystemLogService(this);
        final AddressPicker addressPicker = new AddressPicker(this);
        try {
            addressPicker.pickAddress();
        } catch (Throwable e) {
            Util.throwUncheckedException(e);
        }
        final ServerSocketChannel serverSocketChannel = addressPicker.getServerSocketChannel();
        address = addressPicker.getPublicAddress();
        localMember = new MemberImpl(address, true, localNodeType, UUID.randomUUID().toString());
        String loggingType = groupProperties.LOGGING_TYPE.getString();
        loggingService = new LoggingServiceImpl(systemLogService, config.getGroupConfig().getName(), loggingType, localMember);
        logger = loggingService.getLogger(Node.class.getName());
        initializer = NodeInitializerFactory.create();
        try {
            initializer.beforeInitialize(this);
        } catch (Throwable e) {
            try {
                serverSocketChannel.close();
            } catch (Throwable ignored) {
            }
            Util.throwUncheckedException(e);
        }
        securityContext = config.getSecurityConfig().isEnabled() ? initializer.getSecurityContext() : null;
        clusterImpl = new ClusterImpl(this);
        baseVariables = new NodeBaseVariables(address, localMember);
        //initialize managers..
        clusterService = new ClusterService(this);
        clusterService.start();
        executorManager = new ExecutorManager(this);
        connectionManager = new ConnectionManager(new NodeIOService(this), serverSocketChannel);
        clusterManager = new ClusterManager(this);
        clientHandlerService = new ClientHandlerService(this);
        concurrentMapManager = new ConcurrentMapManager(this);
        blockingQueueManager = new BlockingQueueManager(this);
        listenerManager = new ListenerManager(this);
        clientService = new ClientServiceImpl(concurrentMapManager);
        topicManager = new TopicManager(this);
        textCommandService = new TextCommandServiceImpl(this);
        clusterManager.addMember(false, localMember);
        initializer.printNodeInfo(this);
        buildNumber = initializer.getBuildNumber();
        versionCheck.check(this, initializer.getVersion(), initializer.getVersion().endsWith("-ee"));
        Join join = config.getNetworkConfig().getJoin();
        MulticastService mcService = null;
        try {
            if (join.getMulticastConfig().isEnabled()) {
                MulticastConfig multicastConfig = join.getMulticastConfig();
                MulticastSocket multicastSocket = new MulticastSocket(null);
                multicastSocket.setReuseAddress(true);
                // bind to receive interface
                multicastSocket.bind(new InetSocketAddress(multicastConfig.getMulticastPort()));
                multicastSocket.setTimeToLive(multicastConfig.getMulticastTimeToLive());
                // set the send interface
                try {
                    final Address bindAddress = addressPicker.getBindAddress();
                    multicastSocket.setInterface(bindAddress.getInetAddress());
                } catch (Exception e) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
                multicastSocket.setReceiveBufferSize(64 * 1024);
                multicastSocket.setSendBufferSize(64 * 1024);
                String multicastGroup = System.getProperty("hazelcast.multicast.group");
                if (multicastGroup == null) {
                    multicastGroup = multicastConfig.getMulticastGroup();
                }
                multicastConfig.setMulticastGroup(multicastGroup);
                multicastSocket.joinGroup(InetAddress.getByName(multicastGroup));
                multicastSocket.setSoTimeout(1000);
                mcService = new MulticastService(this, multicastSocket);
                mcService.addMulticastListener(new NodeMulticastListener(this));
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        this.multicastService = mcService;
        wanReplicationService = new WanReplicationService(this);
        initializeListeners(config);
        joiner = createJoiner();
    }

    private void initializeListeners(Config config) {
        for (final ListenerConfig listenerCfg : config.getListenerConfigs()) {
            Object listener = listenerCfg.getImplementation();
            if (listener == null) {
                try {
                    listener = Serializer.newInstance(Serializer.loadClass(listenerCfg.getClassName()));
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
            if (listener instanceof InstanceListener) {
                factory.addInstanceListener((InstanceListener) listener);
            } else if (listener instanceof MembershipListener) {
                clusterImpl.addMembershipListener((MembershipListener) listener);
            } else if (listener instanceof MigrationListener) {
                concurrentMapManager.partitionServiceImpl.addMigrationListener((MigrationListener) listener);
            } else if (listener instanceof LifecycleListener) {
                factory.lifecycleService.addLifecycleListener((LifecycleListener) listener);
            } else if (listener != null) {
                final String error = "Unknown listener type: " + listener.getClass();
                Throwable t = new IllegalArgumentException(error);
                logger.log(Level.WARNING, error, t);
            }
        }
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public ManagementCenterService getManagementCenterService() {
        return managementCenterService;
    }

    public SystemLogService getSystemLogService() {
        return systemLogService;
    }

    public void failedConnection(Address address) {
        logger.log(Level.FINEST, getThisAddress() + " failed connecting to " + address);
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

    public MemberImpl getLocalMember() {
        return localMember;
    }

    public String getName() {
        return factory.getName();
    }

    public String getThreadNamePrefix(String name) {
        return "hz." + getName() + "." + name;
    }

    public String getThreadPoolNamePrefix(String poolName) {
        return getThreadNamePrefix(poolName) + ".thread-";
    }

    public void handleInterruptedException(Thread thread, Exception e) {
        logger.log(Level.FINEST, thread.getName() + " is interrupted ", e);
    }

    public void checkNodeState() {
        if (!isActive()) {
            throw new IllegalStateException("Hazelcast Instance is not active!");
        }
    }

    public final boolean isLiteMember() {
        return liteMember;
    }

    public boolean joined() {
        return joined.get();
    }

    public boolean isMaster() {
        return address != null && address.equals(masterAddress);
    }

    public void setMasterAddress(final Address master) {
        if (master != null) {
            logger.log(Level.FINEST, "** setting master address to " + master.toString());
        }
        masterAddress = master;
    }

    public void cleanupServiceThread() {
        clusterManager.checkServiceThread();
        baseVariables.qServiceThreadPacketCache.clear();
        concurrentMapManager.reset();
        logger.log(Level.FINEST, "Shutting down the cluster manager");
        clusterManager.stop();
    }

    public void shutdown(final boolean force, final boolean now) {
        if (now) {
            doShutdown(force);
        } else {
            new Thread(new Runnable() {
                public void run() {
                    ThreadContext.get().setCurrentFactory(factory);
                    doShutdown(force);
                }
            }).start();
        }
    }

    void doShutdown(boolean force) {
        long start = Clock.currentTimeMillis();
        logger.log(Level.FINEST, "** we are being asked to shutdown when active = " + String.valueOf(active));
        if (!force && isActive()) {
            final int maxWaitSeconds = groupProperties.GRACEFUL_SHUTDOWN_MAX_WAIT.getInteger();
            int waitSeconds = 0;
            do {
                // Although a node closes its connections to other nodes during shutdown,
                // others will try to connect it to ensure that node is shutdown or terminated.
                // This initial wait before active backup check is to make sure this node aware of all
                // possible disconnecting nodes. Otherwise there may be a data race between
                // syncForDead events and node shutdown.
                // A better way of solving this issue is to make a node to inform others about its termination
                // by sending shutting down message to all others.
                try {
                    //noinspection BusyWait
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            } while (concurrentMapManager.partitionManager.hasActiveBackupTask() && ++waitSeconds < maxWaitSeconds);
            if (waitSeconds >= maxWaitSeconds) {
                logger.log(Level.WARNING, "Graceful shutdown could not be completed in " + maxWaitSeconds + " seconds!");
            }
        }
        if (isActive()) {
            // set the joined=false first so that
            // threads do not process unnecessary
            // events, such as remove address
            joined.set(false);
            setActive(false);
            setMasterAddress(null);
            versionCheck.shutdown();
            wanReplicationService.shutdown();
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
            } catch (Throwable ignored) {
            }
            if (managementCenterService != null) {
                managementCenterService.shutdown();
            }
            logger.log(Level.FINEST, "Shutting down the clientHandlerService");
            clientHandlerService.shutdown();
            // connections should be destroyed first of all (write queues may be flushed before sockets are closed)
            logger.log(Level.FINEST, "Shutting down the connection manager");
            connectionManager.shutdown();
            if (multicastService != null) {
                logger.log(Level.FINEST, "Shutting down the multicast service");
                multicastService.stop();
            }
            logger.log(Level.FINEST, "Shutting down the concurrentMapManager");
            concurrentMapManager.shutdown();
            logger.log(Level.FINEST, "Shutting down the cluster service");
            clusterService.stop();
            logger.log(Level.FINEST, "Shutting down the executorManager");
            executorManager.stop();
            textCommandService.stop();
            masterAddress = null;
            if (securityContext != null) {
                securityContext.destroy();
            }
            initializer.destroy();
            int numThreads = threadGroup.activeCount();
            Thread[] threads = new Thread[numThreads * 2];
            numThreads = threadGroup.enumerate(threads, false);
            for (int i = 0; i < numThreads; i++) {
                Thread thread = threads[i];
                logger.log(Level.FINEST, "Shutting down thread " + thread.getName());
                thread.interrupt();
            }
            failedConnections.clear();
            serviceThreadPacketQueue.clear();
            systemLogService.shutdown();
            ThreadContext.get().shutdown(this.factory);
            logger.log(Level.INFO, "Hazelcast Shutdown is completed in " + (Clock.currentTimeMillis() - start) + " ms.");
        }
    }

    public void start() {
        logger.log(Level.FINEST, "We are asked to start and completelyShutdown is " + String.valueOf(completelyShutdown));
        if (completelyShutdown) return;
        serviceThread = clusterService.getServiceThread();
        serviceThread.setPriority(groupProperties.SERVICE_THREAD_PRIORITY.getInteger());
        logger.log(Level.FINEST, "Starting thread " + serviceThread.getName());
        serviceThread.start();
        connectionManager.start();
        final NetworkConfig networkConfig = config.getNetworkConfig();
        if (networkConfig.getJoin().getMulticastConfig().isEnabled()) {
            final Thread multicastServiceThread = new Thread(threadGroup, multicastService, getThreadNamePrefix("MulticastThread"));
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
        if (address.getPort() >= networkConfig.getPort() + clusterSize) {
            StringBuilder sb = new StringBuilder("Config seed port is ");
            sb.append(networkConfig.getPort());
            sb.append(" and cluster size is ");
            sb.append(clusterSize);
            sb.append(". Some of the ports seem occupied!");
            logger.log(Level.WARNING, sb.toString());
        }
        try {
            managementCenterService = new ManagementCenterService(factory);
        } catch (Exception e) {
            logger.log(Level.WARNING, "ManagementCenterService could not be created!", e);
        }
        initializer.afterInitialize(this);

        HealthMonitorLevel level = HealthMonitorLevel.valueOf(groupProperties.HEALTH_MONITORING_LEVEL.getString());
        if(level!=HealthMonitorLevel.OFF){
            int delaySeconds = groupProperties.HEALTH_MONITORING_DELAY_SECONDS.getInteger();
            new HealthMonitor(this,level,delaySeconds).start();
        }
    }

    public void onRestart() {
        joined.set(false);
        joiner.reset();
        final String uuid = UUID.randomUUID().toString();
        logger.log(Level.FINEST, "Generated new UUID for local member: " + uuid);
        localMember.setUuid(uuid);
    }

    public ILogger getLogger(String name) {
        return loggingService.getLogger(name);
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

    void onOutOfMemory() {
        outOfMemory = true;
        joined.set(false);
        setActive(false);
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
                        shutdown(false, true);
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
        systemLogService.logJoin("setJoined() master: " + masterAddress);
    }

    public JoinInfo createJoinInfo() {
        return createJoinInfo(false);
    }

    public JoinInfo createJoinInfo(boolean withCredentials) {
        final JoinInfo jr = new JoinInfo(this.getLogger(JoinInfo.class.getName()), true, address, config, getLocalNodeType(),
                Packet.PACKET_VERSION, buildNumber, clusterImpl.getMembers().size(), 0, localMember.getUuid());
        if (withCredentials && securityContext != null) {
            Credentials c = securityContext.getCredentialsFactory().newCredentials();
            jr.setCredentials(c);
        }
        return jr;
    }

    public boolean validateJoinRequest(JoinRequest joinRequest) throws Exception {
        boolean valid = Packet.PACKET_VERSION == joinRequest.packetVersion;
        if (valid) {
            try {
                valid = config.isCompatible(joinRequest.config);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Invalid join request, reason:" + e.getMessage());
                systemLogService.logJoin("Invalid join request, reason:" + e.getMessage());
                throw e;
            }
        }
        return valid;
    }

    void rejoin() {
        systemLogService.logJoin("Rejoining!");
        masterAddress = null;
        joined.set(false);
        clusterImpl.reset();
        failedConnections.clear();
        join();
    }

    void join() {
        final long joinStartTime = joiner != null ? joiner.getStartTime() : Clock.currentTimeMillis();
        final long maxJoinMillis = getGroupProperties().MAX_JOIN_SECONDS.getInteger() * 1000;
        try {
            if (joiner == null) {
                logger.log(Level.WARNING, "No join method is enabled! Starting standalone.");
                setAsMaster();
            } else {
                joiner.join(joined);
            }
        } catch (Exception e) {
            if (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis) {
                logger.log(Level.WARNING, "Trying to rejoin: " + e.getMessage());
                rejoin();
            } else {
                logger.log(Level.SEVERE, "Could not join cluster, shutting down!", e);
                shutdown(false, true);
            }
        }
    }

    Joiner getJoiner() {
        return joiner;
    }

    Joiner createJoiner() {
        Join join = config.getNetworkConfig().getJoin();
        if (join.getMulticastConfig().isEnabled() && multicastService != null) {
            systemLogService.logJoin("Creating MulticastJoiner");
            return new MulticastJoiner(this);
        } else if (join.getTcpIpConfig().isEnabled()) {
            systemLogService.logJoin("Creating TcpIpJoiner");
            return new TcpIpJoiner(this);
        } else if (join.getAwsConfig().isEnabled()) {
            try {
                Class clazz = Class.forName("com.hazelcast.impl.TcpIpJoinerOverAWS");
                Constructor constructor = clazz.getConstructor(Node.class);
                systemLogService.logJoin("Creating AWSJoiner");
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
        systemLogService.logJoin("No master node found! Setting this node as the master.");
        masterAddress = address;
        clusterManager.enqueueAndWait(new Processable() {
            public void process() {
                clusterManager.addMember(address, getLocalNodeType(), localMember.getUuid()); // add
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

    public boolean isServiceThread() {
        return Thread.currentThread() == serviceThread;
    }

    public String toString() {
        return "Node[" + getName() + "]";
    }
}
