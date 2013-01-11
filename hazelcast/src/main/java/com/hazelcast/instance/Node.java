/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.ascii.TextCommandService;
import com.hazelcast.ascii.TextCommandServiceImpl;
import com.hazelcast.client.ClientCommandService;
import com.hazelcast.cluster.*;
import com.hazelcast.config.Config;
import com.hazelcast.config.Join;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.core.InstanceListener;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.logging.SystemLogService;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.*;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.Util;
import com.hazelcast.util.VersionCheck;
import com.hazelcast.wan.WanReplicationService;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public class Node {

    private final ILogger logger;

    private final AtomicBoolean joined = new AtomicBoolean(false);

    private volatile boolean active = false;

    private volatile boolean outOfMemory = false;

    private volatile boolean completelyShutdown = false;

    private final Set<Address> failedConnections = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private final NodeShutdownHookThread shutdownHookThread = new NodeShutdownHookThread("hz.ShutdownThread");

    private final boolean liteMember;

    private final NodeType localNodeType;

    public final SerializationServiceImpl serializationService;

    public final NodeEngineImpl nodeEngine;

    public final PartitionService partitionService;

    public final ClusterService clusterService;

    public final MulticastService multicastService;

    public final ConnectionManager connectionManager;

    public final TextCommandServiceImpl textCommandService;

    public final Config config;

    public final GroupProperties groupProperties;

    public final Address address;

    public final MemberImpl localMember;

    private volatile Address masterAddress = null;

    public final HazelcastInstanceImpl hazelcastInstance;

    private final int buildNumber;

    public final LoggingServiceImpl loggingService;

    public final ClientCommandService clientCommandService;

    //    public final ClientServiceImpl clientService;

    private final SystemLogService systemLogService;

    private final WanReplicationService wanReplicationService;

    private final Joiner joiner;

    final NodeInitializer initializer;

    private ManagementCenterService managementCenterService;

    public final SecurityContext securityContext;

    public final ThreadGroup threadGroup;

    public Node(HazelcastInstanceImpl hazelcastInstance, Config config) {
        ThreadContext.get().setCurrentInstance(hazelcastInstance);
        this.hazelcastInstance = hazelcastInstance;
        this.threadGroup = hazelcastInstance.threadGroup;
        this.config = config;
        this.groupProperties = new GroupProperties(config);
        this.liteMember = config.isLiteMember();
        this.localNodeType = (liteMember) ? NodeType.LITE_MEMBER : NodeType.MEMBER;
        serializationService = new SerializationServiceImpl(1, null, hazelcastInstance.managedContext);
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
        nodeEngine = new NodeEngineImpl(this);
        connectionManager = new ConnectionManager(new NodeIOService(this), serverSocketChannel);
        clusterService = new ClusterService(this);
        partitionService = new PartitionService(this);
        clientCommandService = new ClientCommandService(this);
//        clientHandlerService = new ClientHandlerService(this);
//        clientService = new ClientServiceImpl(concurrentMapManager);
        textCommandService = new TextCommandServiceImpl(this);
        clusterService.addMember(localMember);
        initializer.printNodeInfo(this);
        buildNumber = initializer.getBuildNumber();
        VersionCheck.check(this, initializer.getBuild(), initializer.getVersion());
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
                    listener = ClassLoaderUtil.newInstance(listenerCfg.getClassName());
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
            if (listener instanceof InstanceListener) {
                hazelcastInstance.addInstanceListener((InstanceListener) listener);
            } else if (listener instanceof MembershipListener) {
                clusterService.addMembershipListener((MembershipListener) listener);
            } else if (listener instanceof MigrationListener) {
                partitionService.addMigrationListener((MigrationListener) listener);
            } else if (listener instanceof LifecycleListener) {
                hazelcastInstance.lifecycleService.addLifecycleListener((LifecycleListener) listener);
            } else if (listener != null) {
                final String error = "Unknown listener type: " + listener.getClass();
                Throwable t = new IllegalArgumentException(error);
                logger.log(Level.WARNING, error, t);
            }
        }
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

    public SerializationService getSerializationService() {
        return serializationService;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public PartitionService getPartitionService() {
        return partitionService;
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
        return hazelcastInstance.getName();
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

    public void start() {
        logger.log(Level.FINEST, "We are asked to start and completelyShutdown is " + String.valueOf(completelyShutdown));
        if (completelyShutdown) return;
        nodeEngine.start();
        connectionManager.start();
        if (config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
            final Thread multicastServiceThread = new Thread(hazelcastInstance.threadGroup, multicastService, getThreadNamePrefix("MulticastThread"));
            multicastServiceThread.start();
        }
        setActive(true);
        if (!completelyShutdown) {
            logger.log(Level.FINEST, "Adding ShutdownHook");
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
        logger.log(Level.FINEST, "finished starting threads, calling join");
        join();
        int clusterSize = clusterService.getSize();
        if (config.getNetworkConfig().isPortAutoIncrement()
                && address.getPort() >= config.getNetworkConfig().getPort() + clusterSize) {
            StringBuilder sb = new StringBuilder("Config seed port is ");
            sb.append(config.getPort());
            sb.append(" and cluster size is ");
            sb.append(clusterSize);
            sb.append(". Some of the ports seem occupied!");
            logger.log(Level.WARNING, sb.toString());
        }
        try {
            managementCenterService = new ManagementCenterService(hazelcastInstance);
        } catch (Exception e) {
            logger.log(Level.WARNING, "ManagementCenterService could not be constructed!", e);
        }
        initializer.afterInitialize(this);
    }

    public void shutdown(final boolean force, final boolean now) {
        if (now) {
            doShutdown(force);
        } else {
            new Thread(new Runnable() {
                public void run() {
                    ThreadContext.get().setCurrentInstance(hazelcastInstance);
                    doShutdown(force);
                }
            }).start();
        }
    }

    private void doShutdown(boolean force) {
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
            } while (partitionService.hasActiveBackupTask() && ++waitSeconds < maxWaitSeconds);
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
            wanReplicationService.shutdown();
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
            } catch (Throwable ignored) {
            }
            if (managementCenterService != null) {
                managementCenterService.shutdown();
            }
//            logger.log(Level.FINEST, "Shutting down the clientHandlerService");
//            clientHandlerService.shutdown();
            logger.log(Level.FINEST, "Shutting down the node engine");
            nodeEngine.shutdown();
            if (multicastService != null) {
                logger.log(Level.FINEST, "Shutting down the multicast service");
                multicastService.stop();
            }
            logger.log(Level.FINEST, "Shutting down the connection manager");
            connectionManager.shutdown();
//            logger.log(Level.FINEST, "Shutting down the executorManager");
//            executorManager.stop();
            textCommandService.stop();
            masterAddress = null;
            if (securityContext != null) {
                securityContext.destroy();
            }
            initializer.destroy();
            serializationService.destroy();
            int numThreads = threadGroup.activeCount();
            Thread[] threads = new Thread[numThreads * 2];
            numThreads = threadGroup.enumerate(threads, false);
            for (int i = 0; i < numThreads; i++) {
                Thread thread = threads[i];
                logger.log(Level.FINEST, "Shutting down thread " + thread.getName());
                thread.interrupt();
            }
            failedConnections.clear();
            systemLogService.shutdown();
            ThreadContext.get().shutdown(hazelcastInstance);
            logger.log(Level.INFO, "Hazelcast Shutdown is completed in " + (Clock.currentTimeMillis() - start) + " ms.");
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

    public void inactivate() {
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
                Packet.PACKET_VERSION, buildNumber, clusterService.getSize(), 0, localMember.getUuid());
        if (withCredentials && securityContext != null) {
            Credentials c = securityContext.getCredentialsFactory().newCredentials();
            jr.setCredentials(c);
        }
        return jr;
    }

    public void rejoin() {
        systemLogService.logJoin("Rejoining!");
        masterAddress = null;
        joined.set(false);
        clusterService.reset();
        failedConnections.clear();
        join();
    }

    public void join() {
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

    public Joiner getJoiner() {
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
                Class clazz = Class.forName("com.hazelcast.impl.cluster.TcpIpJoinerOverAWS");
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

    public void setAsMaster() {
        logger.log(Level.FINEST, "This node is being set as the master");
        systemLogService.logJoin("No master node found! Setting this node as the master.");
        masterAddress = address;
        setJoined();
    }

    public Config getConfig() {
        return config;
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

    public String toString() {
        return "Node[" + getName() + "]";
    }
}
