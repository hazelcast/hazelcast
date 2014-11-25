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

package com.hazelcast.instance;

import com.hazelcast.ascii.TextCommandService;
import com.hazelcast.ascii.TextCommandServiceImpl;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.cluster.impl.ConfigCheck;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.cluster.impl.JoinRequest;
import com.hazelcast.cluster.Joiner;
import com.hazelcast.cluster.impl.MulticastJoiner;
import com.hazelcast.cluster.impl.MulticastService;
import com.hazelcast.cluster.impl.NodeMulticastListener;
import com.hazelcast.cluster.impl.TcpIpJoiner;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.ProxyServiceImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.VersionCheck;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.NodeShutdownHelper.shutdownNodeByFiringEvents;

public class Node {

    private final ILogger logger;

    private final AtomicBoolean joined = new AtomicBoolean(false);

    private volatile boolean active;

    private volatile boolean completelyShutdown;

    private final NodeShutdownHookThread shutdownHookThread = new NodeShutdownHookThread("hz.ShutdownThread");

    private final SerializationService serializationService;

    public final NodeEngineImpl nodeEngine;

    public final ClientEngineImpl clientEngine;

    public final InternalPartitionService partitionService;

    public final ClusterServiceImpl clusterService;

    public final MulticastService multicastService;

    public final ConnectionManager connectionManager;

    public final TextCommandServiceImpl textCommandService;

    public final Config config;

    public final GroupProperties groupProperties;

    public final Address address;

    public final MemberImpl localMember;

    private volatile Address masterAddress = null;

    public final HazelcastInstanceImpl hazelcastInstance;

    public final LoggingServiceImpl loggingService;

    private final Joiner joiner;

    private final NodeExtension nodeExtension;

    private ManagementCenterService managementCenterService;

    public final SecurityContext securityContext;

    public final ThreadGroup threadGroup;

    private final ClassLoader configClassLoader;

    private final BuildInfo buildInfo;

    private final VersionCheck versionCheck = new VersionCheck();

    public Node(HazelcastInstanceImpl hazelcastInstance, Config config, NodeContext nodeContext) {
        this.hazelcastInstance = hazelcastInstance;
        this.threadGroup = hazelcastInstance.threadGroup;
        this.config = config;
        configClassLoader = config.getClassLoader();
        this.groupProperties = new GroupProperties(config);
        buildInfo = BuildInfoProvider.getBuildInfo();

        String loggingType = groupProperties.LOGGING_TYPE.getString();
        loggingService = new LoggingServiceImpl(config.getGroupConfig().getName(), loggingType, buildInfo);
        final AddressPicker addressPicker = nodeContext.createAddressPicker(this);
        try {
            addressPicker.pickAddress();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        }

        final ServerSocketChannel serverSocketChannel = addressPicker.getServerSocketChannel();
        try {
            address = addressPicker.getPublicAddress();
            final Map<String, Object> memberAttributes = findMemberAttributes(config.getMemberAttributeConfig().asReadOnly());
            localMember = new MemberImpl(address, true, UuidUtil.createMemberUuid(address), hazelcastInstance, memberAttributes);
            loggingService.setThisMember(localMember);
            logger = loggingService.getLogger(Node.class.getName());
            nodeExtension = NodeExtensionFactory.create(configClassLoader);
            nodeExtension.beforeStart(this);

            serializationService = nodeExtension.createSerializationService();
            securityContext = config.getSecurityConfig().isEnabled() ? nodeExtension.getSecurityContext() : null;
            nodeEngine = new NodeEngineImpl(this);
            clientEngine = new ClientEngineImpl(this);
            connectionManager = nodeContext.createConnectionManager(this, serverSocketChannel);
            partitionService = new InternalPartitionServiceImpl(this);
            clusterService = new ClusterServiceImpl(this);
            textCommandService = new TextCommandServiceImpl(this);
            nodeExtension.printNodeInfo(this);
            versionCheck.check(this, getBuildInfo().getVersion(), buildInfo.isEnterprise());
            this.multicastService = createMulticastService(addressPicker);
            initializeListeners(config);
            joiner = nodeContext.createJoiner(this);

        } catch (Throwable e) {
            try {
                serverSocketChannel.close();
            } catch (Throwable ignored) {
            }
            throw ExceptionUtil.rethrow(e);
        }
    }

    private MulticastService createMulticastService(AddressPicker addressPicker) {
        MulticastService mcService = null;
        try {
            JoinConfig join = config.getNetworkConfig().getJoin();
            if (join.getMulticastConfig().isEnabled()) {
                MulticastConfig multicastConfig = join.getMulticastConfig();
                MulticastSocket multicastSocket = new MulticastSocket(null);
                multicastSocket.setReuseAddress(true);
                // bind to receive interface
                multicastSocket.bind(new InetSocketAddress(multicastConfig.getMulticastPort()));
                multicastSocket.setTimeToLive(multicastConfig.getMulticastTimeToLive());
                try {
                    // set the send interface
                    final Address bindAddress = addressPicker.getBindAddress();
                    // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4417033
                    // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6402758
                    if (!bindAddress.getInetAddress().isLoopbackAddress()) {
                        multicastSocket.setInterface(bindAddress.getInetAddress());
                    }
                    else if (multicastConfig.isLoopbackModeEnabled()) {
                        multicastSocket.setLoopbackMode(true);
			            multicastSocket.setInterface(bindAddress.getInetAddress());
                    }
                } catch (Exception e) {
                    logger.warning(e);
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
            logger.severe(e);
        }
        return mcService;
    }

    private void initializeListeners(Config config) {
        for (final ListenerConfig listenerCfg : config.getListenerConfigs()) {
            Object listener = listenerCfg.getImplementation();
            if (listener == null) {
                try {
                    listener = ClassLoaderUtil.newInstance(configClassLoader, listenerCfg.getClassName());
                } catch (Exception e) {
                    logger.severe(e);
                }
            }
            if (listener instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) listener).setHazelcastInstance(hazelcastInstance);
            }
            boolean known = false;
            if (listener instanceof DistributedObjectListener) {
                final ProxyServiceImpl proxyService = (ProxyServiceImpl) nodeEngine.getProxyService();
                proxyService.addProxyListener((DistributedObjectListener) listener);
                known = true;
            }
            if (listener instanceof MembershipListener) {
                clusterService.addMembershipListener((MembershipListener) listener);
                known = true;
            }
            if (listener instanceof MigrationListener) {
                partitionService.addMigrationListener((MigrationListener) listener);
                known = true;
            }
            if (listener instanceof LifecycleListener) {
                hazelcastInstance.lifecycleService.addLifecycleListener((LifecycleListener) listener);
                known = true;
            }
            if (listener != null && !known) {
                final String error = "Unknown listener type: " + listener.getClass();
                Throwable t = new IllegalArgumentException(error);
                logger.warning(error, t);
            }
        }
    }

    public ManagementCenterService getManagementCenterService() {
        return managementCenterService;
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    public ClusterServiceImpl getClusterService() {
        return clusterService;
    }

    public InternalPartitionService getPartitionService() {
        return partitionService;
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

    public boolean joined() {
        return joined.get();
    }

    public boolean isMaster() {
        return address != null && address.equals(masterAddress);
    }

    public void setMasterAddress(final Address master) {
        if (master != null) {
            if (logger.isFinestEnabled()) {
                logger.finest("** setting master address to " + master);
            }
        }
        masterAddress = master;
    }

    public void start() {
        if (logger.isFinestEnabled()) {
            logger.finest("We are asked to start and completelyShutdown is " + String.valueOf(completelyShutdown));
        }
        if (completelyShutdown) return;
        nodeEngine.start();
        connectionManager.start();
        if (config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
            final Thread multicastServiceThread = new Thread(hazelcastInstance.threadGroup, multicastService, getThreadNamePrefix("MulticastThread"));
            multicastServiceThread.start();
        }
        setActive(true);
        if (!completelyShutdown) {
            logger.finest("Adding ShutdownHook");
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
        logger.finest("finished starting threads, calling join");
        join();
        int clusterSize = clusterService.getSize();
        if (config.getNetworkConfig().isPortAutoIncrement()
                && address.getPort() >= config.getNetworkConfig().getPort() + clusterSize) {
            StringBuilder sb = new StringBuilder("Config seed port is ");
            sb.append(config.getNetworkConfig().getPort());
            sb.append(" and cluster size is ");
            sb.append(clusterSize);
            sb.append(". Some of the ports seem occupied!");
            logger.warning(sb.toString());
        }
        try {
            managementCenterService = new ManagementCenterService(hazelcastInstance);
        } catch (Exception e) {
            logger.warning("ManagementCenterService could not be constructed!", e);
        }
        nodeExtension.afterStart(this);
    }

    public void shutdown(final boolean terminate) {
        long start = Clock.currentTimeMillis();
        if (logger.isFinestEnabled()) {
            logger.finest("** we are being asked to shutdown when active = " + String.valueOf(active));
        }
        if (!terminate && isActive() && joined()) {
            final int maxWaitSeconds = groupProperties.GRACEFUL_SHUTDOWN_MAX_WAIT.getInteger();
            if (!partitionService.prepareToSafeShutdown(maxWaitSeconds, TimeUnit.SECONDS)) {
                logger.warning("Graceful shutdown could not be completed in " + maxWaitSeconds + " seconds!");
            }
        }
        if (isActive()) {
            if (!terminate) {
                final int maxWaitSeconds = groupProperties.GRACEFUL_SHUTDOWN_MAX_WAIT.getInteger();
                if (!partitionService.prepareToSafeShutdown(maxWaitSeconds, TimeUnit.SECONDS)) {
                    logger.warning("Graceful shutdown could not be completed in " + maxWaitSeconds + " seconds!");
                }
                clusterService.sendShutdownMessage();
            } else {
                logger.warning("Terminating forcefully...");
            }
            // set the joined=false first so that
            // threads do not process unnecessary
            // events, such as remove address
            joined.set(false);
            setActive(false);
            setMasterAddress(null);
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
            } catch (Throwable ignored) {
            }
            versionCheck.shutdown();
            if (managementCenterService != null) {
                managementCenterService.shutdown();
            }

            textCommandService.stop();
            if (multicastService != null) {
                logger.info("Shutting down multicast service...");
                multicastService.stop();
            }
            logger.info("Shutting down connection manager...");
            connectionManager.shutdown();

            logger.info("Shutting down node engine...");
            nodeEngine.shutdown(terminate);

            if (securityContext != null) {
                securityContext.destroy();
            }
            nodeExtension.destroy();
            logger.finest("Destroying serialization service...");
            serializationService.destroy();

            int numThreads = threadGroup.activeCount();
            Thread[] threads = new Thread[numThreads * 2];
            numThreads = threadGroup.enumerate(threads, false);
            for (int i = 0; i < numThreads; i++) {
                Thread thread = threads[i];
                if (thread.isAlive()) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Shutting down thread " + thread.getName());
                    }
                    thread.interrupt();
                }
            }
            logger.info("Hazelcast Shutdown is completed in " + (Clock.currentTimeMillis() - start) + " ms.");
        }
    }

    public void onRestart() {
        joined.set(false);
        joiner.reset();
        final String uuid = UuidUtil.createMemberUuid(address);
        if (logger.isFinestEnabled()) {
            logger.finest("Generated new UUID for local member: " + uuid);
        }
        localMember.setUuid(uuid);
    }

    public ILogger getLogger(String name) {
        return loggingService.getLogger(name);
    }

    public ILogger getLogger(Class clazz) {
        return loggingService.getLogger(clazz);
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

    public ClassLoader getConfigClassLoader() {
        return configClassLoader;
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    public NodeExtension getNodeExtension() {
        return nodeExtension;
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
                        hazelcastInstance.getLifecycleService().terminate();
                    }
                } else {
                    logger.finest(
                            "shutdown hook - we are not --> active and not completely down so we are not calling shutdown");
                }
            } catch (Exception e) {
                logger.warning(e);
            }
        }
    }

    public void setJoined() {
        joined.set(true);
    }

    public JoinRequest createJoinRequest() {
        return createJoinRequest(false);
    }

    public JoinRequest createJoinRequest(boolean withCredentials) {
        final Credentials credentials = (withCredentials && securityContext != null)
                ? securityContext.getCredentialsFactory().newCredentials() : null;

        return new JoinRequest(Packet.VERSION, buildInfo.getBuildNumber(), address,
                localMember.getUuid(), createConfigCheck(), credentials, clusterService.getSize(), 0,
                config.getMemberAttributeConfig().getAttributes());
    }

    public ConfigCheck createConfigCheck() {
        String joinerType = joiner == null ? "" : joiner.getType();
        return new ConfigCheck(config, joinerType);
    }

    public void rejoin() {
        prepareForJoin();
        join();
    }

    private void prepareForJoin() {
        masterAddress = null;
        joined.set(false);
        clusterService.reset();
    }

    public void join() {
        if (joiner == null) {
            logger.warning("No join method is enabled! Starting standalone.");
            setAsMaster();
            return;
        }

        try {
            prepareForJoin();
            joiner.join();
        } catch (Throwable e) {
            logger.severe("Error while joining the cluster!", e);
        }

        if (!joined()) {
            long maxJoinTime = groupProperties.MAX_JOIN_SECONDS.getInteger() * 1000L;
            logger.severe("Could not join cluster in " + maxJoinTime + " ms. Shutting down now!");
            shutdownNodeByFiringEvents(Node.this, true);
        }
    }

    public Joiner getJoiner() {
        return joiner;
    }

    Joiner createJoiner() {
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.verify();

        if (join.getMulticastConfig().isEnabled() && multicastService != null) {
            logger.info("Creating MulticastJoiner");
            return new MulticastJoiner(this);
        } else if (join.getTcpIpConfig().isEnabled()) {
            logger.info("Creating TcpIpJoiner");
            return new TcpIpJoiner(this);
        } else if (join.getAwsConfig().isEnabled()) {
            Class clazz;
            try {
                logger.info("Creating AWSJoiner");
                clazz = Class.forName("com.hazelcast.cluster.impl.TcpIpJoinerOverAWS");
                Constructor constructor = clazz.getConstructor(Node.class);
                return (Joiner) constructor.newInstance(this);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return null;
    }


    public void setAsMaster() {
        logger.finest("This node is being set as the master");
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

    public String toString() {
        return "Node[" + getName() + "]";
    }

    public BuildInfo getBuildInfo() {
        return buildInfo;
    }

    private Map<String, Object> findMemberAttributes(MemberAttributeConfig attributeConfig) {
        Map<String, Object> attributes = new HashMap<String, Object>(attributeConfig.getAttributes());
        Properties properties = System.getProperties();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("hazelcast.member.attribute.")) {
                String shortKey = key.substring("hazelcast.member.attribute.".length());
                String value = properties.getProperty(key);
                attributes.put(shortKey, value);
            }
        }
        return attributes;
    }
}
