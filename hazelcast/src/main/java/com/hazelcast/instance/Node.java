/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.cluster.Joiner;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.cluster.impl.ConfigCheck;
import com.hazelcast.cluster.impl.JoinMessage;
import com.hazelcast.cluster.impl.JoinRequest;
import com.hazelcast.cluster.impl.MulticastJoiner;
import com.hazelcast.cluster.impl.MulticastService;
import com.hazelcast.cluster.impl.TcpIpJoiner;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.TextCommandServiceImpl;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.VersionCheck;

import java.lang.reflect.Constructor;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.cluster.impl.MulticastService.createMulticastService;
import static com.hazelcast.instance.NodeShutdownHelper.shutdownNodeByFiringEvents;
import static com.hazelcast.util.UuidUtil.createMemberUuid;

public class Node {

    private static final AtomicReferenceFieldUpdater<Node, NodeState> NODE_STATE_UPDATER
            = AtomicReferenceFieldUpdater.newUpdater(Node.class, NodeState.class, "state");

    private final ILogger logger;

    private final AtomicBoolean joined = new AtomicBoolean(false);

    private volatile NodeState state;

    private final NodeShutdownHookThread shutdownHookThread = new NodeShutdownHookThread("hz.ShutdownThread");

    private final SerializationService serializationService;

    public final NodeEngineImpl nodeEngine;

    public final ClientEngineImpl clientEngine;

    public final InternalPartitionServiceImpl partitionService;

    public final ClusterServiceImpl clusterService;

    public final MulticastService multicastService;

    public final ConnectionManager connectionManager;

    public final TextCommandServiceImpl textCommandService;

    public final Config config;

    public final GroupProperties groupProperties;

    public final Address address;

    public final MemberImpl localMember;

    private volatile Address masterAddress;

    public final HazelcastInstanceImpl hazelcastInstance;

    public final LoggingServiceImpl loggingService;

    private final Joiner joiner;

    private final NodeExtension nodeExtension;

    private ManagementCenterService managementCenterService;

    public final SecurityContext securityContext;

    private final ClassLoader configClassLoader;

    private final BuildInfo buildInfo;

    private final VersionCheck versionCheck = new VersionCheck();

    private final HazelcastThreadGroup hazelcastThreadGroup;

    public Node(HazelcastInstanceImpl hazelcastInstance, Config config, NodeContext nodeContext) {
        this.hazelcastInstance = hazelcastInstance;
        this.config = config;
        this.configClassLoader = config.getClassLoader();
        this.groupProperties = new GroupProperties(config);
        this.buildInfo = BuildInfoProvider.getBuildInfo();

        String loggingType = groupProperties.getString(GroupProperty.LOGGING_TYPE);
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
            localMember = new MemberImpl(address, true, createMemberUuid(address), hazelcastInstance, memberAttributes);
            loggingService.setThisMember(localMember);
            logger = loggingService.getLogger(Node.class.getName());
            hazelcastThreadGroup = new HazelcastThreadGroup(hazelcastInstance.getName(), logger, configClassLoader);
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
            this.multicastService = createMulticastService(addressPicker.getBindAddress(), this, config, logger);
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

    public HazelcastThreadGroup getHazelcastThreadGroup() {
        return hazelcastThreadGroup;
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
            if (listener instanceof PartitionLostListener) {
                partitionService.addPartitionLostListener((PartitionLostListener) listener);
                known = true;
            }
            if (listener instanceof LifecycleListener) {
                hazelcastInstance.lifecycleService.addLifecycleListener((LifecycleListener) listener);
                known = true;
            }
            if (listener instanceof ClientListener) {
                String serviceName = ClientEngineImpl.SERVICE_NAME;
                nodeEngine.getEventService().registerLocalListener(serviceName, serviceName, listener);
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

    void start() {
        nodeEngine.start();
        connectionManager.start();
        if (config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
            final Thread multicastServiceThread = new Thread(
                    hazelcastThreadGroup.getInternalThreadGroup(), multicastService,
                    hazelcastThreadGroup.getThreadNamePrefix("MulticastThread"));
            multicastServiceThread.start();
        }
        if (groupProperties.getBoolean(GroupProperty.SHUTDOWNHOOK_ENABLED)) {
            logger.finest("Adding ShutdownHook");
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
        state = NodeState.ACTIVE;

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
        versionCheck.check(this, getBuildInfo().getVersion(), buildInfo.isEnterprise());
    }

    public void shutdown(final boolean terminate) {
        long start = Clock.currentTimeMillis();
        if (logger.isFinestEnabled()) {
            logger.finest("We are being asked to shutdown when state = " + state);
        }

        if (!NODE_STATE_UPDATER.compareAndSet(this, NodeState.ACTIVE, NodeState.SHUTTING_DOWN)) {
            waitIfAlreadyShuttingDown();
            return;
        }

        if (!terminate) {
            final int maxWaitSeconds = groupProperties.getSeconds(GroupProperty.GRACEFUL_SHUTDOWN_MAX_WAIT);
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
        setMasterAddress(null);
        try {
            if (groupProperties.getBoolean(GroupProperty.SHUTDOWNHOOK_ENABLED)) {
                Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
            }
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

        hazelcastThreadGroup.destroy();
        logger.info("Hazelcast Shutdown is completed in " + (Clock.currentTimeMillis() - start) + " ms.");
        state = NodeState.SHUT_DOWN;
    }

    private void waitIfAlreadyShuttingDown() {
        if (state == NodeState.SHUT_DOWN) {
            return;
        }

        if (state == NodeState.SHUTTING_DOWN) {
            logger.info("Node is already shutting down... Waiting for shutdown process to complete...");
            do {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    logger.warning("Interrupted while waiting for shutdown!");
                    return;
                }
            } while (state != NodeState.SHUT_DOWN);
        }
    }

    /**
     * Resets the internal cluster-state of the Node to be able to make it ready to join a new cluster.
     * After this method is called,
     * a new join process can be triggered by calling {@link #join()}.
     * <p/>
     * This method is called during merge process after a split-brain is detected.
     */
    public void reset() {
        setMasterAddress(null);
        joined.set(false);
        joiner.reset();
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
                if (state == NodeState.ACTIVE) {
                    logger.info("Running shutdown hook... Current state: " + state);
                    hazelcastInstance.getLifecycleService().terminate();
                }
            } catch (Exception e) {
                logger.warning(e);
            }
        }
    }

    public void setJoined() {
        joined.set(true);
    }

    public JoinMessage createSplitBrainJoinMessage() {
        return new JoinMessage(Packet.VERSION, buildInfo.getBuildNumber(), address, localMember.getUuid(),
                createConfigCheck(), clusterService.getMemberAddresses());
    }

    public JoinRequest createJoinRequest(boolean withCredentials) {
        final Credentials credentials = (withCredentials && securityContext != null)
                ? securityContext.getCredentialsFactory().newCredentials() : null;

        return new JoinRequest(Packet.VERSION, buildInfo.getBuildNumber(), address,
                localMember.getUuid(), createConfigCheck(), credentials,
                config.getMemberAttributeConfig().getAttributes());
    }

    public ConfigCheck createConfigCheck() {
        String joinerType = joiner == null ? "" : joiner.getType();
        return new ConfigCheck(config, joinerType);
    }

    public void join() {
        if (joiner == null) {
            logger.warning("No join method is enabled! Starting standalone.");
            setAsMaster();
            return;
        }

        try {
            masterAddress = null;
            joined.set(false);
            clusterService.reset();

            joiner.join();
        } catch (Throwable e) {
            logger.severe("Error while joining the cluster!", e);
        }

        if (!joined()) {
            long maxJoinTimeMillis = groupProperties.getMillis(GroupProperty.MAX_JOIN_SECONDS);
            logger.severe("Could not join cluster in " + maxJoinTimeMillis + " ms. Shutting down now!");
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
        this.getClusterService().getClusterClock().setClusterStartTime(Clock.currentTimeMillis());
        this.getClusterService().setClusterId(UuidUtil.createClusterUuid());
    }

    public Config getConfig() {
        return config;
    }

    /**
     * Returns the node state.
     *
     * @return current state of the node
     */
    public NodeState getState() {
        return state;
    }

    @Override
    public String toString() {
        return "Node[" + hazelcastInstance.getName() + "]";
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
