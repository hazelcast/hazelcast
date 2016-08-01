/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Joiner;
import com.hazelcast.cluster.impl.TcpIpJoiner;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
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
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.ConfigCheck;
import com.hazelcast.internal.cluster.impl.DiscoveryJoiner;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.cluster.impl.MulticastJoiner;
import com.hazelcast.internal.cluster.impl.MulticastService;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.internal.partition.impl.InternalMigrationListener;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.PhoneHome;
import com.hazelcast.util.UuidUtil;

import java.lang.reflect.Constructor;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.NodeShutdownHelper.shutdownNodeByFiringEvents;
import static com.hazelcast.internal.cluster.impl.MulticastService.createMulticastService;
import static com.hazelcast.spi.properties.GroupProperty.DISCOVERY_SPI_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.GRACEFUL_SHUTDOWN_MAX_WAIT;
import static com.hazelcast.spi.properties.GroupProperty.LOGGING_TYPE;
import static com.hazelcast.spi.properties.GroupProperty.MAX_JOIN_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.SHUTDOWNHOOK_ENABLED;
import static com.hazelcast.util.UuidUtil.createMemberUuid;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:visibilitymodifier", "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity"})
@PrivateApi
public class Node {

    private static final int THREAD_SLEEP_DURATION_MS = 500;

    public final HazelcastInstanceImpl hazelcastInstance;

    public final Config config;

    public final NodeEngineImpl nodeEngine;
    public final ClientEngineImpl clientEngine;

    public final InternalPartitionServiceImpl partitionService;
    public final ClusterServiceImpl clusterService;
    public final MulticastService multicastService;
    public final DiscoveryService discoveryService;
    public final TextCommandServiceImpl textCommandService;
    public final LoggingServiceImpl loggingService;

    public final ConnectionManager connectionManager;

    public final Address address;
    public final MemberImpl localMember;

    public final SecurityContext securityContext;

    private final ILogger logger;

    private final AtomicBoolean joined = new AtomicBoolean(false);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private final NodeShutdownHookThread shutdownHookThread = new NodeShutdownHookThread("hz.ShutdownThread");

    private final PhoneHome phoneHome = new PhoneHome();

    private final InternalSerializationService serializationService;
    private final ClassLoader configClassLoader;

    private final NodeExtension nodeExtension;

    private final HazelcastProperties properties;
    private final BuildInfo buildInfo;

    private final HazelcastThreadGroup hazelcastThreadGroup;

    private final Joiner joiner;

    private final boolean liteMember;

    private ManagementCenterService managementCenterService;

    private volatile NodeState state;

    private volatile Address masterAddress;

    @SuppressWarnings("checkstyle:executablestatementcount")
    public Node(HazelcastInstanceImpl hazelcastInstance, Config config, NodeContext nodeContext) {
        this.hazelcastInstance = hazelcastInstance;
        this.config = config;
        this.liteMember = config.isLiteMember();
        this.configClassLoader = config.getClassLoader();
        this.properties = new HazelcastProperties(config);
        this.buildInfo = BuildInfoProvider.getBuildInfo();

        String loggingType = properties.getString(LOGGING_TYPE);
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
            localMember = new MemberImpl(address, true, createMemberUuid(address),
                    hazelcastInstance, memberAttributes, liteMember);
            loggingService.setThisMember(localMember);
            logger = loggingService.getLogger(Node.class.getName());
            hazelcastThreadGroup = new HazelcastThreadGroup(hazelcastInstance.getName(), logger, configClassLoader);

            this.nodeExtension = createNodeExtension(nodeContext);
            nodeExtension.printNodeInfo();
            nodeExtension.beforeStart();

            serializationService = nodeExtension.createSerializationService();
            securityContext = config.getSecurityConfig().isEnabled() ? nodeExtension.getSecurityContext() : null;

            nodeEngine = new NodeEngineImpl(this);

            clientEngine = new ClientEngineImpl(this);
            connectionManager = nodeContext.createConnectionManager(this, serverSocketChannel);
            partitionService = new InternalPartitionServiceImpl(this);
            clusterService = new ClusterServiceImpl(this);
            textCommandService = new TextCommandServiceImpl(this);
            multicastService = createMulticastService(addressPicker.getBindAddress(), this, config, logger);
            discoveryService = createDiscoveryService(config);
            joiner = nodeContext.createJoiner(this);
        } catch (Throwable e) {
            try {
                serverSocketChannel.close();
            } catch (Throwable ignored) {
                EmptyStatement.ignore(ignored);
            }
            throw ExceptionUtil.rethrow(e);
        }
    }

    public HazelcastThreadGroup getHazelcastThreadGroup() {
        return hazelcastThreadGroup;
    }

    private DiscoveryService createDiscoveryService(Config config) {
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        DiscoveryConfig discoveryConfig = joinConfig.getDiscoveryConfig().getAsReadOnly();

        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }
        ILogger logger = getLogger(DiscoveryService.class);

        DiscoveryServiceSettings settings = new DiscoveryServiceSettings()
                .setConfigClassLoader(configClassLoader)
                .setLogger(logger)
                .setDiscoveryMode(DiscoveryMode.Member)
                .setDiscoveryConfig(discoveryConfig).setDiscoveryNode(
                        new SimpleDiscoveryNode(localMember.getAddress(), localMember.getAttributes()));

        return factory.newDiscoveryService(settings);
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
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

            if (listener instanceof InternalMigrationListener) {
                final InternalPartitionServiceImpl partitionService =
                        (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
                partitionService.setInternalMigrationListener((InternalMigrationListener) listener);
                known = true;
            }

            if (nodeExtension.registerListener(listener)) {
                known = true;
            }

            if (listener != null && !known) {
                final String error = "Unknown listener type: " + listener.getClass();
                Throwable t = new IllegalArgumentException(error);
                logger.warning(error, t);
            }
        }
    }

    protected NodeExtension createNodeExtension(NodeContext nodeContext) {
        return nodeContext.createNodeExtension(this);
    }

    public ManagementCenterService getManagementCenterService() {
        return managementCenterService;
    }

    public InternalSerializationService getSerializationService() {
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
        initializeListeners(config);
        connectionManager.start();
        if (config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
            final Thread multicastServiceThread = new Thread(
                    hazelcastThreadGroup.getInternalThreadGroup(), multicastService,
                    hazelcastThreadGroup.getThreadNamePrefix("MulticastThread"));
            multicastServiceThread.start();
        }
        if (properties.getBoolean(DISCOVERY_SPI_ENABLED)) {
            discoveryService.start();

            // Discover local metadata from environment and merge into member attributes
            mergeEnvironmentProvidedMemberMetadata();
        }

        if (properties.getBoolean(SHUTDOWNHOOK_ENABLED)) {
            logger.finest("Adding ShutdownHook");
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
        state = NodeState.ACTIVE;

        nodeExtension.beforeJoin();
        join();
        int clusterSize = clusterService.getSize();
        if (config.getNetworkConfig().isPortAutoIncrement()
                && address.getPort() >= config.getNetworkConfig().getPort() + clusterSize) {
            logger.warning("Config seed port is " + config.getNetworkConfig().getPort()
                    + " and cluster size is " + clusterSize + ". Some of the ports seem occupied!");
        }
        try {
            managementCenterService = new ManagementCenterService(hazelcastInstance);
        } catch (Exception e) {
            logger.warning("ManagementCenterService could not be constructed!", e);
        }
        nodeExtension.afterStart();
        phoneHome.check(this, getBuildInfo().getVersion(), buildInfo.isEnterprise());
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    public void shutdown(final boolean terminate) {
        long start = Clock.currentTimeMillis();
        if (logger.isFinestEnabled()) {
            logger.finest("We are being asked to shutdown when state = " + state);
        }

        if (!setShuttingDown()) {
            waitIfAlreadyShuttingDown();
            return;
        }

        if (!terminate) {
            final int maxWaitSeconds = properties.getSeconds(GRACEFUL_SHUTDOWN_MAX_WAIT);
            if (!partitionService.prepareToSafeShutdown(maxWaitSeconds, TimeUnit.SECONDS)) {
                logger.warning("Graceful shutdown could not be completed in " + maxWaitSeconds + " seconds!");
            }
            try {
                clusterService.sendShutdownMessage();
                if (logger.isFinestEnabled()) {
                    logger.finest("Shutdown message sent to other members");
                }
            } catch (Throwable t) {
                EmptyStatement.ignore(t);
            }
        } else {
            logger.warning("Terminating forcefully...");
        }

        // set the joined=false first so that
        // threads do not process unnecessary
        // events, such as remove address
        joined.set(false);
        setMasterAddress(null);
        try {
            if (properties.getBoolean(SHUTDOWNHOOK_ENABLED)) {
                Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
            }
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        }

        try {
            discoveryService.destroy();
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        }

        try {
            shutdownServices(terminate);
            state = NodeState.SHUT_DOWN;
            logger.info("Hazelcast Shutdown is completed in " + (Clock.currentTimeMillis() - start) + " ms.");
        } finally {
            if (state != NodeState.SHUT_DOWN) {
                shuttingDown.compareAndSet(true, false);
            }
        }
    }

    private void shutdownServices(boolean terminate) {
        nodeExtension.beforeShutdown();
        phoneHome.shutdown();
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
        logger.finest("Destroying serialization service...");
        serializationService.dispose();

        hazelcastThreadGroup.destroy();
        nodeExtension.shutdown();
    }

    private void mergeEnvironmentProvidedMemberMetadata() {
        Map<String, Object> metadata = discoveryService.discoverLocalMetadata();
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Byte) {
                localMember.setByteAttribute(entry.getKey(), (Byte) value);
            } else if (value instanceof Short) {
                localMember.setShortAttribute(entry.getKey(), (Short) value);
            } else if (value instanceof Integer) {
                localMember.setIntAttribute(entry.getKey(), (Integer) value);
            } else if (value instanceof Long) {
                localMember.setLongAttribute(entry.getKey(), (Long) value);
            } else if (value instanceof Float) {
                localMember.setFloatAttribute(entry.getKey(), (Float) value);
            } else if (value instanceof Double) {
                localMember.setDoubleAttribute(entry.getKey(), (Double) value);
            } else if (value instanceof Boolean) {
                localMember.setBooleanAttribute(entry.getKey(), (Boolean) value);
            } else {
                localMember.setStringAttribute(entry.getKey(), value.toString());
            }
        }
    }

    private boolean setShuttingDown() {
        if (shuttingDown.compareAndSet(false, true)) {
            state = NodeState.PASSIVE;
            return true;
        }
        return false;
    }

    /**
     * Indicates that node is not shutting down or it has not already shut down
     *
     * @return true if node is not shutting down or it has not already shut down
     */
    public boolean isRunning() {
        return !shuttingDown.get();
    }

    private void waitIfAlreadyShuttingDown() {
        if (!shuttingDown.get()) {
            return;
        }
        logger.info("Node is already shutting down... Waiting for shutdown process to complete...");
        while (state != NodeState.SHUT_DOWN && shuttingDown.get()) {
            try {
                Thread.sleep(THREAD_SLEEP_DURATION_MS);
            } catch (InterruptedException e) {
                logger.warning("Interrupted while waiting for shutdown!");
                return;
            }
        }
        if (state != NodeState.SHUT_DOWN) {
            throw new IllegalStateException("Node failed to shutdown!");
        }
    }

    public void changeNodeStateToActive() {
        final ClusterState clusterState = clusterService.getClusterState();
        if (clusterState == ClusterState.PASSIVE) {
            throw new IllegalStateException("This method can be called only when cluster-state is not " + clusterState);
        }
        state = NodeState.ACTIVE;
    }

    public void changeNodeStateToPassive() {
        final ClusterState clusterState = clusterService.getClusterState();
        if (clusterState != ClusterState.PASSIVE) {
            throw new IllegalStateException("This method can be called only when cluster-state is " + clusterState);
        }
        state = NodeState.PASSIVE;
    }

    /**
     * Resets the internal cluster-state of the Node to be able to make it ready to join a new cluster.
     * After this method is called,
     * a new join process can be triggered by calling {@link #join()}.
     * <p/>
     * This method is called during merge process after a split-brain is detected.
     */
    public void reset() {
        state = NodeState.ACTIVE;
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

    public HazelcastProperties getProperties() {
        return properties;
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

    public DiscoveryService getDiscoveryService() {
        return discoveryService;
    }

    public class NodeShutdownHookThread extends Thread {

        NodeShutdownHookThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            try {
                if (isRunning()) {
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
                localMember.isLiteMember(), createConfigCheck(), clusterService.getMemberAddresses(),
                clusterService.getSize(MemberSelectors.DATA_MEMBER_SELECTOR));
    }

    public JoinRequest createJoinRequest(boolean withCredentials) {
        final Credentials credentials = (withCredentials && securityContext != null)
                ? securityContext.getCredentialsFactory().newCredentials() : null;

        return new JoinRequest(Packet.VERSION, buildInfo.getBuildNumber(), address,
                localMember.getUuid(), localMember.isLiteMember(), createConfigCheck(), credentials,
                localMember.getAttributes());
    }

    public ConfigCheck createConfigCheck() {
        String joinerType = joiner == null ? "" : joiner.getType();
        return new ConfigCheck(config, joinerType);
    }

    public void join() {
        if (joined()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Calling join on already joined node. ", new Exception("stacktrace"));
            } else {
                logger.warning("Calling join on already joined node. ");
            }
            return;
        }
        if (joiner == null) {
            logger.warning("No join method is enabled! Starting standalone.");
            setAsMaster();
            return;
        }

        try {
            masterAddress = null;
            joiner.join();
        } catch (Throwable e) {
            logger.severe("Error while joining the cluster!", e);
        }

        if (!joined()) {
            long maxJoinTimeMillis = properties.getMillis(MAX_JOIN_SECONDS);
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

        if (properties.getBoolean(DISCOVERY_SPI_ENABLED)) {
            //TODO: Auto-Upgrade Multicast+AWS configuration!
            logger.info("Activating Discovery SPI Joiner");
            return new DiscoveryJoiner(this, discoveryService, properties.getBoolean(DISCOVERY_SPI_PUBLIC_IP_ENABLED));
        } else {
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
        }
        return null;
    }

    public void setAsMaster() {
        logger.finest("This node is being set as the master");
        masterAddress = address;
        setJoined();
        getClusterService().getClusterClock().setClusterStartTime(Clock.currentTimeMillis());
        getClusterService().setClusterId(UuidUtil.createClusterUuid());
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

    public boolean isLiteMember() {
        return liteMember;
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
