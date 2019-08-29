/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.cache.impl.JCacheDetector;
import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.ClientConnectionStrategy;
import com.hazelcast.client.impl.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.connection.nio.ClusterConnectorService;
import com.hazelcast.client.impl.connection.nio.ClusterConnectorServiceImpl;
import com.hazelcast.client.impl.connection.nio.DefaultClientConnectionStrategy;
import com.hazelcast.client.cp.internal.CPSubsystemImpl;
import com.hazelcast.client.cp.internal.session.ClientProxySessionManager;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import com.hazelcast.client.impl.querycache.ClientQueryCacheContext;
import com.hazelcast.client.impl.statistics.Statistics;
import com.hazelcast.client.impl.proxy.ClientClusterProxy;
import com.hazelcast.client.impl.proxy.PartitionServiceProxy;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientExecutionService;
import com.hazelcast.client.impl.spi.ClientInvocationService;
import com.hazelcast.client.impl.spi.ClientListenerService;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.ClientTransactionManagerService;
import com.hazelcast.client.impl.spi.ProxyManager;
import com.hazelcast.client.impl.spi.impl.AbstractClientInvocationService;
import com.hazelcast.client.impl.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.impl.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientPartitionServiceImpl;
import com.hazelcast.client.impl.spi.impl.ClientTransactionManagerServiceImpl;
import com.hazelcast.client.impl.spi.impl.ClientUserCodeDeploymentService;
import com.hazelcast.client.impl.spi.impl.NonSmartClientInvocationService;
import com.hazelcast.client.impl.spi.impl.SmartClientInvocationService;
import com.hazelcast.client.impl.spi.impl.listener.AbstractClientListenerService;
import com.hazelcast.client.impl.spi.impl.listener.NonSmartClientListenerService;
import com.hazelcast.client.impl.spi.impl.listener.SmartClientListenerService;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.AtomicReferenceService;
import com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.unsafe.idgen.IdGeneratorService;
import com.hazelcast.cp.internal.datastructures.unsafe.lock.LockServiceImpl;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.SemaphoreService;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.client.Client;
import com.hazelcast.client.ClientService;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.collection.IList;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.map.IMap;
import com.hazelcast.collection.IQueue;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.topic.ITopic;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.crdt.pncounter.PNCounterService;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.diagnostics.BuildInfoPlugin;
import com.hazelcast.internal.diagnostics.ConfigPropertiesPlugin;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.EventQueuePlugin;
import com.hazelcast.internal.diagnostics.MetricsPlugin;
import com.hazelcast.internal.diagnostics.NetworkingImbalancePlugin;
import com.hazelcast.internal.diagnostics.SystemLogPlugin;
import com.hazelcast.internal.diagnostics.SystemPropertiesPlugin;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.metrics.metricsets.ClassLoadingMetricSet;
import com.hazelcast.internal.metrics.metricsets.FileMetricSet;
import com.hazelcast.internal.metrics.metricsets.GarbageCollectionMetricSet;
import com.hazelcast.internal.metrics.metricsets.OperatingSystemMetricSet;
import com.hazelcast.internal.metrics.metricsets.RuntimeMetricSet;
import com.hazelcast.internal.metrics.metricsets.ThreadMetricSet;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.ConcurrencyDetection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Connection;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.impl.xa.XAService;
import com.hazelcast.util.ServiceLoader;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.properties.ClientProperty.CONCURRENT_WINDOW_MS;
import static com.hazelcast.client.properties.ClientProperty.IO_WRITE_THROUGH_ENABLED;
import static com.hazelcast.client.properties.ClientProperty.MAX_CONCURRENT_INVOCATIONS;
import static com.hazelcast.client.properties.ClientProperty.RESPONSE_THREAD_DYNAMIC;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static java.lang.System.currentTimeMillis;

public class HazelcastClientInstanceImpl implements HazelcastInstance, SerializationServiceSupport {

    private static final AtomicInteger CLIENT_ID = new AtomicInteger();

    private final ConcurrencyDetection concurrencyDetection;
    private final HazelcastProperties properties;
    private final int id = CLIENT_ID.getAndIncrement();
    private final String instanceName;
    private final ClientFailoverConfig clientFailoverConfig;
    private final ClientConfig config;
    private final LifecycleServiceImpl lifecycleService;
    private final ClientConnectionManagerImpl connectionManager;
    private final ClientClusterServiceImpl clusterService;
    private final ClientPartitionServiceImpl partitionService;
    private final AbstractClientInvocationService invocationService;
    private final ClientExecutionServiceImpl executionService;
    private final AbstractClientListenerService listenerService;
    private final ClientTransactionManagerServiceImpl transactionManager;
    private final NearCacheManager nearCacheManager;
    private final ProxyManager proxyManager;
    private final ConcurrentMap<String, Object> userContext;
    private final LoadBalancer loadBalancer;
    private final ClientExtension clientExtension;
    private final ClusterConnectorService clusterConnectorService;
    private final ClientConnectionStrategy clientConnectionStrategy;
    private final LoggingService loggingService;
    private final MetricsRegistryImpl metricsRegistry;
    private final Statistics statistics;
    private final Diagnostics diagnostics;
    private final InternalSerializationService serializationService;
    private final ClientICacheManager hazelcastCacheManager;
    private final ClientQueryCacheContext queryCacheContext;
    private final ClientLockReferenceIdGenerator lockReferenceIdGenerator;
    private final ClientExceptionFactory clientExceptionFactory;
    private final ClientUserCodeDeploymentService userCodeDeploymentService;
    private final ClientDiscoveryService clientDiscoveryService;
    private final ClientProxySessionManager proxySessionManager;
    private final CPSubsystemImpl cpSubsystem;

    public HazelcastClientInstanceImpl(ClientConfig clientConfig,
                                       ClientFailoverConfig clientFailoverConfig,
                                       ClientConnectionManagerFactory clientConnectionManagerFactory,
                                       AddressProvider externalAddressProvider) {
        assert clientConfig != null || clientFailoverConfig != null : "At most one type of config can be provided";
        assert clientConfig == null || clientFailoverConfig == null : "At least one config should be provided ";
        if (clientConfig != null) {
            this.config = clientConfig;
        } else {
            this.config = clientFailoverConfig.getClientConfigs().get(0);
        }
        this.clientFailoverConfig = clientFailoverConfig;
        if (config.getInstanceName() != null) {
            instanceName = config.getInstanceName();
        } else {
            instanceName = "hz.client_" + id;
        }

        GroupConfig groupConfig = config.getGroupConfig();
        String loggingType = config.getProperty(GroupProperty.LOGGING_TYPE.getName());
        loggingService = new ClientLoggingService(groupConfig.getName(),
                loggingType, BuildInfoProvider.getBuildInfo(), instanceName);
        logGroupPasswordInfo();
        ClassLoader classLoader = config.getClassLoader();
        properties = new HazelcastProperties(config.getProperties());
        concurrencyDetection = initConcurrencyDetection();
        clientExtension = createClientInitializer(classLoader);
        clientExtension.beforeStart(this);
        lifecycleService = new LifecycleServiceImpl(this);
        metricsRegistry = initMetricsRegistry();
        serializationService = clientExtension.createSerializationService((byte) -1);
        proxyManager = new ProxyManager(this);
        executionService = initExecutionService();
        loadBalancer = initLoadBalancer(config);
        transactionManager = new ClientTransactionManagerServiceImpl(this, loadBalancer);
        partitionService = new ClientPartitionServiceImpl(this);
        clientDiscoveryService = initClientDiscoveryService(externalAddressProvider);
        clientConnectionStrategy = initializeStrategy();
        connectionManager = (ClientConnectionManagerImpl) clientConnectionManagerFactory.createConnectionManager(this);
        clusterConnectorService = initClusterConnectorService();
        clusterService = new ClientClusterServiceImpl(this);
        invocationService = initInvocationService();
        listenerService = initListenerService();
        userContext = new ConcurrentHashMap<String, Object>();
        userContext.putAll(config.getUserContext());
        diagnostics = initDiagnostics();
        hazelcastCacheManager = new ClientICacheManager(this);
        queryCacheContext = new ClientQueryCacheContext(this);
        lockReferenceIdGenerator = new ClientLockReferenceIdGenerator();
        nearCacheManager = clientExtension.createNearCacheManager();
        clientExceptionFactory = initClientExceptionFactory();
        statistics = new Statistics(this);
        userCodeDeploymentService = new ClientUserCodeDeploymentService(config.getUserCodeDeploymentConfig(), classLoader);
        proxySessionManager = new ClientProxySessionManager(this);
        cpSubsystem = new CPSubsystemImpl(this);
    }

    private ConcurrencyDetection initConcurrencyDetection() {
        boolean writeThrough = properties.getBoolean(IO_WRITE_THROUGH_ENABLED);
        boolean dynamicResponse = properties.getBoolean(RESPONSE_THREAD_DYNAMIC);
        boolean backPressureEnabled = properties.getInteger(MAX_CONCURRENT_INVOCATIONS) < Integer.MAX_VALUE;

        if (writeThrough || dynamicResponse || backPressureEnabled) {
            return ConcurrencyDetection.createEnabled(properties.getInteger(CONCURRENT_WINDOW_MS));
        } else {
            return ConcurrencyDetection.createDisabled();
        }
    }

    private ClusterConnectorService initClusterConnectorService() {
        ClusterConnectorServiceImpl service = new ClusterConnectorServiceImpl(this, connectionManager,
                clientConnectionStrategy, clientDiscoveryService);
        connectionManager.addConnectionListener(service);
        return service;
    }

    private ClientConnectionStrategy initializeStrategy() {
        ClientConnectionStrategy strategy;
        //internal property
        String className = properties.get("hazelcast.client.connection.strategy.classname");
        if (className != null) {
            try {
                ClassLoader configClassLoader = config.getClassLoader();
                return ClassLoaderUtil.newInstance(configClassLoader, className);
            } catch (Exception e) {
                throw rethrow(e);
            }
        } else {
            strategy = new DefaultClientConnectionStrategy();
        }
        return strategy;
    }

    private ClientDiscoveryService initClientDiscoveryService(AddressProvider externalAddressProvider) {
        int tryCount;
        List<ClientConfig> configs;
        if (clientFailoverConfig == null) {
            tryCount = 0;
            configs = Collections.singletonList(config);
        } else {
            tryCount = clientFailoverConfig.getTryCount();
            configs = clientFailoverConfig.getClientConfigs();
        }
        ClientDiscoveryServiceBuilder builder = new ClientDiscoveryServiceBuilder(tryCount, configs, loggingService,
                externalAddressProvider, properties, clientExtension);
        return builder.build();
    }

    private Diagnostics initDiagnostics() {
        String name = "diagnostics-client-" + id + "-" + currentTimeMillis();
        ILogger logger = loggingService.getLogger(Diagnostics.class);
        return new Diagnostics(name, logger, instanceName, properties);
    }

    private MetricsRegistryImpl initMetricsRegistry() {
        ProbeLevel probeLevel = properties.getEnum(Diagnostics.METRICS_LEVEL, ProbeLevel.class);
        ILogger logger = loggingService.getLogger(MetricsRegistryImpl.class);
        MetricsRegistryImpl metricsRegistry = new MetricsRegistryImpl(getName(), logger, probeLevel);
        return metricsRegistry;
    }

    private void startMetrics() {
        RuntimeMetricSet.register(metricsRegistry);
        GarbageCollectionMetricSet.register(metricsRegistry);
        OperatingSystemMetricSet.register(metricsRegistry);
        ThreadMetricSet.register(metricsRegistry);
        ClassLoadingMetricSet.register(metricsRegistry);
        FileMetricSet.register(metricsRegistry);
        metricsRegistry.scanAndRegister(clientExtension.getMemoryStats(), "memory");
        metricsRegistry.collectMetrics(clientExtension);
        metricsRegistry.collectMetrics(executionService);
    }

    private LoadBalancer initLoadBalancer(ClientConfig config) {
        LoadBalancer lb = config.getLoadBalancer();
        if (lb == null) {
            lb = new RoundRobinLB();
        }
        return lb;
    }

    @SuppressWarnings("checkstyle:illegaltype")
    private AbstractClientInvocationService initInvocationService() {
        final ClientNetworkConfig networkConfig = config.getNetworkConfig();
        if (networkConfig.isSmartRouting()) {
            return new SmartClientInvocationService(this, loadBalancer);
        } else {
            return new NonSmartClientInvocationService(this);
        }
    }

    public int getId() {
        return id;
    }

    private ClientExtension createClientInitializer(ClassLoader classLoader) {
        try {
            String factoryId = ClientExtension.class.getName();
            Iterator<ClientExtension> iter = ServiceLoader.iterator(ClientExtension.class, factoryId, classLoader);
            while (iter.hasNext()) {
                ClientExtension initializer = iter.next();
                if (!(initializer.getClass().equals(DefaultClientExtension.class))) {
                    return initializer;
                }
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
        return new DefaultClientExtension();
    }

    @SuppressWarnings("checkstyle:illegaltype")
    private AbstractClientListenerService initListenerService() {
        return config.getNetworkConfig().isSmartRouting()
                ? new SmartClientListenerService(this)
                : new NonSmartClientListenerService(this);
    }

    private ClientExecutionServiceImpl initExecutionService() {
        return new ClientExecutionServiceImpl(instanceName,
                config.getClassLoader(), properties, config.getExecutorPoolSize(), loggingService);
    }

    private void logGroupPasswordInfo() {
        if (!isNullOrEmpty(config.getGroupConfig().getPassword())) {
            ILogger logger = loggingService.getLogger(HazelcastClient.class);
            logger.info("A non-empty group password is configured for the Hazelcast client."
                    + " Starting with Hazelcast version 3.11, clients with the same group name,"
                    + " but with different group passwords (that do not use authentication) will be accepted to a cluster."
                    + " The group password configuration will be removed completely in a future release.");
        }
    }

    public void start() {
        try {
            lifecycleService.start();
            startMetrics();
            invocationService.start();
            clusterService.start();
            ClientContext clientContext = new ClientContext(this);
            userCodeDeploymentService.start();
            connectionManager.start();
            clientConnectionStrategy.init(clientContext);
            clientConnectionStrategy.start();

            diagnostics.start();

            // static loggers at beginning of file
            diagnostics.register(
                    new BuildInfoPlugin(loggingService.getLogger(BuildInfoPlugin.class)));
            diagnostics.register(
                    new ConfigPropertiesPlugin(loggingService.getLogger(ConfigPropertiesPlugin.class), properties));
            diagnostics.register(
                    new SystemPropertiesPlugin(loggingService.getLogger(SystemPropertiesPlugin.class)));

            // periodic loggers
            diagnostics.register(
                    new MetricsPlugin(loggingService.getLogger(MetricsPlugin.class), metricsRegistry, properties));
            diagnostics.register(
                    new SystemLogPlugin(properties, connectionManager, this, loggingService.getLogger(SystemLogPlugin.class)));
            diagnostics.register(
                    new NetworkingImbalancePlugin(properties, connectionManager.getNetworking(),
                            loggingService.getLogger(NetworkingImbalancePlugin.class)));
            diagnostics.register(
                    new EventQueuePlugin(loggingService.getLogger(EventQueuePlugin.class), listenerService.getEventExecutor(),
                            properties));

            metricsRegistry.collectMetrics(listenerService);

            proxyManager.init(config, clientContext);
            listenerService.start();
            loadBalancer.init(getCluster(), config);
            partitionService.start();
            statistics.start();
            clientExtension.afterStart(this);
            cpSubsystem.init(clientContext);
        } catch (Throwable e) {
            try {
                lifecycleService.terminate();
            } catch (Throwable t) {
                ignore(t);
            }
            rethrow(e);
        }
    }

    public void onClusterConnect(Connection ownerConnection) throws Exception {
        partitionService.listenPartitionTable(ownerConnection);
        clusterService.listenMembershipEvents(ownerConnection);
        userCodeDeploymentService.deploy(this, ownerConnection);
        proxyManager.createDistributedObjectsOnCluster(ownerConnection);
    }

    public void onClusterDisconnect() {
        partitionService.cleanupOnDisconnect();
        clusterService.cleanupOnDisconnect();
    }

    public MetricsRegistryImpl getMetricsRegistry() {
        return metricsRegistry;
    }

    @Override
    public HazelcastXAResource getXAResource() {
        return getDistributedObject(XAService.SERVICE_NAME, XAService.SERVICE_NAME);
    }

    @Override
    public Config getConfig() {
        return new ClientDynamicClusterConfig(this);
    }

    public HazelcastProperties getProperties() {
        return properties;
    }

    @Override
    public String getName() {
        return instanceName;
    }

    @Override
    public <E> IQueue<E> getQueue(String name) {
        checkNotNull(name, "Retrieving a queue instance with a null name is not allowed!");
        return getDistributedObject(QueueService.SERVICE_NAME, name);
    }

    @Override
    public <E> ITopic<E> getTopic(String name) {
        checkNotNull(name, "Retrieving a topic instance with a null name is not allowed!");
        return getDistributedObject(TopicService.SERVICE_NAME, name);
    }

    @Override
    public <E> ISet<E> getSet(String name) {
        checkNotNull(name, "Retrieving a set instance with a null name is not allowed!");
        return getDistributedObject(SetService.SERVICE_NAME, name);
    }

    @Override
    public <E> IList<E> getList(String name) {
        checkNotNull(name, "Retrieving a list instance with a null name is not allowed!");
        return getDistributedObject(ListService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> IMap<K, V> getMap(String name) {
        checkNotNull(name, "Retrieving a map instance with a null name is not allowed!");
        return getDistributedObject(MapService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        checkNotNull(name, "Retrieving a multi-map instance with a null name is not allowed!");
        return getDistributedObject(MultiMapService.SERVICE_NAME, name);

    }

    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(String name) {
        checkNotNull(name, "Retrieving a replicated map instance with a null name is not allowed!");
        return getDistributedObject(ReplicatedMapService.SERVICE_NAME, name);
    }

    @Override
    public ILock getLock(String key) {
        checkNotNull(key, "Retrieving a lock instance with a null key is not allowed!");
        return getDistributedObject(LockServiceImpl.SERVICE_NAME, key);
    }

    @Override
    public <E> ITopic<E> getReliableTopic(String name) {
        checkNotNull(name, "Retrieving a topic instance with a null name is not allowed!");
        return getDistributedObject(ReliableTopicService.SERVICE_NAME, name);
    }

    @Override
    public <E> Ringbuffer<E> getRingbuffer(String name) {
        checkNotNull(name, "Retrieving a ringbuffer instance with a null name is not allowed!");
        return getDistributedObject(RingbufferService.SERVICE_NAME, name);
    }

    @Override
    public ClientICacheManager getCacheManager() {
        return hazelcastCacheManager;
    }

    @Override
    public Cluster getCluster() {
        return new ClientClusterProxy(clusterService);
    }

    @Override
    public Client getLocalEndpoint() {
        return clusterService.getLocalClient();
    }

    @Override
    public IExecutorService getExecutorService(String name) {
        checkNotNull(name, "Retrieving an executor instance with a null name is not allowed!");
        return getDistributedObject(DistributedExecutorService.SERVICE_NAME, name);
    }

    @Override
    public DurableExecutorService getDurableExecutorService(String name) {
        checkNotNull(name, "Retrieving a durable executor instance with a null name is not allowed!");
        return getDistributedObject(DistributedDurableExecutorService.SERVICE_NAME, name);
    }

    @Override
    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
        return transactionManager.executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        return transactionManager.executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return transactionManager.newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(TransactionOptions options) {
        return transactionManager.newTransactionContext(options);
    }

    public ClientTransactionManagerService getTransactionManager() {
        return transactionManager;
    }

    @Override
    public IdGenerator getIdGenerator(String name) {
        checkNotNull(name, "Retrieving an ID-generator instance with a null name is not allowed!");
        return getDistributedObject(IdGeneratorService.SERVICE_NAME, name);
    }

    @Override
    public FlakeIdGenerator getFlakeIdGenerator(String name) {
        checkNotNull(name, "Retrieving a Flake ID-generator instance with a null name is not allowed!");
        return getDistributedObject(FlakeIdGeneratorService.SERVICE_NAME, name);
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        checkNotNull(name, "Retrieving an atomic-long instance with a null name is not allowed!");
        return getDistributedObject(AtomicLongService.SERVICE_NAME, name);
    }

    @Override
    public CardinalityEstimator getCardinalityEstimator(String name) {
        checkNotNull(name, "Retrieving a cardinality estimator instance with a null name is not allowed!");
        return getDistributedObject(CardinalityEstimatorService.SERVICE_NAME, name);
    }

    @Override
    public PNCounter getPNCounter(String name) {
        checkNotNull(name, "Retrieving a PN counter instance with a null name is not allowed!");
        return getDistributedObject(PNCounterService.SERVICE_NAME, name);
    }

    @Override
    public <E> IAtomicReference<E> getAtomicReference(String name) {
        checkNotNull(name, "Retrieving an atomic-reference instance with a null name is not allowed!");
        return getDistributedObject(AtomicReferenceService.SERVICE_NAME, name);
    }

    @Override
    public ICountDownLatch getCountDownLatch(String name) {
        checkNotNull(name, "Retrieving a countdown-latch instance with a null name is not allowed!");
        return getDistributedObject(CountDownLatchService.SERVICE_NAME, name);
    }

    @Override
    public ISemaphore getSemaphore(String name) {
        checkNotNull(name, "Retrieving a semaphore instance with a null name is not allowed!");
        return getDistributedObject(SemaphoreService.SERVICE_NAME, name);
    }

    @Override
    public IScheduledExecutorService getScheduledExecutorService(String name) {
        checkNotNull(name, "Retrieving a scheduled executor instance with a null name is not allowed!");
        return getDistributedObject(DistributedScheduledExecutorService.SERVICE_NAME, name);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        try {
            ClientMessage request = ClientGetDistributedObjectsCodec.encodeRequest();
            final Future<ClientMessage> future = new ClientInvocation(this, request, getName()).invoke();
            ClientMessage response = future.get();
            ClientGetDistributedObjectsCodec.ResponseParameters resultParameters =
                    ClientGetDistributedObjectsCodec.decodeResponse(response);

            Collection<? extends DistributedObject> distributedObjects = proxyManager.getDistributedObjects();
            Set<DistributedObjectInfo> localDistributedObjects = new HashSet<DistributedObjectInfo>();
            for (DistributedObject localInfo : distributedObjects) {
                localDistributedObjects.add(new DistributedObjectInfo(localInfo.getServiceName(), localInfo.getName()));
            }

            Collection<DistributedObjectInfo> newDistributedObjectInfo = resultParameters.response;
            for (DistributedObjectInfo distributedObjectInfo : newDistributedObjectInfo) {
                localDistributedObjects.remove(distributedObjectInfo);
                getDistributedObject(distributedObjectInfo.getServiceName(), distributedObjectInfo.getName());
            }

            for (DistributedObjectInfo distributedObjectInfo : localDistributedObjects) {
                proxyManager.destroyProxyLocally(distributedObjectInfo.getServiceName(), distributedObjectInfo.getName());
            }
            return (Collection<DistributedObject>) proxyManager.getDistributedObjects();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        return proxyManager.addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(String registrationId) {
        return proxyManager.removeDistributedObjectListener(registrationId);
    }

    @Override
    public PartitionService getPartitionService() {
        return new PartitionServiceProxy(partitionService, listenerService);
    }

    @Override
    public QuorumService getQuorumService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClientService getClientService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LoggingService getLoggingService() {
        return loggingService;
    }

    @Override
    public LifecycleService getLifecycleService() {
        return lifecycleService;
    }

    @Override
    public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
        return (T) proxyManager.getOrCreateProxy(serviceName, name);
    }

    @Override
    public CPSubsystem getCPSubsystem() {
        return cpSubsystem;
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return userContext;
    }

    public ClientConfig getClientConfig() {
        return config;
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return serializationService;
    }

    public ClientUserCodeDeploymentService getUserCodeDeploymentService() {
        return userCodeDeploymentService;
    }

    public ClientProxySessionManager getProxySessionManager() {
        return proxySessionManager;
    }

    public ProxyManager getProxyManager() {
        return proxyManager;
    }

    public ClientConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public ClientClusterService getClientClusterService() {
        return clusterService;
    }

    public ClientExecutionService getClientExecutionService() {
        return executionService;
    }

    public ClientPartitionService getClientPartitionService() {
        return partitionService;
    }

    public ClientInvocationService getInvocationService() {
        return invocationService;
    }

    public ClientListenerService getListenerService() {
        return listenerService;
    }

    public NearCacheManager getNearCacheManager() {
        return nearCacheManager;
    }

    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    public ClientExtension getClientExtension() {
        return clientExtension;
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    public void doShutdown(boolean isGraceful) {
        if (isGraceful) {
            proxySessionManager.shutdown();
        }
        proxyManager.destroy();
        connectionManager.shutdown();
        clientConnectionStrategy.shutdown();
        clusterConnectorService.shutdown();
        clientDiscoveryService.shutdown();
        clusterService.shutdown();
        partitionService.reset();
        transactionManager.shutdown();
        invocationService.shutdown();
        executionService.shutdown();
        listenerService.shutdown();
        nearCacheManager.destroyAllNearCaches();
        metricsRegistry.shutdown();
        diagnostics.shutdown();
        serializationService.dispose();
    }

    public ClientLockReferenceIdGenerator getLockReferenceIdGenerator() {
        return lockReferenceIdGenerator;
    }

    private ClientExceptionFactory initClientExceptionFactory() {
        boolean jCacheAvailable = JCacheDetector.isJCacheAvailable(getClientConfig().getClassLoader());
        return new ClientExceptionFactory(jCacheAvailable);
    }

    public ClientExceptionFactory getClientExceptionFactory() {
        return clientExceptionFactory;
    }

    public ClusterConnectorService getClusterConnectorService() {
        return clusterConnectorService;
    }

    public ClientDiscoveryService getClientDiscoveryService() {
        return clientDiscoveryService;
    }

    public ClientConnectionStrategy getConnectionStrategy() {
        return clientConnectionStrategy;
    }

    public ClientFailoverConfig getFailoverConfig() {
        return clientFailoverConfig;
    }

    public ClientQueryCacheContext getQueryCacheContext() {
        return queryCacheContext;
    }

    public ConcurrencyDetection getConcurrencyDetection() {
        return concurrencyDetection;
    }
}
