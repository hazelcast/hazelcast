/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.cache.impl.JCacheDetector;
import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import com.hazelcast.client.impl.statistics.Statistics;
import com.hazelcast.client.proxy.ClientClusterProxy;
import com.hazelcast.client.proxy.PartitionServiceProxy;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.ClientTransactionManagerService;
import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.client.spi.impl.AwsAddressProvider;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.client.spi.impl.ClientNonSmartInvocationServiceImpl;
import com.hazelcast.client.spi.impl.ClientPartitionServiceImpl;
import com.hazelcast.client.spi.impl.ClientSmartInvocationServiceImpl;
import com.hazelcast.client.spi.impl.ClientTransactionManagerServiceImpl;
import com.hazelcast.client.spi.impl.ClientUserCodeDeploymentService;
import com.hazelcast.client.spi.impl.DefaultAddressProvider;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressProvider;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.client.spi.impl.listener.ClientNonSmartListenerService;
import com.hazelcast.client.spi.impl.listener.ClientSmartListenerService;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.executor.impl.DistributedExecutorService;
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Connection;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.spi.impl.sequence.CallIdSequence;
import com.hazelcast.spi.impl.sequence.CallIdSequenceWithBackpressure;
import com.hazelcast.spi.impl.sequence.CallIdSequenceWithoutBackpressure;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.spi.properties.ClientProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
import static com.hazelcast.client.spi.properties.ClientProperty.MAX_CONCURRENT_INVOCATIONS;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.System.currentTimeMillis;

public class HazelcastClientInstanceImpl implements HazelcastInstance, SerializationServiceSupport {

    private static final AtomicInteger CLIENT_ID = new AtomicInteger();
    private static final short PROTOCOL_VERSION = ClientMessage.VERSION;

    private final HazelcastProperties properties;
    private final int id = CLIENT_ID.getAndIncrement();
    private final String instanceName;
    private final ClientConfig config;
    private final LifecycleServiceImpl lifecycleService;
    private final ClientConnectionManagerImpl connectionManager;
    private final ClientClusterServiceImpl clusterService;
    private final ClientPartitionServiceImpl partitionService;
    private final ClientInvocationServiceImpl invocationService;
    private final ClientExecutionServiceImpl executionService;
    private final ClientListenerServiceImpl listenerService;
    private final ClientTransactionManagerServiceImpl transactionManager;
    private final NearCacheManager nearCacheManager;
    private final ProxyManager proxyManager;
    private final ConcurrentMap<String, Object> userContext;
    private final LoadBalancer loadBalancer;
    private final ClientExtension clientExtension;
    private final Credentials credentials;
    private final DiscoveryService discoveryService;
    private final LoggingService loggingService;
    private final MetricsRegistryImpl metricsRegistry;
    private final Statistics statistics;
    private final Diagnostics diagnostics;
    private final SerializationService serializationService;
    private final ClientICacheManager hazelcastCacheManager;
    private final ClientLockReferenceIdGenerator lockReferenceIdGenerator;
    private final ClientExceptionFactory clientExceptionFactory;
    private final CallIdSequence callIdSequence;
    private final ClientUserCodeDeploymentService userCodeDeploymentService;

    public HazelcastClientInstanceImpl(ClientConfig config,
                                       ClientConnectionManagerFactory clientConnectionManagerFactory,
                                       AddressProvider externalAddressProvider) {
        this.config = config;
        if (config.getInstanceName() != null) {
            instanceName = config.getInstanceName();
        } else {
            instanceName = "hz.client_" + id;
        }

        GroupConfig groupConfig = config.getGroupConfig();
        String loggingType = config.getProperty(GroupProperty.LOGGING_TYPE.getName());
        loggingService = new ClientLoggingService(groupConfig.getName(),
                loggingType, BuildInfoProvider.getBuildInfo(), instanceName);
        ClassLoader classLoader = config.getClassLoader();
        clientExtension = createClientInitializer(classLoader);
        clientExtension.beforeStart(this);

        credentials = initCredentials(config);
        lifecycleService = new LifecycleServiceImpl(this);
        properties = new HazelcastProperties(config.getProperties());

        metricsRegistry = initMetricsRegistry();
        serializationService = clientExtension.createSerializationService((byte) -1);
        metricsRegistry.collectMetrics(clientExtension);

        proxyManager = new ProxyManager(this);
        executionService = initExecutionService();
        metricsRegistry.collectMetrics(executionService);
        loadBalancer = initLoadBalancer(config);
        transactionManager = new ClientTransactionManagerServiceImpl(this, loadBalancer);
        partitionService = new ClientPartitionServiceImpl(this);
        discoveryService = initDiscoveryService(config);
        Collection<AddressProvider> addressProviders = createAddressProviders(externalAddressProvider);
        connectionManager = (ClientConnectionManagerImpl) clientConnectionManagerFactory
                .createConnectionManager(config, this, discoveryService, addressProviders);
        clusterService = new ClientClusterServiceImpl(this);

        int maxAllowedConcurrentInvocations = properties.getInteger(MAX_CONCURRENT_INVOCATIONS);
        long backofftimeoutMs = properties.getLong(BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS);
        if (maxAllowedConcurrentInvocations == Integer.MAX_VALUE) {
            callIdSequence = new CallIdSequenceWithoutBackpressure();
        } else {
            callIdSequence = new CallIdSequenceWithBackpressure(maxAllowedConcurrentInvocations, backofftimeoutMs);
        }

        invocationService = initInvocationService();
        listenerService = initListenerService();
        userContext = new ConcurrentHashMap<String, Object>();
        diagnostics = initDiagnostics();

        hazelcastCacheManager = new ClientICacheManager(this);

        lockReferenceIdGenerator = new ClientLockReferenceIdGenerator();
        nearCacheManager = clientExtension.createNearCacheManager();
        clientExceptionFactory = initClientExceptionFactory();
        statistics = new Statistics(this);
        userCodeDeploymentService = new ClientUserCodeDeploymentService(config.getUserCodeDeploymentConfig(), classLoader);
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
        RuntimeMetricSet.register(metricsRegistry);
        GarbageCollectionMetricSet.register(metricsRegistry);
        OperatingSystemMetricSet.register(metricsRegistry);
        ThreadMetricSet.register(metricsRegistry);
        ClassLoadingMetricSet.register(metricsRegistry);
        FileMetricSet.register(metricsRegistry);
        return metricsRegistry;
    }

    private Collection<AddressProvider> createAddressProviders(AddressProvider externalAddressProvider) {
        ClientNetworkConfig networkConfig = getClientConfig().getNetworkConfig();
        final ClientAwsConfig awsConfig = networkConfig.getAwsConfig();
        Collection<AddressProvider> addressProviders = new LinkedList<AddressProvider>();

        if (externalAddressProvider != null) {
            addressProviders.add(externalAddressProvider);
        }

        if (discoveryService != null) {
            addressProviders.add(new DiscoveryAddressProvider(discoveryService, loggingService));
        }

        if (awsConfig != null && awsConfig.isEnabled()) {
            try {
                addressProviders.add(new AwsAddressProvider(awsConfig, loggingService));
            } catch (NoClassDefFoundError e) {
                ILogger logger = loggingService.getLogger(HazelcastClient.class);
                logger.warning("hazelcast-aws.jar might be missing!");
                throw e;
            }
        }

        addressProviders.add(new DefaultAddressProvider(networkConfig, addressProviders.isEmpty()));
        return addressProviders;
    }

    private DiscoveryService initDiscoveryService(ClientConfig config) {
        // Prevent confusing behavior where the DiscoveryService is started
        // and strategies are resolved but the AddressProvider is never registered
        if (!properties.getBoolean(ClientProperty.DISCOVERY_SPI_ENABLED)) {
            return null;
        }

        ILogger logger = loggingService.getLogger(DiscoveryService.class);
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        DiscoveryConfig discoveryConfig = networkConfig.getDiscoveryConfig().getAsReadOnly();

        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }

        DiscoveryServiceSettings settings = new DiscoveryServiceSettings().setConfigClassLoader(config.getClassLoader())
                .setLogger(logger).setDiscoveryMode(DiscoveryMode.Client).setDiscoveryConfig(discoveryConfig);

        DiscoveryService discoveryService = factory.newDiscoveryService(settings);
        discoveryService.start();
        return discoveryService;
    }

    private LoadBalancer initLoadBalancer(ClientConfig config) {
        LoadBalancer lb = config.getLoadBalancer();
        if (lb == null) {
            lb = new RoundRobinLB();
        }
        return lb;
    }

    private Credentials initCredentials(ClientConfig config) {
        final GroupConfig groupConfig = config.getGroupConfig();
        final ClientSecurityConfig securityConfig = config.getSecurityConfig();
        Credentials c = securityConfig.getCredentials();
        if (c == null) {
            final String credentialsClassname = securityConfig.getCredentialsClassname();
            if (credentialsClassname != null) {
                try {
                    c = ClassLoaderUtil.newInstance(config.getClassLoader(), credentialsClassname);
                } catch (Exception e) {
                    throw rethrow(e);
                }
            }
        }
        if (c == null) {
            c = new UsernamePasswordCredentials(groupConfig.getName(), groupConfig.getPassword());
        }
        return c;
    }

    private ClientInvocationServiceImpl initInvocationService() {
        final ClientNetworkConfig networkConfig = config.getNetworkConfig();
        if (networkConfig.isSmartRouting()) {
            return new ClientSmartInvocationServiceImpl(this, loadBalancer);
        } else {
            return new ClientNonSmartInvocationServiceImpl(this);
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

    private ClientListenerServiceImpl initListenerService() {
        int eventQueueCapacity = properties.getInteger(ClientProperty.EVENT_QUEUE_CAPACITY);
        int eventThreadCount = properties.getInteger(ClientProperty.EVENT_THREAD_COUNT);
        final ClientNetworkConfig networkConfig = config.getNetworkConfig();
        if (networkConfig.isSmartRouting()) {
            return new ClientSmartListenerService(this, eventThreadCount, eventQueueCapacity);
        } else {
            return new ClientNonSmartListenerService(this, eventThreadCount, eventQueueCapacity);
        }
    }

    private ClientExecutionServiceImpl initExecutionService() {
        return new ClientExecutionServiceImpl(instanceName,
                config.getClassLoader(), properties, config.getExecutorPoolSize(), loggingService);
    }

    public void start() {
        lifecycleService.setStarted();
        invocationService.start();
        clusterService.start();
        ClientContext clientContext = new ClientContext(this);
        try {
            userCodeDeploymentService.start();
            connectionManager.start(clientContext);
        } catch (Exception e) {
            throw rethrow(e);
        }

        diagnostics.start();
        diagnostics.register(
                new EventQueuePlugin(loggingService.getLogger(EventQueuePlugin.class), listenerService.getEventExecutor(),
                        properties));
        diagnostics.register(
                new BuildInfoPlugin(loggingService.getLogger(BuildInfoPlugin.class)));
        diagnostics.register(
                new ConfigPropertiesPlugin(loggingService.getLogger(ConfigPropertiesPlugin.class), properties));
        diagnostics.register(
                new SystemPropertiesPlugin(loggingService.getLogger(SystemPropertiesPlugin.class)));
        diagnostics.register(
                new MetricsPlugin(loggingService.getLogger(MetricsPlugin.class), metricsRegistry, properties));
        diagnostics.register(
                new SystemLogPlugin(properties, connectionManager, this, loggingService.getLogger(SystemLogPlugin.class)));
        diagnostics.register(
                new NetworkingImbalancePlugin(properties, connectionManager.getEventLoopGroup(),
                        loggingService.getLogger(NetworkingImbalancePlugin.class)));

        metricsRegistry.collectMetrics(listenerService);

        proxyManager.init(config, clientContext);
        listenerService.start();
        loadBalancer.init(getCluster(), config);
        partitionService.start();
        statistics.start();
        clientExtension.afterStart(this);
    }

    public void onClusterConnect(Connection ownerConnection) throws Exception {
        clusterService.listenMembershipEvents(ownerConnection);
        userCodeDeploymentService.deploy(this, ownerConnection);
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
        return getDistributedObject(QueueService.SERVICE_NAME, name);
    }

    @Override
    public <E> ITopic<E> getTopic(String name) {
        return getDistributedObject(TopicService.SERVICE_NAME, name);
    }

    @Override
    public <E> ISet<E> getSet(String name) {
        return getDistributedObject(SetService.SERVICE_NAME, name);
    }

    @Override
    public <E> IList<E> getList(String name) {
        return getDistributedObject(ListService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> IMap<K, V> getMap(String name) {
        return getDistributedObject(MapService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getDistributedObject(MultiMapService.SERVICE_NAME, name);

    }

    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(String name) {
        return getDistributedObject(ReplicatedMapService.SERVICE_NAME, name);
    }

    @Override
    public JobTracker getJobTracker(String name) {
        return getDistributedObject(MapReduceService.SERVICE_NAME, name);
    }

    @Override
    public ILock getLock(String key) {
        return getDistributedObject(LockServiceImpl.SERVICE_NAME, key);
    }

    @Override
    public <E> ITopic<E> getReliableTopic(String name) {
        return getDistributedObject(ReliableTopicService.SERVICE_NAME, name);
    }

    @Override
    public <E> Ringbuffer<E> getRingbuffer(String name) {
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
        return getDistributedObject(DistributedExecutorService.SERVICE_NAME, name);
    }

    @Override
    public DurableExecutorService getDurableExecutorService(String name) {
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
        return getDistributedObject(IdGeneratorService.SERVICE_NAME, name);
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        return getDistributedObject(AtomicLongService.SERVICE_NAME, name);
    }

    @Override
    public CardinalityEstimator getCardinalityEstimator(String name) {
        return getDistributedObject(CardinalityEstimatorService.SERVICE_NAME, name);
    }

    @Override
    public <E> IAtomicReference<E> getAtomicReference(String name) {
        return getDistributedObject(AtomicReferenceService.SERVICE_NAME, name);
    }

    @Override
    public ICountDownLatch getCountDownLatch(String name) {
        return getDistributedObject(CountDownLatchService.SERVICE_NAME, name);
    }

    @Override
    public ISemaphore getSemaphore(String name) {
        return getDistributedObject(SemaphoreService.SERVICE_NAME, name);
    }

    @Override
    public IScheduledExecutorService getScheduledExecutorService(String name) {
        return getDistributedObject(DistributedScheduledExecutorService.SERVICE_NAME, name);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        try {
            ClientMessage request = ClientGetDistributedObjectsCodec.encodeRequest();
            final Future<ClientMessage> future = new ClientInvocation(this, request).invoke();
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
                proxyManager.removeProxy(distributedObjectInfo.getServiceName(), distributedObjectInfo.getName());
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
    public ConcurrentMap<String, Object> getUserContext() {
        return userContext;
    }

    public ClientConfig getClientConfig() {
        return config;
    }

    @Override
    public SerializationService getSerializationService() {
        return serializationService;
    }

    public ClientUserCodeDeploymentService getUserCodeDeploymentService() {
        return userCodeDeploymentService;
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

    public CallIdSequence getCallIdSequence() {
        return callIdSequence;
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public short getProtocolVersion() {
        return PROTOCOL_VERSION;
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    public void doShutdown() {
        proxyManager.destroy();
        connectionManager.shutdown();
        clusterService.shutdown();
        partitionService.stop();
        transactionManager.shutdown();
        invocationService.shutdown();
        executionService.shutdown();
        listenerService.shutdown();
        nearCacheManager.destroyAllNearCaches();
        if (discoveryService != null) {
            discoveryService.destroy();
        }
        metricsRegistry.shutdown();
        diagnostics.shutdown();
        ((InternalSerializationService) serializationService).dispose();
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
}
