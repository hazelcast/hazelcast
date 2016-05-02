package com.hazelcast.client.impl;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import com.hazelcast.client.proxy.ClientClusterProxy;
import com.hazelcast.client.proxy.PartitionServiceProxy;
import com.hazelcast.client.spi.ClientClusterService;
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
import com.hazelcast.client.spi.impl.ClientNonSmartInvocationServiceImpl;
import com.hazelcast.client.spi.impl.ClientPartitionServiceImpl;
import com.hazelcast.client.spi.impl.ClientServiceNotFoundException;
import com.hazelcast.client.spi.impl.ClientSmartInvocationServiceImpl;
import com.hazelcast.client.spi.impl.ClientTransactionManagerServiceImpl;
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
import com.hazelcast.core.HazelcastException;
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
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.monitors.ConfigPropertiesPlugin;
import com.hazelcast.internal.monitors.MetricsPlugin;
import com.hazelcast.internal.monitors.PerformanceMonitor;
import com.hazelcast.internal.monitors.SystemPropertiesPlugin;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.spi.exception.ServiceNotFoundException;
import com.hazelcast.spi.impl.SerializationServiceSupport;
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
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ServiceLoader;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.System.currentTimeMillis;

public class HazelcastClientInstanceImpl implements HazelcastInstance, SerializationServiceSupport {

    private static final AtomicInteger CLIENT_ID = new AtomicInteger();
    private static final short protocolVersion = ClientMessage.VERSION;

    private final HazelcastProperties properties;
    private final int id = CLIENT_ID.getAndIncrement();
    private final String instanceName;
    private final ClientConfig config;
    private final ThreadGroup threadGroup;
    private final LifecycleServiceImpl lifecycleService;
    private final ClientConnectionManager connectionManager;
    private final ClientClusterServiceImpl clusterService;
    private final ClientPartitionServiceImpl partitionService;
    private final ClientInvocationService invocationService;
    private final ClientExecutionServiceImpl executionService;
    private final ClientListenerServiceImpl listenerService;
    private final ClientTransactionManagerService transactionManager;
    private final NearCacheManager nearCacheManager;
    private final ProxyManager proxyManager;
    private final ConcurrentMap<String, Object> userContext;
    private final LoadBalancer loadBalancer;
    private final ClientExtension clientExtension;
    private final Credentials credentials;
    private final DiscoveryService discoveryService;
    private final LoggingService loggingService;
    private final MetricsRegistryImpl metricsRegistry;
    private final PerformanceMonitor performanceMonitor;
    private final SerializationService serializationService;

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
        String loggingType = GroupProperty.LOGGING_TYPE.getSystemProperty();
        loggingService = new ClientLoggingService(groupConfig.getName(),
                loggingType, BuildInfoProvider.getBuildInfo(), instanceName);
        clientExtension = createClientInitializer(config.getClassLoader());
        clientExtension.beforeStart(this);

        credentials = initCredentials(config);
        threadGroup = new ThreadGroup(instanceName);
        lifecycleService = new LifecycleServiceImpl(this);
        properties = new HazelcastProperties(config.getProperties());

        metricsRegistry = initMetricsRegistry();
        serializationService = clientExtension.createSerializationService((byte) -1);
        proxyManager = new ProxyManager(this);
        executionService = initExecutionService();
        loadBalancer = initLoadBalancer(config);
        transactionManager = new ClientTransactionManagerServiceImpl(this, loadBalancer);
        partitionService = new ClientPartitionServiceImpl(this);
        discoveryService = initDiscoveryService(config);
        connectionManager = clientConnectionManagerFactory.createConnectionManager(config, this, discoveryService);
        Collection<AddressProvider> addressProviders = createAddressProviders(externalAddressProvider);
        clusterService = new ClientClusterServiceImpl(this, addressProviders);
        invocationService = initInvocationService();
        listenerService = initListenerService();
        userContext = new ConcurrentHashMap<String, Object>();
        nearCacheManager = clientExtension.createNearCacheManager();

        performanceMonitor = initPerformanceMonitor(config);

        proxyManager.init(config);
    }

    private PerformanceMonitor initPerformanceMonitor(ClientConfig config) {
        String name = "performance-client-" + id + "-" + currentTimeMillis();
        ILogger logger = loggingService.getLogger(PerformanceMonitor.class);
        HazelcastThreadGroup hzThreadGroup = new HazelcastThreadGroup(getName(), logger, config.getClassLoader());
        return new PerformanceMonitor(name, logger, hzThreadGroup, properties);
    }

    private MetricsRegistryImpl initMetricsRegistry() {
        ProbeLevel probeLevel = properties.getEnum(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.class);
        return new MetricsRegistryImpl(loggingService.getLogger(MetricsRegistryImpl.class), probeLevel);
    }

    private Collection<AddressProvider> createAddressProviders(AddressProvider externalAddressProvider) {
        ClientNetworkConfig networkConfig = getClientConfig().getNetworkConfig();
        final ClientAwsConfig awsConfig = networkConfig.getAwsConfig();
        Collection<AddressProvider> addressProviders = new LinkedList<AddressProvider>();

        addressProviders.add(new DefaultAddressProvider(networkConfig));
        if (externalAddressProvider != null) {
            addressProviders.add(externalAddressProvider);
        }

        if (discoveryService != null) {
            addressProviders.add(new DiscoveryAddressProvider(discoveryService, loggingService));
        }

        if (properties.getBoolean(ClientProperty.DISCOVERY_SPI_ENABLED)) {
            discoveryService.start();
        }

        if (awsConfig != null && awsConfig.isEnabled()) {
            try {
                addressProviders.add(new AwsAddressProvider(awsConfig, loggingService));
            } catch (NoClassDefFoundError e) {
                ILogger logger = loggingService.getLogger(HazelcastClient.class);
                logger.log(Level.WARNING, "hazelcast-cloud.jar might be missing!");
                throw e;
            }
        }
        return addressProviders;
    }

    private DiscoveryService initDiscoveryService(ClientConfig config) {
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        DiscoveryConfig discoveryConfig = networkConfig.getDiscoveryConfig().getAsReadOnly();
        if (discoveryConfig == null || !discoveryConfig.isEnabled()) {
            return null;
        }
        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }
        ILogger logger = loggingService.getLogger(DiscoveryService.class);

        DiscoveryServiceSettings settings = new DiscoveryServiceSettings().setConfigClassLoader(config.getClassLoader())
                .setLogger(logger).setDiscoveryMode(DiscoveryMode.Client).setDiscoveryConfig(discoveryConfig);

        return factory.newDiscoveryService(settings);
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
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
        if (c == null) {
            c = new UsernamePasswordCredentials(groupConfig.getName(), groupConfig.getPassword());
        }
        return c;
    }

    private ClientInvocationService initInvocationService() {
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
            throw ExceptionUtil.rethrow(e);
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
        return new ClientExecutionServiceImpl(instanceName, threadGroup,
                config.getClassLoader(), properties, config.getExecutorPoolSize(), loggingService);
    }

    public void start() {
        lifecycleService.setStarted();
        invocationService.start();
        connectionManager.start();
        try {
            clusterService.start();
        } catch (Exception e) {
            lifecycleService.shutdown();
            throw ExceptionUtil.rethrow(e);
        }
        listenerService.start();
        loadBalancer.init(getCluster(), config);
        partitionService.start();
        clientExtension.afterStart(this);

        performanceMonitor.start();
        performanceMonitor.register(
                new ConfigPropertiesPlugin(loggingService.getLogger(ConfigPropertiesPlugin.class), properties));
        performanceMonitor.register(
                new SystemPropertiesPlugin(loggingService.getLogger(SystemPropertiesPlugin.class)));
        performanceMonitor.register(
                new MetricsPlugin(loggingService.getLogger(MetricsPlugin.class), metricsRegistry, properties));
    }

    public MetricsRegistryImpl getMetricsRegistry() {
        return metricsRegistry;
    }

    @Override
    public HazelcastXAResource getXAResource() {
        return getDistributedObject(XAService.SERVICE_NAME, XAService.SERVICE_NAME);
    }

    public Config getConfig() {
        throw new UnsupportedOperationException("Client cannot access cluster config!");
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
    public <K, V> ICache<K, V> getCache(String name) {
        checkNotNull(name, "Retrieving a cache instance with a null name is not allowed!");
        return getCacheByFullName(HazelcastCacheManager.CACHE_MANAGER_PREFIX + name);
    }

    public <K, V> ICache<K, V> getCacheByFullName(String fullName) {
        checkNotNull(fullName, "Retrieving a cache instance with a null name is not allowed!");
        try {
            return getDistributedObject(ICacheService.SERVICE_NAME, fullName);
        } catch (ClientServiceNotFoundException e) {
            // Cache support is not available at client side
            throw new IllegalStateException("At client, " + ICacheService.CACHE_SUPPORT_NOT_AVAILABLE_ERROR_MESSAGE);
        } catch (HazelcastException e) {
            if (e.getCause() instanceof ServiceNotFoundException) {
                // Cache support is not available at server side
                throw new IllegalStateException("At server, " + ICacheService.CACHE_SUPPORT_NOT_AVAILABLE_ERROR_MESSAGE);
            } else {
                throw e;
            }
        }
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
    public Collection<DistributedObject> getDistributedObjects() {
        try {
            ClientMessage request = ClientGetDistributedObjectsCodec.encodeRequest();
            final Future<ClientMessage> future = new ClientInvocation(this, request).invoke();
            ClientMessage response = future.get();
            ClientGetDistributedObjectsCodec.ResponseParameters resultParameters =
                    ClientGetDistributedObjectsCodec.decodeResponse(response);

            Collection<DistributedObjectInfo> infoCollection = resultParameters.response;
            for (DistributedObjectInfo distributedObjectInfo : infoCollection) {
                getDistributedObject(distributedObjectInfo.getServiceName(), distributedObjectInfo.getName());
            }
            return (Collection<DistributedObject>) proxyManager.getDistributedObjects();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
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

    public SerializationService getSerializationService() {
        return serializationService;
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

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    public ClientExtension getClientExtension() {
        return clientExtension;
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public short getProtocolVersion() {
        return protocolVersion;
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    public void doShutdown() {
        proxyManager.destroy();
        connectionManager.shutdown();
        clusterService.shutdown();
        executionService.shutdown();
        partitionService.stop();
        transactionManager.shutdown();
        invocationService.shutdown();
        listenerService.shutdown();
        ((InternalSerializationService) serializationService).dispose();
        nearCacheManager.destroyAllNearCaches();
        if (discoveryService != null) {
            discoveryService.destroy();
        }
        metricsRegistry.shutdown();
        performanceMonitor.shutdown();
    }

}
