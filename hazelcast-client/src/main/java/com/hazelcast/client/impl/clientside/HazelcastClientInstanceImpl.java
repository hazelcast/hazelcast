/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientAliasedDiscoveryConfigUtils;
import com.hazelcast.client.config.ClientCloudConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.connection.nio.DefaultCredentialsFactory;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
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
import com.hazelcast.client.spi.impl.AbstractClientInvocationService;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientPartitionServiceImpl;
import com.hazelcast.client.spi.impl.ClientTransactionManagerServiceImpl;
import com.hazelcast.client.spi.impl.ClientUserCodeDeploymentService;
import com.hazelcast.client.spi.impl.DefaultAddressProvider;
import com.hazelcast.client.spi.impl.DefaultAddressTranslator;
import com.hazelcast.client.spi.impl.NonSmartClientInvocationService;
import com.hazelcast.client.spi.impl.SmartClientInvocationService;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressProvider;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressTranslator;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudAddressProvider;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudAddressTranslator;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudDiscovery;
import com.hazelcast.client.spi.impl.listener.AbstractClientListenerService;
import com.hazelcast.client.spi.impl.listener.NonSmartClientListenerService;
import com.hazelcast.client.spi.impl.listener.SmartClientListenerService;
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
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
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
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
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
import com.hazelcast.util.ServiceLoader;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.spi.properties.ClientProperty.DISCOVERY_SPI_ENABLED;
import static com.hazelcast.client.spi.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN;
import static com.hazelcast.config.AliasedDiscoveryConfigUtils.allUsePublicAddress;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;
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
    private final AbstractClientInvocationService invocationService;
    private final ClientExecutionServiceImpl executionService;
    private final AbstractClientListenerService listenerService;
    private final ClientTransactionManagerServiceImpl transactionManager;
    private final NearCacheManager nearCacheManager;
    private final ProxyManager proxyManager;
    private final ConcurrentMap<String, Object> userContext;
    private final LoadBalancer loadBalancer;
    private final ClientExtension clientExtension;
    private final ICredentialsFactory credentialsFactory;
    private final DiscoveryService discoveryService;
    private final LoggingService loggingService;
    private final MetricsRegistryImpl metricsRegistry;
    private final Statistics statistics;
    private final Diagnostics diagnostics;
    private final SerializationService serializationService;
    private final ClientICacheManager hazelcastCacheManager;
    private final ClientLockReferenceIdGenerator lockReferenceIdGenerator;
    private final ClientExceptionFactory clientExceptionFactory;
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

        credentialsFactory = initCredentialsFactory(config);
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
        AddressTranslator addressTranslator = createAddressTranslator();
        connectionManager = (ClientConnectionManagerImpl) clientConnectionManagerFactory
                .createConnectionManager(this, addressTranslator, addressProviders);
        clusterService = new ClientClusterServiceImpl(this);


        invocationService = initInvocationService();
        listenerService = initListenerService();
        userContext = new ConcurrentHashMap<String, Object>();
        userContext.putAll(config.getUserContext());
        diagnostics = initDiagnostics();

        hazelcastCacheManager = new ClientICacheManager(this);

        lockReferenceIdGenerator = new ClientLockReferenceIdGenerator();
        nearCacheManager = clientExtension.createNearCacheManager();
        clientExceptionFactory = initClientExceptionFactory();
        statistics = new Statistics(this);
        userCodeDeploymentService = new ClientUserCodeDeploymentService(config.getUserCodeDeploymentConfig(), classLoader);
    }

    private int getConnectionTimeoutMillis() {
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        int connTimeout = networkConfig.getConnectionTimeout();
        return connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;
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
        metricsRegistry.scanAndRegister(clientExtension.getMemoryStats(), "memory");
        return metricsRegistry;
    }

    private Collection<AddressProvider> createAddressProviders(AddressProvider externalAddressProvider) {
        ClientNetworkConfig networkConfig = getClientConfig().getNetworkConfig();
        Collection<AddressProvider> addressProviders = new LinkedList<AddressProvider>();

        if (externalAddressProvider != null) {
            addressProviders.add(externalAddressProvider);
        }

        if (discoveryService != null) {
            addressProviders.add(new DiscoveryAddressProvider(discoveryService, loggingService));
        }

        ClientCloudConfig cloudConfig = networkConfig.getCloudConfig();
        HazelcastCloudAddressProvider cloudAddressProvider = initCloudAddressProvider(cloudConfig);
        if (cloudAddressProvider != null) {
            addressProviders.add(cloudAddressProvider);
        }

        addressProviders.add(new DefaultAddressProvider(networkConfig, addressProviders.isEmpty()));
        return addressProviders;
    }

    private HazelcastCloudAddressProvider initCloudAddressProvider(ClientCloudConfig cloudConfig) {
        if (cloudConfig.isEnabled()) {
            String discoveryToken = cloudConfig.getDiscoveryToken();
            String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(getProperties(), discoveryToken);
            return new HazelcastCloudAddressProvider(urlEndpoint, getConnectionTimeoutMillis(), loggingService);
        }

        String cloudToken = properties.getString(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN);
        if (cloudToken != null) {
            String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(getProperties(), cloudToken);
            return new HazelcastCloudAddressProvider(urlEndpoint, getConnectionTimeoutMillis(), loggingService);
        }
        return null;
    }

    private AddressTranslator createAddressTranslator() {
        ClientNetworkConfig networkConfig = getClientConfig().getNetworkConfig();
        ClientCloudConfig cloudConfig = networkConfig.getCloudConfig();

        List<String> addresses = networkConfig.getAddresses();
        boolean addressListProvided = addresses.size() != 0;
        boolean awsDiscoveryEnabled = networkConfig.getAwsConfig() != null && networkConfig.getAwsConfig().isEnabled();
        boolean gcpDiscoveryEnabled = networkConfig.getGcpConfig() != null && networkConfig.getGcpConfig().isEnabled();
        boolean azureDiscoveryEnabled = networkConfig.getAzureConfig() != null && networkConfig.getAzureConfig().isEnabled();
        boolean kubernetesDiscoveryEnabled = networkConfig.getKubernetesConfig() != null
                && networkConfig.getKubernetesConfig().isEnabled();
        boolean eurekaDiscoveryEnabled = networkConfig.getEurekaConfig() != null && networkConfig.getEurekaConfig().isEnabled();
        boolean discoverySpiEnabled = discoverySpiEnabled(networkConfig);
        String cloudDiscoveryToken = properties.getString(HAZELCAST_CLOUD_DISCOVERY_TOKEN);
        if (cloudDiscoveryToken != null && cloudConfig.isEnabled()) {
            throw new IllegalStateException("Ambiguous hazelcast.cloud configuration. "
                    + "Both property based and client configuration based settings are provided for "
                    + "Hazelcast cloud discovery together. Use only one.");
        }
        boolean hazelcastCloudEnabled = cloudDiscoveryToken != null || cloudConfig.isEnabled();
        isDiscoveryConfigurationConsistent(addressListProvided, awsDiscoveryEnabled, gcpDiscoveryEnabled, azureDiscoveryEnabled,
                kubernetesDiscoveryEnabled, eurekaDiscoveryEnabled, discoverySpiEnabled, hazelcastCloudEnabled);

        if (discoveryService != null) {
            return new DiscoveryAddressTranslator(discoveryService, usePublicAddress(config));
        } else if (hazelcastCloudEnabled) {
            String discoveryToken;
            if (cloudConfig.isEnabled()) {
                discoveryToken = cloudConfig.getDiscoveryToken();
            } else {
                discoveryToken = cloudDiscoveryToken;
            }
            String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(getProperties(), discoveryToken);
            return new HazelcastCloudAddressTranslator(urlEndpoint, getConnectionTimeoutMillis(), loggingService);
        }

        return new DefaultAddressTranslator();
    }

    private boolean discoverySpiEnabled(ClientNetworkConfig networkConfig) {
        return (networkConfig.getDiscoveryConfig() != null && networkConfig.getDiscoveryConfig().isEnabled())
                || Boolean.parseBoolean(properties.getString(DISCOVERY_SPI_ENABLED));
    }

    private boolean usePublicAddress(ClientConfig config) {
        return getProperties().getBoolean(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED)
                || allUsePublicAddress(ClientAliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(config));
    }

    @SuppressWarnings({"checkstyle:booleanexpressioncomplexity", "checkstyle:npathcomplexity"})
    private void isDiscoveryConfigurationConsistent(boolean addressListProvided, boolean awsDiscoveryEnabled,
                                                    boolean gcpDiscoveryEnabled, boolean azureDiscoveryEnabled,
                                                    boolean kubernetesDiscoveryEnabled, boolean eurekaDiscoveryEnabled,
                                                    boolean discoverySpiEnabled, boolean hazelcastCloudEnabled) {
        int count = 0;
        if (addressListProvided) {
            count++;
        }
        if (awsDiscoveryEnabled) {
            count++;
        }
        if (gcpDiscoveryEnabled) {
            count++;
        }
        if (azureDiscoveryEnabled) {
            count++;
        }
        if (kubernetesDiscoveryEnabled) {
            count++;
        }
        if (eurekaDiscoveryEnabled) {
            count++;
        }
        if (discoverySpiEnabled) {
            count++;
        }
        if (hazelcastCloudEnabled) {
            count++;
        }
        if (count > 1) {
            throw new IllegalStateException("Only one discovery method can be enabled at a time. "
                    + "cluster members given explicitly : " + addressListProvided
                    + ", aws discovery: " + awsDiscoveryEnabled
                    + ", gcp discovery: " + gcpDiscoveryEnabled
                    + ", azure discovery: " + azureDiscoveryEnabled
                    + ", kubernetes discovery: " + kubernetesDiscoveryEnabled
                    + ", eureka discovery: " + eurekaDiscoveryEnabled
                    + ", discovery spi enabled : " + discoverySpiEnabled
                    + ", hazelcast.cloud enabled : " + hazelcastCloudEnabled);
        }
    }

    private DiscoveryService initDiscoveryService(ClientConfig config) {
        // Prevent confusing behavior where the DiscoveryService is started
        // and strategies are resolved but the AddressProvider is never registered
        List<DiscoveryStrategyConfig> aliasedDiscoveryConfigs =
                ClientAliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);

        if (!properties.getBoolean(ClientProperty.DISCOVERY_SPI_ENABLED) && aliasedDiscoveryConfigs.isEmpty()) {
            return null;
        }

        ILogger logger = loggingService.getLogger(DiscoveryService.class);
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        DiscoveryConfig discoveryConfig = networkConfig.getDiscoveryConfig().getAsReadOnly();

        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }

        DiscoveryServiceSettings settings = new DiscoveryServiceSettings()
                .setConfigClassLoader(config.getClassLoader())
                .setLogger(logger)
                .setDiscoveryMode(DiscoveryMode.Client)
                .setAliasedDiscoveryConfigs(aliasedDiscoveryConfigs)
                .setDiscoveryConfig(discoveryConfig);

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

    private ICredentialsFactory initCredentialsFactory(ClientConfig config) {
        ClientSecurityConfig securityConfig = config.getSecurityConfig();
        validateSecurityConfig(securityConfig);
        ICredentialsFactory c = getCredentialsFromFactory(config);
        if (c == null) {
            return new DefaultCredentialsFactory(securityConfig, config.getGroupConfig(), config.getClassLoader());
        }
        return c;
    }

    private void validateSecurityConfig(ClientSecurityConfig securityConfig) {
        boolean configuredViaCredentials = securityConfig.getCredentials() != null
                || securityConfig.getCredentialsClassname() != null;

        CredentialsFactoryConfig factoryConfig = securityConfig.getCredentialsFactoryConfig();
        boolean configuredViaCredentialsFactory = factoryConfig.getClassName() != null
                || factoryConfig.getImplementation() != null;

        if (configuredViaCredentials && configuredViaCredentialsFactory) {
            throw new IllegalStateException("Ambiguous Credentials config. Set only one of Credentials or ICredentialsFactory");
        }
    }

    private ICredentialsFactory getCredentialsFromFactory(ClientConfig config) {
        CredentialsFactoryConfig credentialsFactoryConfig = config.getSecurityConfig().getCredentialsFactoryConfig();
        ICredentialsFactory factory = credentialsFactoryConfig.getImplementation();
        if (factory == null) {
            String factoryClassName = credentialsFactoryConfig.getClassName();
            if (factoryClassName != null) {
                try {
                    factory = ClassLoaderUtil.newInstance(config.getClassLoader(), factoryClassName);
                } catch (Exception e) {
                    throw rethrow(e);
                }
            }
        }
        if (factory == null) {
            return null;
        }
        factory.configure(config.getGroupConfig(), credentialsFactoryConfig.getProperties());
        return factory;
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
        int eventQueueCapacity = properties.getInteger(ClientProperty.EVENT_QUEUE_CAPACITY);
        int eventThreadCount = properties.getInteger(ClientProperty.EVENT_THREAD_COUNT);
        final ClientNetworkConfig networkConfig = config.getNetworkConfig();
        if (networkConfig.isSmartRouting()) {
            return new SmartClientListenerService(this, eventThreadCount, eventQueueCapacity);
        } else {
            return new NonSmartClientListenerService(this, eventThreadCount, eventQueueCapacity);
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
    }

    public void onClusterConnect(Connection ownerConnection) throws Exception {
        partitionService.listenPartitionTable(ownerConnection);
        clusterService.listenMembershipEvents(ownerConnection);
        userCodeDeploymentService.deploy(this, ownerConnection);
        proxyManager.createDistributedObjectsOnCluster(ownerConnection);
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
    public JobTracker getJobTracker(String name) {
        checkNotNull(name, "Retrieving a job tracker instance with a null name is not allowed!");
        return getDistributedObject(MapReduceService.SERVICE_NAME, name);
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

    public ICredentialsFactory getCredentialsFactory() {
        return credentialsFactory;
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
