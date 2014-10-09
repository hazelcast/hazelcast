package com.hazelcast.client.impl;

import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.ClientOutOfMemoryHandler;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.client.GetDistributedObjectsRequest;
import com.hazelcast.client.impl.client.txn.ClientTransactionManager;
import com.hazelcast.client.proxy.ClientClusterProxy;
import com.hazelcast.client.proxy.PartitionServiceProxy;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.client.spi.impl.AwsAddressTranslator;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.client.spi.impl.ClientListenerServiceImpl;
import com.hazelcast.client.spi.impl.ClientPartitionServiceImpl;
import com.hazelcast.client.spi.impl.DefaultAddressTranslator;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockProxy;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.Config;
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
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ServiceLoader;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class HazelcastClientInstanceImpl implements HazelcastInstance {

    static {
        OutOfMemoryErrorDispatcher.setClientHandler(new ClientOutOfMemoryHandler());
    }

    private static final AtomicInteger CLIENT_ID = new AtomicInteger();
    private static final ConcurrentMap<Integer, HazelcastClientProxy> CLIENTS
            = new ConcurrentHashMap<Integer, HazelcastClientProxy>(5);
    private static final ILogger LOGGER = Logger.getLogger(HazelcastClient.class);

    private final ClientProperties clientProperties;
    private final int id = CLIENT_ID.getAndIncrement();
    private final String instanceName;
    private final ClientConfig config;
    private final ThreadGroup threadGroup;
    private final LifecycleServiceImpl lifecycleService;
    private final SerializationService serializationService;
    private final ClientConnectionManager connectionManager;
    private final ClientClusterServiceImpl clusterService;
    private final ClientPartitionServiceImpl partitionService;
    private final ClientInvocationServiceImpl invocationService;
    private final ClientExecutionServiceImpl executionService;
    private final ClientListenerServiceImpl listenerService;
    private final ClientTransactionManager transactionManager;
    private final ProxyManager proxyManager;
    private final ConcurrentMap<String, Object> userContext;
    private final LoadBalancer loadBalancer;
    private final ClientExtension clientExtension;

    public HazelcastClientInstanceImpl(ClientConfig config) {
        this.config = config;
        final GroupConfig groupConfig = config.getGroupConfig();
        instanceName = "hz.client_" + id + (groupConfig != null ? "_" + groupConfig.getName() : "");
        clientExtension = createClientInitializer(config.getClassLoader());
        clientExtension.beforeStart(this);

        threadGroup = new ThreadGroup(instanceName);
        lifecycleService = new LifecycleServiceImpl(this);
        clientProperties = new ClientProperties(config);
        serializationService = clientExtension.createSerializationService();
        proxyManager = new ProxyManager(this);
        executionService = initExecutorService();
        transactionManager = new ClientTransactionManager(this);
        LoadBalancer lb = config.getLoadBalancer();
        if (lb == null) {
            lb = new RoundRobinLB();
        }
        loadBalancer = lb;
        connectionManager = createClientConnectionManager();
        clusterService = new ClientClusterServiceImpl(this);
        invocationService = new ClientInvocationServiceImpl(this);
        listenerService = initListenerService();
        userContext = new ConcurrentHashMap<String, Object>();
        proxyManager.init(config);
        partitionService = new ClientPartitionServiceImpl(this);
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
        int eventQueueCapacity = clientProperties.getEventQueueCapacity().getInteger();
        int eventThreadCount = clientProperties.getEventThreadCount().getInteger();
        return new ClientListenerServiceImpl(this, eventThreadCount, eventQueueCapacity);
    }

    private ClientExecutionServiceImpl initExecutorService() {
        return new ClientExecutionServiceImpl(instanceName, threadGroup,
                config.getClassLoader(), config.getExecutorPoolSize());
    }


    public void start() {
        lifecycleService.setStarted();
        connectionManager.start();
        try {
            clusterService.start();
        } catch (IllegalStateException e) {
            //there was an authentication failure
            // (todo: perhaps use an AuthenticationException ??)
            lifecycleService.shutdown();
            throw e;
        }
        loadBalancer.init(getCluster(), config);
        partitionService.start();
        clientExtension.afterStart(this);
    }

    ClientConnectionManager createClientConnectionManager() {
        final ClientAwsConfig awsConfig = config.getNetworkConfig().getAwsConfig();
        AddressTranslator addressTranslator;
        if (awsConfig != null && awsConfig.isEnabled()) {
            try {
                addressTranslator = new AwsAddressTranslator(awsConfig);
            } catch (NoClassDefFoundError e) {
                LOGGER.log(Level.WARNING, "hazelcast-cloud.jar might be missing!");
                throw e;
            }
        } else {
            addressTranslator = new DefaultAddressTranslator();
        }
        return new ClientConnectionManagerImpl(this, loadBalancer, addressTranslator);
    }

    public Config getConfig() {
        throw new UnsupportedOperationException("Client cannot access cluster config!");
    }

    public ClientProperties getClientProperties() {
        return clientProperties;
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
    @Deprecated
    public ILock getLock(Object key) {
        //this method will be deleted in the near future.
        String name = LockProxy.convertToStringKey(key, serializationService);
        return getDistributedObject(LockServiceImpl.SERVICE_NAME, name);
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
            GetDistributedObjectsRequest request = new GetDistributedObjectsRequest();
            final Future<SerializableCollection> future = invocationService.invokeOnRandomTarget(request);
            final SerializableCollection serializableCollection = serializationService.toObject(future.get());
            for (Data data : serializableCollection) {
                final DistributedObjectInfo o = serializationService.toObject(data);
                getDistributedObject(o.getServiceName(), o.getName());
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
        return new PartitionServiceProxy(partitionService);
    }

    @Override
    public ClientService getClientService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LoggingService getLoggingService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LifecycleService getLifecycleService() {
        return lifecycleService;
    }

    @Override
    @Deprecated
    public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
        if (id instanceof String) {
            return (T) proxyManager.getOrCreateProxy(serviceName, (String) id);
        }
        throw new IllegalArgumentException("'id' must be type of String!");
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

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    public ClientExtension getClientExtension() {
        return clientExtension;
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    public void doShutdown() {
        CLIENTS.remove(id);
        proxyManager.destroy();
        executionService.shutdown();
        partitionService.stop();
        clusterService.stop();
        transactionManager.shutdown();
        connectionManager.shutdown();
        invocationService.shutdown();
        listenerService.shutdown();
        serializationService.destroy();
    }
}
