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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.proxy.ClientClusterProxy;
import com.hazelcast.client.proxy.PartitionServiceProxy;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.client.spi.impl.AwsAddressTranslator;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.client.spi.impl.ClientPartitionServiceImpl;
import com.hazelcast.client.spi.impl.DefaultAddressTranslator;
import com.hazelcast.client.txn.ClientTransactionManager;
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
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.MapService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.queue.QueueService;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.topic.TopicService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Hazelcast Client enables you to do all Hazelcast operations without
 * being a member of the cluster. It connects to one of the
 * cluster members and delegates all cluster wide operations to it.
 * When the connected cluster member dies, client will
 * automatically switch to another live member.
 */
public final class HazelcastClient implements HazelcastInstance {

    static {
        OutOfMemoryErrorDispatcher.setClientHandler(new ClientOutOfMemoryHandler());
    }

    private static final AtomicInteger CLIENT_ID = new AtomicInteger();
    private static final ConcurrentMap<Integer, HazelcastClientProxy> CLIENTS
            = new ConcurrentHashMap<Integer, HazelcastClientProxy>(5);
    private static final ILogger LOGGER = Logger.getLogger(HazelcastClient.class);
    public final ClientProperties clientProperties;
    private final int id = CLIENT_ID.getAndIncrement();
    private final String instanceName;
    private final ClientConfig config;
    private final ThreadGroup threadGroup;
    private final LifecycleServiceImpl lifecycleService;
    private final SerializationServiceImpl serializationService;
    private final ClientConnectionManager connectionManager;
    private final ClientClusterServiceImpl clusterService;
    private final ClientPartitionServiceImpl partitionService;
    private final ClientInvocationServiceImpl invocationService;
    private final ClientExecutionServiceImpl executionService;
    private final ClientTransactionManager transactionManager;
    private final ProxyManager proxyManager;
    private final ConcurrentMap<String, Object> userContext;
    private final LoadBalancer loadBalancer;


    private HazelcastClient(ClientConfig config) {
        this.config = config;
        final GroupConfig groupConfig = config.getGroupConfig();
        instanceName = "hz.client_" + id + (groupConfig != null ? "_" + groupConfig.getName() : "");
        threadGroup = new ThreadGroup(instanceName);
        lifecycleService = new LifecycleServiceImpl(this);
        clientProperties = new ClientProperties(config);
        SerializationService ss;
        try {
            String partitioningStrategyClassName = System.getProperty(GroupProperties.PROP_PARTITIONING_STRATEGY_CLASS);
            final PartitioningStrategy partitioningStrategy;
            if (partitioningStrategyClassName != null && partitioningStrategyClassName.length() > 0) {
                partitioningStrategy = ClassLoaderUtil.newInstance(config.getClassLoader(), partitioningStrategyClassName);
            } else {
                partitioningStrategy = new DefaultPartitioningStrategy();
            }
            ss = new SerializationServiceBuilder()
                    .setManagedContext(new HazelcastClientManagedContext(this, config.getManagedContext()))
                    .setClassLoader(config.getClassLoader())
                    .setConfig(config.getSerializationConfig())
                    .setPartitioningStrategy(partitioningStrategy)
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        serializationService = (SerializationServiceImpl) ss;
        proxyManager = new ProxyManager(this);
        executionService = new ClientExecutionServiceImpl(instanceName, threadGroup,
                Thread.currentThread().getContextClassLoader(), config.getExecutorPoolSize());
        transactionManager = new ClientTransactionManager(this);
        LoadBalancer lb = config.getLoadBalancer();
        if (lb == null) {
            lb = new RoundRobinLB();
        }
        loadBalancer = lb;
        connectionManager = createClientConnectionManager();
        clusterService = new ClientClusterServiceImpl(this);
        invocationService = new ClientInvocationServiceImpl(this);
        userContext = new ConcurrentHashMap<String, Object>();
        proxyManager.init(config);
        partitionService = new ClientPartitionServiceImpl(this);
    }

    public static HazelcastInstance newHazelcastClient() {
        return newHazelcastClient(new XmlClientConfigBuilder().build());
    }

    public static HazelcastInstance newHazelcastClient(ClientConfig config) {
        if (config == null) {
            config = new XmlClientConfigBuilder().build();
        }

        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        HazelcastClientProxy proxy;
        try {
            Thread.currentThread().setContextClassLoader(HazelcastClient.class.getClassLoader());
            final HazelcastClient client = new HazelcastClient(config);
            client.start();
            OutOfMemoryErrorDispatcher.register(client);
            proxy = new HazelcastClientProxy(client);
            CLIENTS.put(client.id, proxy);
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
        return proxy;
    }

    public static Collection<HazelcastInstance> getAllHazelcastClients() {
        return Collections.<HazelcastInstance>unmodifiableCollection(CLIENTS.values());
    }

    public static void shutdownAll() {
        for (HazelcastClientProxy proxy : CLIENTS.values()) {
            try {
                proxy.client.getLifecycleService().shutdown();
            } catch (Exception ignored) {
            }
            proxy.client = null;
        }
        CLIENTS.clear();
    }

    private void start() {
        lifecycleService.setStarted();
        connectionManager.start();
        try {
            clusterService.start();
        } catch (IllegalStateException e) {
            //there was an authentication failure (todo: perhaps use an AuthenticationException
            // ??)
            lifecycleService.shutdown();
            throw e;
        }
        loadBalancer.init(getCluster(), config);
        partitionService.start();
    }

    ClientConnectionManagerImpl createClientConnectionManager() {
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
            return (T) proxyManager.getProxy(serviceName, (String) id);
        }
        throw new IllegalArgumentException("'id' must be type of String!");
    }

    @Override
    public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
        return (T) proxyManager.getProxy(serviceName, name);
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

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    void doShutdown() {
        CLIENTS.remove(id);
        executionService.shutdown();
        partitionService.stop();
        clusterService.stop();
        transactionManager.shutdown();
        connectionManager.shutdown();
        proxyManager.destroy();
        serializationService.destroy();
    }
}
