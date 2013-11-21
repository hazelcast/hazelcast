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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.DummyClientConnectionManager;
import com.hazelcast.client.connection.SmartClientConnectionManager;
import com.hazelcast.client.proxy.ClientClusterProxy;
import com.hazelcast.client.proxy.PartitionServiceProxy;
import com.hazelcast.client.spi.*;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.client.spi.impl.ClientPartitionServiceImpl;
import com.hazelcast.client.txn.TransactionContextProxy;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.concurrent.lock.proxy.LockProxy;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.*;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.queue.QueueService;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hazelcast Client enables you to do all Hazelcast operations without
 * being a member of the cluster. It connects to one of the
 * cluster members and delegates all cluster wide operations to it.
 * When the connected cluster member dies, client will
 * automatically switch to another live member.
 */
public final class HazelcastClient implements HazelcastInstance {

    private final static AtomicInteger CLIENT_ID = new AtomicInteger();
    private final static ConcurrentMap<Integer, HazelcastClientProxy> CLIENTS = new ConcurrentHashMap<Integer, HazelcastClientProxy>(5);
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
    private final ProxyManager proxyManager;
    private final ConcurrentMap<String, Object> userContext;

    private HazelcastClient(ClientConfig config) {
        this.config = config;
        final GroupConfig groupConfig = config.getGroupConfig();
        instanceName = "hz.client_" + id + (groupConfig != null ? "_" + groupConfig.getName() : "");
        threadGroup = new ThreadGroup(instanceName);
        lifecycleService = new LifecycleServiceImpl(this);
        try {
            String partitioningStrategyClassName = System.getProperty(GroupProperties.PROP_PARTITIONING_STRATEGY_CLASS);
            final PartitioningStrategy partitioningStrategy;
            if (partitioningStrategyClassName != null && partitioningStrategyClassName.length() > 0) {
                partitioningStrategy = ClassLoaderUtil.newInstance(config.getClassLoader(), partitioningStrategyClassName);
            } else {
                partitioningStrategy = new DefaultPartitioningStrategy();
            }
            serializationService = new SerializationServiceBuilder()
                    .setManagedContext(new HazelcastClientManagedContext(this, config.getManagedContext()))
                    .setClassLoader(config.getClassLoader())
                    .setConfig(config.getSerializationConfig())
                    .setPartitioningStrategy(partitioningStrategy)
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        proxyManager = new ProxyManager(this);
        executionService = new ClientExecutionServiceImpl(instanceName, threadGroup, Thread.currentThread().getContextClassLoader(), config.getExecutorPoolSize());
        clusterService = new ClientClusterServiceImpl(this);
        LoadBalancer loadBalancer = config.getLoadBalancer();
        if (loadBalancer == null) {
            loadBalancer = new RoundRobinLB();
        }
        if (config.isSmartRouting()) {
            connectionManager = new SmartClientConnectionManager(this, clusterService.getAuthenticator(), loadBalancer);
        } else {
            connectionManager = new DummyClientConnectionManager(this, clusterService.getAuthenticator(), loadBalancer);
        }
        invocationService = new ClientInvocationServiceImpl(this);
        userContext = new ConcurrentHashMap<String, Object>();
        loadBalancer.init(getCluster(), config);
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
        try{
            Thread.currentThread().setContextClassLoader(HazelcastClient.class.getClassLoader());
            final HazelcastClient client = new HazelcastClient(config);
            client.start();

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

    private void start(){
        lifecycleService.setStarted();

        try{
            clusterService.start();
        }catch(IllegalStateException e){
            //there was an authentication failure (todo: perhaps use an AuthenticationException
            // ??)
            lifecycleService.shutdown();
            throw e;
        }
        partitionService.start();
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
    public ILock getLock(String key) {
        return getDistributedObject(LockServiceImpl.SERVICE_NAME, key);
    }

    @Override
    @Deprecated
    public ILock getLock(Object key) {
        //this method will be deleted in the near future.
        return getDistributedObject(LockServiceImpl.SERVICE_NAME, LockProxy.convertToStringKey(key, serializationService));
    }

    @Override
    public Cluster getCluster() {
        return new ClientClusterProxy(clusterService);
    }

    @Override
    public IExecutorService getExecutorService(String name) {
        return getDistributedObject(DistributedExecutorService.SERVICE_NAME, name);
    }

    @Override
    public <T> T executeTransaction(TransactionalTask <T> task) throws TransactionException {
        return executeTransaction(TransactionOptions.getDefault(), task);
    }

    @Override
    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        final TransactionContext context = newTransactionContext(options);
        context.beginTransaction();
        try {
            final T value = task.execute(context);
            context.commitTransaction();
            return value;
        } catch (Throwable e) {
            context.rollbackTransaction();
            if (e instanceof TransactionException) {
                throw (TransactionException) e;
            }
            if (e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new TransactionException(e);
        }
    }

    @Override
    public TransactionContext newTransactionContext() {
        return newTransactionContext(TransactionOptions.getDefault());
    }

    @Override
    public TransactionContext newTransactionContext(TransactionOptions options) {
        return new TransactionContextProxy(this, options);
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
            final SerializableCollection serializableCollection = (SerializableCollection) invocationService.invokeOnRandomTarget(request);
            for (Data data : serializableCollection) {
                final DistributedObjectInfo o = (DistributedObjectInfo) serializationService.toObject(data);
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
        connectionManager.shutdown();
        proxyManager.destroy();
    }
}
