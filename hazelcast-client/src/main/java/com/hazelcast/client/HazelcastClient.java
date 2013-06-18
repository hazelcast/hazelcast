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
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.set.ObjectSetProxy;
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
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.queue.QueueService;
import com.hazelcast.topic.TopicService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
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
    private final String name;
    private final ClientConfig config;
    private final ThreadGroup threadGroup;
    private final LifecycleServiceImpl lifecycleService;
    private final SerializationServiceImpl serializationService;
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
        name = "hz.client_" + id + (groupConfig != null ? "_" + groupConfig.getName() : "");
        threadGroup = new ThreadGroup(name);
        lifecycleService = new LifecycleServiceImpl(this);
        proxyManager = new ProxyManager(this);
        executionService = new ClientExecutionServiceImpl(name, threadGroup, Thread.currentThread().getContextClassLoader());
        clusterService = new ClientClusterServiceImpl(this);
        serializationService = (SerializationServiceImpl) new SerializationServiceBuilder()
            .setClassLoader( config.getClassLoader() ).setConfig( config.getSerializationConfig() ).build();
        LoadBalancer loadBalancer = config.getLoadBalancer();
        if (loadBalancer == null) {
            loadBalancer = new RoundRobinLB();
        }
        if (config.isSmart()){
            connectionManager = new SmartClientConnectionManager(this, clusterService.getAuthenticator(), loadBalancer);
        } else {
            connectionManager = new DummyClientConnectionManager(this, clusterService.getAuthenticator(), loadBalancer);
        }
        invocationService = new ClientInvocationServiceImpl(this);
        userContext = new ConcurrentHashMap<String, Object>();
        clusterService.start();
        loadBalancer.init(getCluster(), config);
        proxyManager.init(config.getProxyFactoryConfig());
        lifecycleService.setStarted();
        partitionService = new ClientPartitionServiceImpl(this);
        partitionService.start();
    }

    public static HazelcastInstance newHazelcastClient(ClientConfig config) {
        if (config == null) {
            config = new ClientConfig();
        }
        final HazelcastClient client = new HazelcastClient(config);
        final HazelcastClientProxy proxy = new HazelcastClientProxy(client);
        CLIENTS.put(client.id, proxy);
        return proxy;
    }

    public Config getConfig() {
        throw new UnsupportedOperationException("Client cannot access cluster config!");
    }

    @Override
    public String getName() {
        return name;
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
        return getDistributedObject(CollectionService.SERVICE_NAME,
                new CollectionProxyId(ObjectSetProxy.COLLECTION_SET_NAME, name, CollectionProxyType.SET));
    }

    @Override
    public <E> IList<E> getList(String name) {
        return getDistributedObject(CollectionService.SERVICE_NAME,
                new CollectionProxyId(ObjectListProxy.COLLECTION_LIST_NAME, name, CollectionProxyType.LIST));
    }

    @Override
    public <K, V> IMap<K, V> getMap(String name) {
        return getDistributedObject(MapService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getDistributedObject(CollectionService.SERVICE_NAME,
                new CollectionProxyId(name, null, CollectionProxyType.MULTI_MAP));
    }

    @Override
    public ILock getLock(Object key) {
        return getDistributedObject(LockServiceImpl.SERVICE_NAME, key);
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
    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
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
        Collection<DistributedObject> objects = new LinkedList<DistributedObject>();
        for (ClientProxy clientProxy : proxyManager.getProxies()) {
            objects.add(clientProxy);
        }
        return objects;
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
    public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
        return (T) proxyManager.getProxy(serviceName, id);
    }

    @Override
    public void registerSerializer(final Class type, final TypeSerializer serializer) {
        serializationService.register(type, serializer);
    }

    @Override
    public void registerGlobalSerializer(TypeSerializer serializer) {
        serializationService.registerGlobal(serializer);
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

    void shutdown() {
        CLIENTS.remove(id);
        executionService.shutdown();
        partitionService.stop();
        clusterService.stop();
        connectionManager.shutdown();
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
}
