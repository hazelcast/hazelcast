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

package com.hazelcast.instance;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.management.ThreadMonitoringService;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.topic.TopicService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;

/**
 * @mdogan 7/31/12
 */

@SuppressWarnings("unchecked")
@PrivateApi
public final class HazelcastInstanceImpl implements HazelcastInstance {

    public final Node node;

    final ILogger logger;

    final String name;

    final ManagementService managementService;

    final LifecycleServiceImpl lifecycleService;

    final ManagedContext managedContext;

    final ThreadMonitoringService threadMonitoringService;

    final ThreadGroup threadGroup;

    final ConcurrentMap<String, Object> userContext;

    HazelcastInstanceImpl(String name, Config config, NodeContext nodeContext) throws Exception {
        this.name = name;
        this.threadGroup = new ThreadGroup(name);
        threadMonitoringService = new ThreadMonitoringService(threadGroup);
        lifecycleService = new LifecycleServiceImpl(this);
        managedContext = new HazelcastManagedContext(this, config.getManagedContext());
        final ConcurrentMap<String, Object> cfgUserContext = config.getUserContext();
        userContext = cfgUserContext != null ? cfgUserContext : new ConcurrentHashMap<String, Object>();
        node = new Node(this, config, nodeContext);
        logger = node.getLogger(getClass().getName());
        lifecycleService.fireLifecycleEvent(STARTING);
        node.start();
        if (!node.isActive()) {
            node.connectionManager.shutdown();
            throw new IllegalStateException("Node failed to start!");
        }
        managementService = new ManagementService(this);
    }

    public ThreadMonitoringService getThreadMonitoringService() {
        return threadMonitoringService;
    }

    public String getName() {
        return name;
    }

    public <K, V> IMap<K, V> getMap(String name) {
        return getDistributedObject(MapService.SERVICE_NAME, name);
    }

    public <E> IQueue<E> getQueue(String name) {
        return getDistributedObject(QueueService.SERVICE_NAME, name);
    }

    public <E> ITopic<E> getTopic(String name) {
        return getDistributedObject(TopicService.SERVICE_NAME, name);
    }

    public <E> ISet<E> getSet(String name) {
        return getDistributedObject(CollectionService.SERVICE_NAME,
                new CollectionProxyId(ObjectSetProxy.COLLECTION_SET_NAME, name, CollectionProxyType.SET));
    }

    public <E> IList<E> getList(String name) {
        return getDistributedObject(CollectionService.SERVICE_NAME,
                new CollectionProxyId(ObjectListProxy.COLLECTION_LIST_NAME, name, CollectionProxyType.LIST));
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getDistributedObject(CollectionService.SERVICE_NAME,
                new CollectionProxyId(name, null, CollectionProxyType.MULTI_MAP));
    }

    public ILock getLock(Object key) {
        return getDistributedObject(LockService.SERVICE_NAME, node.getSerializationService().toData(key));
    }

    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
        return executeTransaction(TransactionOptions.getDefault(), task);
    }

    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        return node.nodeEngine.getTransactionManagerService().executeTransaction(options, task);
    }

    public TransactionContext newTransactionContext() {
        return newTransactionContext(TransactionOptions.getDefault());
    }

    public TransactionContext newTransactionContext(TransactionOptions options) {
        return node.nodeEngine.getTransactionManagerService().newTransactionContext(options);
    }

    public IExecutorService getExecutorService(final String name) {
        return getDistributedObject(DistributedExecutorService.SERVICE_NAME, name);
    }

    public IdGenerator getIdGenerator(final String name) {
        return getDistributedObject(IdGeneratorService.SERVICE_NAME, name);
    }

    public IAtomicLong getAtomicLong(final String name) {
        return getDistributedObject(AtomicLongService.SERVICE_NAME, name);
    }

    public ICountDownLatch getCountDownLatch(final String name) {
        return getDistributedObject(CountDownLatchService.SERVICE_NAME, name);
    }

    public ISemaphore getSemaphore(final String name) {
        return getDistributedObject(SemaphoreService.SERVICE_NAME, name);
    }

    public Cluster getCluster() {
        return node.clusterService.getClusterProxy();
    }

    public Collection<DistributedObject> getDistributedObjects(String serviceName) {
        return node.nodeEngine.getProxyService().getDistributedObjects(serviceName);
    }

    public Collection<DistributedObject> getDistributedObjects() {
        return node.nodeEngine.getProxyService().getAllDistributedObjects();
    }

    public Config getConfig() {
        return node.getConfig();
    }

    public ConcurrentMap<String, Object> getUserContext() {
        return userContext;
    }

    public PartitionService getPartitionService() {
        return node.partitionService.getPartitionServiceProxy();
    }

    public ClientService getClientService() {
        return node.clientEngine.getClientService();
    }

    public LoggingService getLoggingService() {
        return node.loggingService;
    }

    public LifecycleServiceImpl getLifecycleService() {
        return lifecycleService;
    }

    public <S extends DistributedObject> S getDistributedObject(final String serviceName, Object id) {
        return (S) node.nodeEngine.getProxyService().getDistributedObject(serviceName, id);
    }

    public void registerSerializer(final Class type, final TypeSerializer serializer) {
        node.getSerializationService().register(type, serializer);
    }

    public void registerGlobalSerializer(final TypeSerializer serializer) {
        node.getSerializationService().registerGlobal(serializer);
    }

    public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        final ProxyService proxyService = node.nodeEngine.getProxyService();
        return proxyService.addProxyListener(distributedObjectListener);
    }

    public boolean removeDistributedObjectListener(String registrationId) {
        final ProxyService proxyService = node.nodeEngine.getProxyService();
        return proxyService.removeProxyListener(registrationId);
    }

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("HazelcastInstance");
        sb.append("{name='").append(name).append('\'');
        sb.append(", node=").append(node.getThisAddress());
        sb.append('}');
        return sb.toString();
    }
}
