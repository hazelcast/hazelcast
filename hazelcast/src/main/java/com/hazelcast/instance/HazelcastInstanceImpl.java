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

import com.hazelcast.atomicnumber.AtomicNumberService;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.countdownlatch.CountDownLatchService;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.idgen.IdGeneratorProxy;
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.lock.ObjectLockProxy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.management.ThreadMonitoringService;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.queue.QueueService;
import com.hazelcast.semaphore.SemaphoreService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.ProxyServiceImpl;
import com.hazelcast.topic.TopicService;

import java.util.Collection;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.*;

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

    final NodeEngine nodeEngine;

    HazelcastInstanceImpl(String name, Config config, NodeContext nodeContext) throws Exception {
        this.name = name;
        this.threadGroup = new ThreadGroup(name);
        threadMonitoringService = new ThreadMonitoringService(threadGroup);
        lifecycleService = new LifecycleServiceImpl(this);
        managedContext = new HazelcastManagedContext(this, config.getManagedContext());
        node = new Node(this, config, nodeContext);
        nodeEngine = node.nodeEngine;
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
        final IMap<Object, Object> map = getMap(ObjectLockProxy.LOCK_MAP_NAME);
        return new ObjectLockProxy(key, map);
    }

    public IExecutorService getExecutorService(final String name) {
        return getDistributedObject(DistributedExecutorService.SERVICE_NAME, name);
    }

    public Transaction getTransaction() {
        return ThreadContext.createOrGetTransaction(this);
    }

    public IdGenerator getIdGenerator(final String name) {
        return new IdGeneratorProxy(this, name);
    }

    public AtomicNumber getAtomicNumber(final String name) {
        return getDistributedObject(AtomicNumberService.SERVICE_NAME, name);
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

    public Collection<DistributedObject> getDistributedObjects() {
        return node.nodeEngine.getProxyService().getAllDistributedObjects();
    }

    public Config getConfig() {
        return node.getConfig();
    }

    public PartitionService getPartitionService() {
        return node.partitionService.getPartitionServiceProxy();
    }

    public ClientService getClientService() {
        return null;
    }

    public LoggingService getLoggingService() {
        return node.loggingService;
    }

    public LifecycleService getLifecycleService() {
        return lifecycleService;
    }

    public <S extends DistributedObject> S getDistributedObject(final Class<? extends RemoteService> serviceClass, Object id) {
        checkActive();
        return (S) node.nodeEngine.getProxyService().getDistributedObject(serviceClass, id);
    }

    private void checkActive() {
        if (!node.isActive()) {
            throw new HazelcastInstanceNotActiveException();
        }
    }

    public <S extends DistributedObject> S getDistributedObject(final String serviceName, Object id) {
        checkActive();
        return (S) node.nodeEngine.getProxyService().getDistributedObject(serviceName, id);
    }

    public void registerSerializer(final TypeSerializer serializer, final Class type) {
        node.serializationService.register(serializer, type);
    }

    public void registerFallbackSerializer(final TypeSerializer serializer) {
        node.serializationService.registerFallback(serializer);
    }

    public void addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        final ProxyServiceImpl proxyService = (ProxyServiceImpl) node.nodeEngine.getProxyService();
        proxyService.addProxyListener(distributedObjectListener);
    }

    public void removeDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        final ProxyServiceImpl proxyService = (ProxyServiceImpl) node.nodeEngine.getProxyService();
        proxyService.removeProxyListener(distributedObjectListener);
    }

    public void restartToMerge() {
        lifecycleService.fireLifecycleEvent(MERGING);
        lifecycleService.restart();
        lifecycleService.fireLifecycleEvent(MERGED);
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
