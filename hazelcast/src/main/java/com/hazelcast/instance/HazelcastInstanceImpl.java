/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.*;
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.management.ThreadMonitoringService;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceProxy;
import com.hazelcast.spi.impl.ProxyServiceImpl;
import com.hazelcast.topic.TopicService;
import com.hazelcast.transaction.TransactionImpl;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.*;

/**
 * @mdogan 7/31/12
 */

@SuppressWarnings("unchecked")
public final class HazelcastInstanceImpl implements HazelcastInstance {

    public final Node node;

    final ILogger logger;

    final String name;

    final ProxyFactory proxyFactory;

    final ManagementService managementService;

    final LifecycleServiceImpl lifecycleService;

    final ManagedContext managedContext;

    final ThreadMonitoringService threadMonitoringService;

    final ThreadGroup threadGroup;

    HazelcastInstanceImpl(String name, Config config) throws Exception {
        this.name = name;
        this.threadGroup = new ThreadGroup(name);
        threadMonitoringService = new ThreadMonitoringService(threadGroup);
        lifecycleService = new LifecycleServiceImpl(this);
        registerConfigSerializers(config);
        managedContext = new HazelcastManagedContext(this, config.getManagedContext());
        node = new Node(this, config);
        logger = node.getLogger(getClass().getName());
        lifecycleService.fireLifecycleEvent(STARTING);
        proxyFactory = node.initializer.getProxyFactory();
        node.start();
        if (!node.isActive()) {
            node.connectionManager.shutdown();
            throw new IllegalStateException("Node failed to start!");
        }
        managementService = new ManagementService(this);
        managementService.register();
    }

    public ThreadMonitoringService getThreadMonitoringService() {
        return threadMonitoringService;
    }

    public String getName() {
        return name;
    }

    public <K, V> IMap<K, V> getMap(String name) {
        return (IMap<K, V>) getServiceProxy(MapService.MAP_SERVICE_NAME, name);
    }

    public <E> IQueue<E> getQueue(String name) {
        return (IQueue<E>) getServiceProxy(QueueService.QUEUE_SERVICE_NAME, name);
    }

    public <E> ITopic<E> getTopic(String name) {
        return (ITopic<E>) getServiceProxy(TopicService.NAME, name);
    }

    public <E> ISet<E> getSet(String name) {
        throw new UnsupportedOperationException();
    }

    public <E> IList<E> getList(String name) {
        return (IList<E>) getServiceProxy(CollectionService.COLLECTION_SERVICE_NAME,
                new CollectionProxyId(name, CollectionProxyType.LIST));
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return (MultiMap<K, V>) getServiceProxy(CollectionService.COLLECTION_SERVICE_NAME,
                new CollectionProxyId(name, CollectionProxyType.MULTI_MAP));
    }

    public ILock getLock(Object key) {
        throw new UnsupportedOperationException();
    }

    public ExecutorService getExecutorService(final String name) {
        throw new UnsupportedOperationException();
    }

    public Transaction getTransaction() {
        return new TransactionImpl(this);
    }

    public IdGenerator getIdGenerator(final String name) {
        throw new UnsupportedOperationException();
    }

    public AtomicNumber getAtomicNumber(final String name) {
        return (AtomicNumber)getServiceProxy(AtomicNumberService.NAME, name);
    }

    public ICountDownLatch getCountDownLatch(final String name) {
        throw new UnsupportedOperationException();
    }

    public ISemaphore getSemaphore(final String name) {
        throw new UnsupportedOperationException();
    }

    public Cluster getCluster() {
        return node.clusterService.getClusterProxy();
    }

    public Collection<DistributedObject> getDistributedObjects() {
        return node.nodeEngine.getProxyService().getAllProxies();
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

    public <S extends ServiceProxy> S getServiceProxy(final Class<? extends RemoteService> serviceClass, Object id) {
        checkActive();
        return (S) node.nodeEngine.getProxyService().getProxy(serviceClass, id);
    }

    private void checkActive() {
        if (!node.isActive()) {
            throw new IllegalStateException("Hazelcast instance is not active!");
        }
    }

    public <S extends ServiceProxy> S getServiceProxy(final String serviceName, Object id) {
        checkActive();
        return (S) node.nodeEngine.getProxyService().getProxy(serviceName, id);
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

    private void registerConfigSerializers(Config config) throws Exception {
        final Collection<SerializerConfig> serializerConfigs = config.getSerializerConfigs();
        if (serializerConfigs != null) {
            for (SerializerConfig serializerConfig : serializerConfigs) {
                TypeSerializer serializer = serializerConfig.getImplementation();
                if (serializer == null) {
                    serializer = (TypeSerializer) ClassLoaderUtil.newInstance(serializerConfig.getClassName());
                }
                if (serializerConfig.isGlobal()) {
                    registerFallbackSerializer(serializer);
                } else {
                    Class typeClass = serializerConfig.getTypeClass();
                    if (typeClass == null) {
                        typeClass = ClassLoaderUtil.loadClass(serializerConfig.getTypeClassName());
                    }
                    registerSerializer(serializer, typeClass);
                }
            }
        }
    }

    void shutdown() {
        managementService.unregister();
        node.shutdown(false, true);
        HazelcastInstanceFactory.remove(this);
    }

    public void restartToMerge() {
        lifecycleService.fireLifecycleEvent(MERGING);
        lifecycleService.restart();
        lifecycleService.fireLifecycleEvent(MERGED);
    }

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    public ManagedContext getManagedContext() {
        return managedContext;
    }
}
