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

import com.hazelcast.atomicNumber.proxy.AtomicNumberService;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.*;
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.management.ThreadMonitoringService;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.SerializerRegistry;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceProxy;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.*;

/**
 * @mdogan 7/31/12
 */

public final class HazelcastInstanceImpl implements HazelcastInstance {

    public final Node node;

    final ILogger logger;

    final List<InstanceListener> instanceListeners = new CopyOnWriteArrayList<InstanceListener>();

    final String name;

    final ProxyFactory proxyFactory;

    final ManagementService managementService;

    final LifecycleServiceImpl lifecycleService;

    final ManagedContext managedContext;

    final SerializerRegistry serializerRegistry = new SerializerRegistry();

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
//        return getOrCreateInstance(Prefix.MAP + name);
        return (IMap<K, V>) getServiceProxy(MapService.class, name);
    }

    public <E> IQueue<E> getQueue(String name) {
//        return getOrCreateInstance(Prefix.QUEUE + name);
        return (IQueue<E>) getServiceProxy(QueueService.class, name);
    }

    public <E> ITopic<E> getTopic(String name) {
        return getOrCreateInstance(Prefix.TOPIC + name);
    }

    public <E> ISet<E> getSet(String name) {
        return getOrCreateInstance(Prefix.SET + name);
    }

    public <E> IList<E> getList(String name) {
        return getOrCreateInstance(Prefix.AS_LIST + name);
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getOrCreateInstance(Prefix.MULTIMAP + name);
    }

    public ILock getLock(Object key) {
//        return getOrCreateInstance(new ProxyKey("lock", key));
        return null;
    }

    public ExecutorService getExecutorService(final String name) {
        return null;
    }

    public Transaction getTransaction() {
        return null;
    }

    public IdGenerator getIdGenerator(final String name) {
        return null;
    }

    public AtomicNumber getAtomicNumber(final String name) {
        return (AtomicNumber)getServiceProxy(AtomicNumberService.class, name);
    }

    public ICountDownLatch getCountDownLatch(final String name) {
        return null;
    }

    public ISemaphore getSemaphore(final String name) {
        return null;
    }

    public Cluster getCluster() {
        return node.clusterService.getClusterProxy();
    }

    public Collection<Instance> getInstances() {
        final Collection<Instance> instances = new LinkedList<Instance>();
        Collection<RemoteService> services = node.nodeEngine.getServices(RemoteService.class);
        for (RemoteService service : services) {
            final Collection<ServiceProxy> proxies = service.getProxies();
            if (proxies != null && !proxies.isEmpty()) {
                instances.addAll(proxies);
            }
        }
        return instances;
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

    public <S extends ServiceProxy> S getServiceProxy(final Class<? extends RemoteService> serviceClass, String name) {
        Collection services = node.nodeEngine.getServices(serviceClass);
        for (Object service : services) {
            if (serviceClass.isAssignableFrom(service.getClass())) {
                return (S) ((RemoteService) service).getProxy(name);
            }
        }
        throw new IllegalArgumentException();
    }

    public <S extends ServiceProxy> S getServiceProxy(final String serviceName, String name) {
        Object service = node.nodeEngine.getService(serviceName);
        if (service == null) {
            throw new NullPointerException();
        }
        if (service instanceof RemoteService) {
            return (S) ((RemoteService) service).getProxy(name);
        }
        throw new IllegalArgumentException();
    }

    public void registerSerializer(final TypeSerializer serializer, final Class type) {
        serializerRegistry.register(serializer, type);
    }

    public void registerFallbackSerializer(final TypeSerializer serializer) {
        serializerRegistry.registerFallback(serializer);
    }

    public void addInstanceListener(InstanceListener instanceListener) {
        instanceListeners.add(instanceListener);
    }

    public void removeInstanceListener(InstanceListener instanceListener) {
        instanceListeners.remove(instanceListener);
    }

    public <I> I getOrCreateInstance(String name) {
        boolean created = false;
        Instance proxy = null; //proxies.get(name);
        if (proxy == null) {
            created = true;
            if (name.startsWith(Prefix.QUEUE)) {
//                proxy = proxyFactory.createQueueProxy(name);
            } else if (name.startsWith(Prefix.TOPIC)) {
//                proxy = proxyFactory.createTopicProxy(name);
            } else if (name.startsWith(Prefix.MAP)) {
//                proxy = proxyFactory.createMapProxy(name);
            } else if (name.startsWith(Prefix.AS_LIST)) {
//                proxy = proxyFactory.createListProxy(name);
            } else if (name.startsWith(Prefix.MULTIMAP)) {
//                proxy = proxyFactory.createMultiMapProxy(name);
            } else if (name.startsWith(Prefix.SET)) {
//                proxy = proxyFactory.createSetProxy(name);
            } else if (name.startsWith(Prefix.ATOMIC_NUMBER)) {
//                proxy = proxyFactory.createAtomicNumberProxy(name);
            } else if (name.startsWith(Prefix.IDGEN)) {
//                proxy = proxyFactory.createIdGeneratorProxy(name);
            } else if (name.startsWith(Prefix.SEMAPHORE)) {
//                proxy = proxyFactory.createSemaphoreProxy(name);
            } else if (name.startsWith(Prefix.COUNT_DOWN_LATCH)) {
//                proxy = proxyFactory.createCountDownLatchProxy(name);
            } else if (name.equals("lock")) {
//                proxy = proxyFactory.createLockProxy(proxyKey.key);
            }
//            final Instance anotherProxy = proxies.putIfAbsent(name, proxy);
//            if (anotherProxy != null) {
//                created = false;
//                proxy = anotherProxy;
//            }
        }
        if (created) {
            logger.log(Level.FINEST, "Instance created " + name);
            fireInstanceCreateEvent(proxy);
        }
        return (I) proxy;
    }

    public void destroyInstance(final String name) {
        Instance proxy = null; //proxies.remove(name);
        if (proxy != null) {
            logger.log(Level.FINEST, "Instance destroyed " + name);
            destroyInstanceClusterWide(proxy);
            fireInstanceDestroyEvent(proxy);
        }
    }

    private void destroyInstanceClusterWide(final Instance instance) {
    }

    private void fireInstanceCreateEvent(Instance instance) {
        if (instanceListeners.size() > 0) {
            final InstanceEvent instanceEvent = new InstanceEvent(InstanceEvent.InstanceEventType.CREATED, instance);
            for (final InstanceListener instanceListener : instanceListeners) {
                node.nodeEngine.getEventService().executeEvent(new Runnable() {
                    public void run() {
                        instanceListener.instanceCreated(instanceEvent);
                    }
                });
            }
        }
    }

    private void fireInstanceDestroyEvent(Instance instance) {
        if (instanceListeners.size() > 0) {
            final InstanceEvent instanceEvent = new InstanceEvent(InstanceEvent.InstanceEventType.DESTROYED, instance);
            for (final InstanceListener instanceListener : instanceListeners) {
                node.nodeEngine.getEventService().executeEvent(new Runnable() {
                    public void run() {
                        instanceListener.instanceDestroyed(instanceEvent);
                    }
                });
            }
        }
    }

    private void registerConfigSerializers(Config config) throws Exception {
        final Collection<SerializerConfig> serializerConfigs = config.getSerializerConfigs();
        if (serializerConfigs != null) {
            for (SerializerConfig serializerConfig : serializerConfigs) {
                TypeSerializer factory = serializerConfig.getImplementation();
                if (factory == null) {
                    factory = (TypeSerializer) ClassLoaderUtil.newInstance(serializerConfig.getClassName());
                }
                if (serializerConfig.isGlobal()) {
                    serializerRegistry.registerFallback(factory);
                } else {
                    Class typeClass = serializerConfig.getTypeClass();
                    if (typeClass == null) {
                        typeClass = ClassLoaderUtil.loadClass(serializerConfig.getTypeClassName());
                    }
                    serializerRegistry.register(factory, typeClass);
                }
            }
        }
    }

    void shutdown() {
        managementService.unregister();
        node.shutdown(false, true);
        serializerRegistry.destroy();
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

    public SerializerRegistry getSerializerRegistry() {
        return serializerRegistry;
    }

    public ManagedContext getManagedContext() {
        return managedContext;
    }
}
