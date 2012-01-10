/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.cluster.ClusterImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.impl.CMap.InitializationState;
import com.hazelcast.impl.executor.ParallelExecutor;
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;
import com.hazelcast.nio.Serializer;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.util.ResponseQueueFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;

@SuppressWarnings("SynchronizationOnStaticField")
public class FactoryImpl implements HazelcastInstance {

    final static ConcurrentMap<String, FactoryImpl> factories = new ConcurrentHashMap<String, FactoryImpl>(5);

    private static final AtomicInteger factoryIdGen = new AtomicInteger();

    private static final Object INIT_LOCK = new Object();

    final ConcurrentMap<String, HazelcastInstanceAwareInstance> proxiesByName = new ConcurrentHashMap<String, HazelcastInstanceAwareInstance>(1000);

    final ConcurrentMap<ProxyKey, HazelcastInstanceAwareInstance> proxies = new ConcurrentHashMap<ProxyKey, HazelcastInstanceAwareInstance>(1000);

    final MProxy locksMapProxy;

    final MProxy globalProxies;

    final ConcurrentMap<String, ExecutorService> executorServiceProxies = new ConcurrentHashMap<String, ExecutorService>(2);

    final CopyOnWriteArrayList<InstanceListener> lsInstanceListeners = new CopyOnWriteArrayList<InstanceListener>();

    final String name;

    final ProxyFactory proxyFactory;

    final HazelcastInstanceProxy hazelcastInstanceProxy;

    final ManagementService managementService;

    final ILogger logger;

    final LifecycleServiceImpl lifecycleService;

    public final Node node;

    volatile boolean restarted = false;

    public static Set<HazelcastInstance> getAllHazelcastInstanceProxies() {
        final Collection<FactoryImpl> factoryColl = factories.values();
        final Set<HazelcastInstance> instanceSet = new HashSet<HazelcastInstance>(factoryColl.size());
        for (FactoryImpl factoryImpl : factoryColl) {
            if (factoryImpl.getLifecycleService().isRunning()) {
                instanceSet.add(factoryImpl.hazelcastInstanceProxy);
            }
        }
        return instanceSet;
    }

    public static HazelcastInstanceProxy getHazelcastInstanceProxy(String instanceName) {
        synchronized (INIT_LOCK) {
            final FactoryImpl factory = factories.get(instanceName);
            return factory != null ? factory.hazelcastInstanceProxy : null;
        }
    }

    public static HazelcastInstanceProxy newHazelcastInstanceProxy(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        String name = config.getInstanceName();
        if (name == null || name.trim().length() == 0) {
            name = "_hzInstance_" + factoryIdGen.incrementAndGet() + "_" + config.getGroupConfig().getName();
            return newHazelcastInstanceProxy(config, name);
        } else {
            synchronized (INIT_LOCK) {
                if (factories.containsKey(name)) {
                    throw new DuplicateInstanceNameException("HazelcastInstance with name '" + name + "' already exists!");
                }
                return newHazelcastInstanceProxy(config, name);
            }
        }
    }

    private static HazelcastInstanceProxy newHazelcastInstanceProxy(Config config, String instanceName) {
        FactoryImpl factory = null;
        try {
            factory = new FactoryImpl(instanceName, config);
            factories.put(instanceName, factory);
            boolean firstMember = (factory.node.getClusterImpl().getMembers().iterator().next().localMember());
            int initialWaitSeconds = factory.node.groupProperties.INITIAL_WAIT_SECONDS.getInteger();
            if (initialWaitSeconds > 0) {
                try {
                    Thread.sleep(initialWaitSeconds * 1000);
                    if (firstMember) {
                        final ConcurrentMapManager concurrentMapManager = factory.node.concurrentMapManager;
                        concurrentMapManager.enqueueAndReturn(new Processable() {
                            public void process() {
                                concurrentMapManager.partitionManager.firstArrangement();
                            }
                        });
                    } else {
                        Thread.sleep(4 * 1000);
                    }
                } catch (InterruptedException ignored) {
                }
            }
            int initialMinClusterSize = factory.node.groupProperties.INITIAL_MIN_CLUSTER_SIZE.getInteger();
            while (factory.node.getClusterImpl().getMembers().size() < initialMinClusterSize) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
            if (initialMinClusterSize > 0) {
                if (firstMember) {
                    final ConcurrentMapManager concurrentMapManager = factory.node.concurrentMapManager;
                    concurrentMapManager.enqueueAndReturn(new Processable() {
                        public void process() {
                            concurrentMapManager.partitionManager.firstArrangement();
                        }
                    });
                } else {
                    Thread.sleep(4 * 1000);
                }
                factory.logger.log(Level.INFO, "HazelcastInstance starting after waiting for cluster size of "
                        + initialMinClusterSize);
            }
            factory.lifecycleService.fireLifecycleEvent(STARTED);
            return factory.hazelcastInstanceProxy;
        } catch (Throwable t) {
            if (factory != null) {
                factory.logger.log(Level.SEVERE, t.getMessage(), t);
            }
            Util.throwUncheckedException(t);
            return null;
        }
    }

    public int hashCode() {
        return name.hashCode();
    }

    public boolean equals(Object o) {
        if (o == null) return false;
        return o == this || name.equals(((FactoryImpl) o).getName());
    }

    public HazelcastInstanceProxy getHazelcastInstanceProxy() {
        return hazelcastInstanceProxy;
    }

    public static class HazelcastInstanceProxy extends HazelcastInstanceAwareObject implements HazelcastInstance {

        public HazelcastInstanceProxy() {
        }

        public HazelcastInstanceProxy(FactoryImpl factory) {
            this.hazelcastInstance = factory;
        }

        public FactoryImpl getFactory() {
            return (FactoryImpl) hazelcastInstance;
        }

        public String getName() {
            return hazelcastInstance.getName();
        }

        public void shutdown() {
            hazelcastInstance.shutdown();
        }

        public void restart() {
            hazelcastInstance.restart();
        }

        public Collection<Instance> getInstances() {
            return hazelcastInstance.getInstances();
        }

        public ExecutorService getExecutorService() {
            return hazelcastInstance.getExecutorService();
        }

        public ExecutorService getExecutorService(String name) {
            return hazelcastInstance.getExecutorService(name);
        }

        public Cluster getCluster() {
            return hazelcastInstance.getCluster();
        }

        public IdGenerator getIdGenerator(String name) {
            return hazelcastInstance.getIdGenerator(name);
        }

        public AtomicNumber getAtomicNumber(String name) {
            return hazelcastInstance.getAtomicNumber(name);
        }

        public Transaction getTransaction() {
            return hazelcastInstance.getTransaction();
        }

        public <K, V> IMap<K, V> getMap(String name) {
            return hazelcastInstance.getMap(name);
        }

        public <E> IQueue<E> getQueue(String name) {
            return hazelcastInstance.getQueue(name);
        }

        public <E> ITopic<E> getTopic(String name) {
            return hazelcastInstance.getTopic(name);
        }

        public <E> ISet<E> getSet(String name) {
            return hazelcastInstance.getSet(name);
        }

        public <E> IList<E> getList(String name) {
            return hazelcastInstance.getList(name);
        }

        public <K, V> MultiMap<K, V> getMultiMap(String name) {
            return hazelcastInstance.getMultiMap(name);
        }

        public ILock getLock(Object key) {
            return hazelcastInstance.getLock(key);
        }

        public ICountDownLatch getCountDownLatch(String name) {
            return hazelcastInstance.getCountDownLatch(name);
        }

        public ISemaphore getSemaphore(String name) {
            return hazelcastInstance.getSemaphore(name);
        }

        public void addInstanceListener(InstanceListener instanceListener) {
            hazelcastInstance.addInstanceListener(instanceListener);
        }

        public void removeInstanceListener(InstanceListener instanceListener) {
            hazelcastInstance.removeInstanceListener(instanceListener);
        }

        public Config getConfig() {
            return hazelcastInstance.getConfig();
        }

        public PartitionService getPartitionService() {
            return hazelcastInstance.getPartitionService();
        }

        public LoggingService getLoggingService() {
            return hazelcastInstance.getLoggingService();
        }

        public LifecycleService getLifecycleService() {
            return hazelcastInstance.getLifecycleService();
        }

        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (this == obj) {
                return true;
            }
            if (obj instanceof HazelcastInstanceProxy) {
                HazelcastInstanceProxy that = (HazelcastInstanceProxy) obj;
                return hazelcastInstance.equals(that.hazelcastInstance);
            }
            return false;
        }

        public int hashCode() {
            return hazelcastInstance.hashCode();
        }
    }

    public String getName() {
        return name;
    }

    public static void shutdownAll() {
        Collection<FactoryImpl> colFactories = factories.values();
        for (FactoryImpl factory : colFactories) {
            factory.shutdown();
        }
        factories.clear();
        shutdownManagementService();
        ThreadContext.shutdownAll();
    }

    public static void kill(HazelcastInstanceProxy hazelcastInstanceProxy) {
        FactoryImpl factory = hazelcastInstanceProxy.getFactory();
        factory.managementService.unregister();
        factory.proxies.clear();
        for (ExecutorService esp : factory.executorServiceProxies.values()) {
            esp.shutdown();
        }
        factory.node.shutdown(true, true);
        factories.remove(factory.getName());
        if (factories.size() == 0) {
            shutdownManagementService();
        }
    }

    public static void shutdown(HazelcastInstanceProxy hazelcastInstanceProxy) {
        FactoryImpl factory = hazelcastInstanceProxy.getFactory();
        factory.managementService.unregister();
        factory.proxies.clear();
        for (ExecutorService esp : factory.executorServiceProxies.values()) {
            esp.shutdown();
        }
        factory.node.shutdown(false, true);
        factories.remove(factory.getName());
        if (factories.size() == 0) {
            shutdownManagementService();
        }
    }

    private static void shutdownManagementService() {
        ManagementService.shutdown();
    }

    public FactoryImpl(String name, Config config) {
        this.name = name;
        node = new Node(this, config);
        proxyFactory = node.initializer.getProxyFactory();
        logger = node.getLogger(FactoryImpl.class.getName());
        globalProxies = proxyFactory.createMapProxy(Prefix.MAP_HAZELCAST + "Proxies");
        lifecycleService = new LifecycleServiceImpl(FactoryImpl.this);
        hazelcastInstanceProxy = new HazelcastInstanceProxy(this);
        locksMapProxy = proxyFactory.createMapProxy(Prefix.MAP_HAZELCAST + "Locks");
        lifecycleService.fireLifecycleEvent(STARTING);
        node.start();
        if (!node.isActive()) {
            throw new IllegalStateException("Node failed to start!");
        }
        globalProxies.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent event) {
                if (node.localMember.equals(event.getMember())) {
                    return;
                }
                final ProxyKey proxyKey = (ProxyKey) event.getKey();
                if (!proxies.containsKey(proxyKey)) {
                    logger.log(Level.FINEST, "Instance created " + proxyKey);
                    node.clusterService.enqueueAndReturn(new Processable() {
                        public void process() {
                            createProxy(proxyKey);
                        }
                    });
                }
            }

            public void entryRemoved(EntryEvent event) {
                if (node.localMember.equals(event.getMember())) {
                    return;
                }
                final ProxyKey proxyKey = (ProxyKey) event.getKey();
                logger.log(Level.FINEST, "Instance removed " + proxyKey);
                node.clusterService.enqueueAndReturn(new Processable() {
                    public void process() {
                        destroyProxy(proxyKey);
                    }
                });
            }

            public void entryUpdated(EntryEvent event) {
                logger.log(Level.FINEST, "Instance updated " + event.getKey());
            }

            public void entryEvicted(EntryEvent event) {
                // should not happen!
                logger.log(Level.FINEST, "Instance evicted " + event.getKey());
            }
        }, false);
        if (node.getClusterImpl().getMembers().size() > 1) {
            Set<ProxyKey> proxyKeys = globalProxies.allKeys();
            for (final ProxyKey proxyKey : proxyKeys) {
                if (!proxies.containsKey(proxyKey)) {
                    node.clusterService.enqueueAndReturn(new Processable() {
                        public void process() {
                            createProxy(proxyKey);
                        }
                    });
                }
            }
        }
        managementService = new ManagementService(this);
        managementService.register();
        initializeListeners(config);
    }

    private void initializeListeners(Config config) {
        for (final ListenerConfig listenerCfg : config.getListenerConfigs()) {
            Object listener = listenerCfg.getImplementation();
            if (listener == null) {
                try {
                    listener = Serializer.newInstance(Serializer.loadClass(listenerCfg.getClassName()));
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
            if (listener instanceof InstanceListener) {
                addInstanceListener((InstanceListener) listener);
            } else if (listener instanceof MembershipListener) {
                getCluster().addMembershipListener((MembershipListener) listener);
            } else if (listener instanceof MigrationListener) {
                getPartitionService().addMigrationListener((MigrationListener) listener);
            } else if (listener != null) {
                final String error = "Unknown listener type: " + listener.getClass();
                Throwable t = new IllegalArgumentException(error);
                logger.log(Level.WARNING, error, t);
            }
        }
    }

    public Set<String> getLongInstanceNames() {
        return proxiesByName.keySet();
    }

    @Override
    public String toString() {
        return "HazelcastInstance {name='" + name + "'}";
    }

    public Config getConfig() {
        return node.getConfig();
    }

    public Collection<Instance> getInstances() {
        return new ArrayList<Instance>(proxies.values());
    }

    public Collection<HazelcastInstanceAwareInstance> getProxies() {
        initialChecks();
        return proxies.values();
    }

    public ExecutorService getExecutorService() {
        initialChecks();
        return getExecutorService("default");
    }

    public ExecutorService getExecutorService(String name) {
        if (name == null) throw new IllegalArgumentException("ExecutorService name cannot be null");
        initialChecks();
        name = Prefix.EXECUTOR_SERVICE + name;
        ExecutorService executorServiceProxy = executorServiceProxies.get(name);
        if (executorServiceProxy == null) {
            executorServiceProxy = proxyFactory.createExecutorServiceProxy(name);
            ExecutorService old = executorServiceProxies.putIfAbsent(name, executorServiceProxy);
            if (old != null) {
                executorServiceProxy = old;
            }
        }
        return executorServiceProxy;
    }

    public ClusterImpl getCluster() {
        initialChecks();
        return node.getClusterImpl();
    }

    public IdGenerator getIdGenerator(String name) {
        return (IdGenerator) getOrCreateProxyByName(Prefix.IDGEN + name);
    }

    public AtomicNumber getAtomicNumber(String name) {
        return (AtomicNumber) getOrCreateProxyByName(Prefix.ATOMIC_NUMBER + name);
    }

    public ICountDownLatch getCountDownLatch(String name) {
        return (ICountDownLatch) getOrCreateProxyByName(Prefix.COUNT_DOWN_LATCH + name);
    }

    public ISemaphore getSemaphore(String name) {
        return (ISemaphore) getOrCreateProxyByName(Prefix.SEMAPHORE + name);
    }

    public Transaction getTransaction() {
        initialChecks();
        ThreadContext threadContext = ThreadContext.get();
        threadContext.setCurrentFactory(this);
        TransactionImpl txn = threadContext.getCallContext().getTransaction();
        if (txn == null) {
            txn = proxyFactory.createTransaction();
            threadContext.getCallContext().setTransaction(txn);
        }
        return txn;
    }

    public PartitionService getPartitionService() {
        return node.concurrentMapManager.partitionServiceImpl;
    }

    public LoggingService getLoggingService() {
        return node.loggingService;
    }

    public LifecycleService getLifecycleService() {
        return lifecycleService;
    }

    public void restart() {
        lifecycleService.restart();
    }

    public void shutdown() {
        lifecycleService.shutdown();
    }

    public <K, V> IMap<K, V> getMap(String name) {
        return (IMap<K, V>) getOrCreateProxyByName(Prefix.MAP + name);
    }

    public <E> IQueue<E> getQueue(String name) {
        return (IQueue) getOrCreateProxyByName(Prefix.QUEUE + name);
    }

    public <E> ITopic<E> getTopic(String name) {
        return (ITopic<E>) getOrCreateProxyByName(Prefix.TOPIC + name);
    }

    public <E> ISet<E> getSet(String name) {
        return (ISet<E>) getOrCreateProxyByName(Prefix.SET + name);
    }

    public <E> IList<E> getList(String name) {
        return (IList<E>) getOrCreateProxyByName(Prefix.AS_LIST + name);
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return (MultiMap<K, V>) getOrCreateProxyByName(Prefix.MULTIMAP + name);
    }

    public ILock getLock(Object key) {
        return (ILock) getOrCreateProxy(new ProxyKey("lock", key));
    }

    public Object getOrCreateProxyByName(final String name) {
        Object proxy = proxiesByName.get(name);
        if (proxy == null) {
            proxy = getOrCreateProxy(new ProxyKey(name, null));
        }
        checkInitialization(proxy);
        return proxy;
    }

    public Object getOrCreateProxy(final ProxyKey proxyKey) {
        initialChecks();
        Object proxy = proxies.get(proxyKey);
        if (proxy == null) {
            proxyFactory.checkProxy(proxyKey);
            proxy = createInstanceClusterWide(proxyKey);
        }
        return proxy;
    }

    private void checkInitialization(Object proxy) {
        if (proxy instanceof MProxy) {
            MProxy mProxy = (MProxy) proxy;
            CMap cmap = node.concurrentMapManager.getMap(mProxy.getLongName());
            if (cmap == null) {
                logger.log(Level.WARNING, "CMap[" + mProxy.getLongName() + "] has not been created yet! Initialization attempt failed!");
                return;
            }
            if (!cmap.isMapForQueue() && cmap.initState.notInitialized()) {
                synchronized (cmap.getInitLock()) {
                    if (cmap.initState.notInitialized()) {
                        final MapStoreConfig mapStoreConfig = cmap.mapConfig.getMapStoreConfig();
                        if (mapStoreConfig != null && mapStoreConfig.isEnabled()) {
                            cmap.initState = InitializationState.INITIALIZING;
                            try {
                                ExecutorService es = getExecutorService();
                                final Set<Member> members = new HashSet<Member>(getCluster().getMembers());
                                members.remove(node.localMember);
                                final MultiTask task = new MultiTask(new InitializeMap(mProxy.getName()), members);
                                es.execute(task);
                                if (cmap.loader != null) {
                                    Set keys = cmap.loader.loadAllKeys();
                                    if (keys != null) {
                                        int count = 0;
                                        PartitionService partitionService = getPartitionService();
                                        Queue<Set> chunks = new LinkedList<Set>();
                                        Set ownedKeys = new HashSet();
                                        for (Object key : keys) {
                                            if (partitionService.getPartition(key).getOwner().localMember()) {
                                                ownedKeys.add(key);
                                                count++;
                                                if (ownedKeys.size() >= node.groupProperties.MAP_LOAD_CHUNK_SIZE.getInteger()) {
                                                    chunks.add(ownedKeys);
                                                    ownedKeys = new HashSet();
                                                }
                                            }
                                        }
                                        chunks.add(ownedKeys);
                                        loadChunks(mProxy, cmap, chunks);
                                        logger.log(Level.INFO, node.address + "[" + mProxy.getName() + "] loaded " + count + " in total.");
                                    }
                                }
                                task.get();
                            } catch (Throwable e) {
                                if (node.isActive()) {
                                    logger.log(Level.SEVERE, e.getMessage(), e);
                                }
                            }
                        }
                    }
                    cmap.initState = InitializationState.INITIALIZED;
                }
            }
        }
    }

    private void loadChunks(final MProxy mProxy, final CMap cmap, final Queue<Set> chunks) throws InterruptedException {
        if (chunks.size() > 0) {
            ParallelExecutor es = node.executorManager.getMapLoaderExecutorService();
            final CountDownLatch latch = new CountDownLatch(chunks.size());
            for (final Set chunk : chunks) {
                es.execute(new Runnable() {
                    public void run() {
                        if (chunk == null) return;
                        try {
                            loadKeys(mProxy, cmap, chunk);
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Initial loading failed.", e);
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
            latch.await();
        }
    }

    private void loadKeys(MProxy mProxy, CMap cmap, Set keys) {
        if (keys.size() > 0) {
            Map map = cmap.loader.loadAll(keys);
            if (map != null && map.size() > 0) {
                Set<Map.Entry> entries = map.entrySet();
                for (Map.Entry entry : entries) {
                    mProxy.putTransient(entry.getKey(), entry.getValue(), 0, null);
                }
            }
        }
    }

    public static class InitializeMap implements Callable<Boolean>, DataSerializable, HazelcastInstanceAware {
        String name;
        private transient FactoryImpl factory = null;

        public InitializeMap(String name) {
            this.name = name;
        }

        public InitializeMap() {
        }

        public Boolean call() throws Exception {
            factory.getMap(name).getName();
            return Boolean.TRUE;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.factory = (FactoryImpl) hazelcastInstance;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(name);
        }

        public void readData(DataInput in) throws IOException {
            name = in.readUTF();
        }
    }

    public void initialChecks() {
        while (node.isActive() && lifecycleService.paused.get()) {
            try {
                //noinspection BusyWait
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
        }
        if (!node.isActive()) throw new IllegalStateException("Hazelcast Instance is not active!");
    }

    public void destroyProxy(final ProxyKey proxyKey) {
        proxiesByName.remove(proxyKey.name);
        Instance proxy = proxies.remove(proxyKey);
        if (proxy != null) {
            String name = proxyKey.name;
            if (name.startsWith(Prefix.QUEUE)) {
                node.blockingQueueManager.destroy(name);
            } else if (name.startsWith(Prefix.MAP)) {
                node.concurrentMapManager.destroy(name);
            } else if (name.startsWith(Prefix.MAP_BASED)) {
                node.concurrentMapManager.destroy(name);
            } else if (name.startsWith(Prefix.TOPIC)) {
                node.topicManager.destroy(name);
            }
            fireInstanceDestroyEvent(proxy);
        }
    }

    // should only be called from service thread!!
    public Object createProxy(ProxyKey proxyKey) {
        node.clusterManager.checkServiceThread();
        boolean created = false;
        HazelcastInstanceAwareInstance proxy = proxies.get(proxyKey);
        if (proxy == null) {
            created = true;
            String name = proxyKey.name;
            if (name.startsWith(Prefix.QUEUE)) {
                proxy = proxyFactory.createQueueProxy(name);
                node.blockingQueueManager.getOrCreateBQ(name);
            } else if (name.startsWith(Prefix.TOPIC)) {
                proxy = proxyFactory.createTopicProxy(name);
                node.topicManager.getTopicInstance(name);
            } else if (name.startsWith(Prefix.MAP)) {
                proxy = proxyFactory.createMapProxy(name);
                node.concurrentMapManager.getOrCreateMap(name);
            } else if (name.startsWith(Prefix.AS_LIST)) {
                proxy = proxyFactory.createListProxy(name);
            } else if (name.startsWith(Prefix.MULTIMAP)) {
                proxy = proxyFactory.createMultiMapProxy(name);
                node.concurrentMapManager.getOrCreateMap(name);
            } else if (name.startsWith(Prefix.SET)) {
                proxy = proxyFactory.createSetProxy(name);
            } else if (name.startsWith(Prefix.ATOMIC_NUMBER)) {
                proxy = proxyFactory.createAtomicNumberProxy(name);
            } else if (name.startsWith(Prefix.IDGEN)) {
                proxy = proxyFactory.createIdGeneratorProxy(name);
            } else if (name.startsWith(Prefix.SEMAPHORE)) {
                proxy = proxyFactory.createSemaphoreProxy(name);
            } else if (name.startsWith(Prefix.COUNT_DOWN_LATCH)) {
                proxy = proxyFactory.createCountDownLatchProxy(name);
            } else if (name.equals("lock")) {
                proxy = proxyFactory.createLockProxy(proxyKey.key);
            }
            final HazelcastInstanceAwareInstance anotherProxy = proxies.putIfAbsent(proxyKey, proxy);
            if (anotherProxy != null) {
                created = false;
                proxy = anotherProxy;
            }
            if (proxyKey.key == null) {
                proxiesByName.put(proxyKey.name, proxy);
            }
        }
        if (created) {
            fireInstanceCreateEvent(proxy);
        }
        return proxy;
    }

    public void addInstanceListener(InstanceListener instanceListener) {
        lsInstanceListeners.add(instanceListener);
    }

    public void removeInstanceListener(InstanceListener instanceListener) {
        lsInstanceListeners.remove(instanceListener);
    }

    void fireInstanceCreateEvent(Instance instance) {
        if (lsInstanceListeners.size() > 0) {
            final InstanceEvent instanceEvent = new InstanceEvent(InstanceEvent.InstanceEventType.CREATED, instance);
            for (final InstanceListener instanceListener : lsInstanceListeners) {
                node.executorManager.executeLocally(new Runnable() {
                    public void run() {
                        instanceListener.instanceCreated(instanceEvent);
                    }
                });
            }
        }
    }

    void fireInstanceDestroyEvent(Instance instance) {
        if (lsInstanceListeners.size() > 0) {
            final InstanceEvent instanceEvent = new InstanceEvent(InstanceEvent.InstanceEventType.DESTROYED, instance);
            for (final InstanceListener instanceListener : lsInstanceListeners) {
                node.executorManager.executeLocally(new Runnable() {
                    public void run() {
                        instanceListener.instanceDestroyed(instanceEvent);
                    }
                });
            }
        }
    }

    Object createInstanceClusterWide(final ProxyKey proxyKey) {
        final BlockingQueue<Object> result = ResponseQueueFactory.newResponseQueue();
        node.clusterService.enqueueAndWait(new Processable() {
            public void process() {
                try {
                    result.put(createProxy(proxyKey));
                } catch (InterruptedException ignored) {
                }
            }
        }, 10);
        Object proxy = null;
        try {
            proxy = result.take();
        } catch (InterruptedException e) {
        }
        globalProxies.put(proxyKey, Constants.IO.EMPTY_DATA);
        return proxy;
    }

    void destroyInstanceClusterWide(String name, Object key) {
        final ProxyKey proxyKey = new ProxyKey(name, key);
        if (proxies.containsKey(proxyKey)) {
            if (name.equals("lock")) {
                locksMapProxy.remove(key);
            }
            globalProxies.remove(proxyKey);
            node.clusterService.enqueueAndWait(new Processable() {
                public void process() {
                    try {
                        destroyProxy(proxyKey);
                    } catch (Exception e) {
                    }
                }
            }, 5);
        } else {
            logger.log(Level.WARNING, "Destroying unknown instance name: " + name);
        }
    }

    public static class ProxyKey extends SerializationHelper implements DataSerializable {
        String name;
        Object key;

        public ProxyKey() {
        }

        public ProxyKey(String name, Object key) {
            this.name = name;
            this.key = key;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(name);
            boolean keyNull = (key == null);
            out.writeBoolean(keyNull);
            if (!keyNull) {
                writeObject(out, key);
            }
        }

        public void readData(DataInput in) throws IOException {
            name = in.readUTF();
            boolean keyNull = in.readBoolean();
            if (!keyNull) {
                key = readObject(in);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProxyKey pk = (ProxyKey) o;
            return (name != null ? name.equals(pk.name) : pk.name == null)
                    && (key != null ? key.equals(pk.key) : pk.key == null);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ProxyKey {name='" + name + "', key=" + key + '}';
        }

        public String getName() {
            return name;
        }

        public Object getKey() {
            return key;
        }
    }
}
