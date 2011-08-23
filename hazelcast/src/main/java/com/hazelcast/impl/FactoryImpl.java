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
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.impl.ConcurrentMapManager.*;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;
import com.hazelcast.impl.base.RuntimeInterruptedException;
import com.hazelcast.impl.concurrentmap.AddMapIndex;
import com.hazelcast.impl.management.ManagementCenterService;
import com.hazelcast.impl.monitor.*;
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.monitor.*;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.util.ResponseQueueFactory;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;
import static com.hazelcast.impl.Util.toMillis;
import static com.hazelcast.nio.IOUtil.toData;

public class FactoryImpl implements HazelcastInstance {

    final static ConcurrentMap<String, FactoryImpl> factories = new ConcurrentHashMap<String, FactoryImpl>(5);

    private static AtomicInteger factoryIdGen = new AtomicInteger();

    final ConcurrentMap<String, HazelcastInstanceAwareInstance> proxiesByName = new ConcurrentHashMap<String, HazelcastInstanceAwareInstance>(1000);

    final ConcurrentMap<ProxyKey, HazelcastInstanceAwareInstance> proxies = new ConcurrentHashMap<ProxyKey, HazelcastInstanceAwareInstance>(1000);

    final MProxy locksMapProxy;

    final MProxy idGeneratorMapProxy;

    final MProxy globalProxies;

    final ConcurrentMap<String, ExecutorServiceProxy> executorServiceProxies = new ConcurrentHashMap<String, ExecutorServiceProxy>(2);

    final CopyOnWriteArrayList<InstanceListener> lsInstanceListeners = new CopyOnWriteArrayList<InstanceListener>();

    final String name;

    final TransactionFactory transactionFactory;

    final HazelcastInstanceProxy hazelcastInstanceProxy;

    final ManagementService managementService;

    final ILogger logger;

    final LifecycleServiceImpl lifecycleService;

    final ManagementCenterService managementCenterService;

    public final Node node;

    volatile boolean restarted = false;

    public static HazelcastInstanceProxy newHazelcastInstanceProxy(Config config) {
        FactoryImpl factory = null;
        try {
            if (config == null) {
                config = new XmlConfigBuilder().build();
            }
            String name = "_hzInstance_" + factoryIdGen.incrementAndGet() + "_" + config.getGroupConfig().getName();
            factory = new FactoryImpl(name, config);
            factories.put(name, factory);
            boolean firstMember = (factory.node.getClusterImpl().getMembers().iterator().next().localMember());
            int initialWaitSeconds = factory.node.groupProperties.INITIAL_WAIT_SECONDS.getInteger();
            if (initialWaitSeconds > 0) {
                try {
                    Thread.sleep(initialWaitSeconds * 1000);
                    if (firstMember) {
                        final ConcurrentMapManager concurrentMapManager = factory.node.concurrentMapManager;
                        concurrentMapManager.enqueueAndReturn(new Processable() {
                            public void process() {
                                concurrentMapManager.partitionManager.quickBlockRearrangement();
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
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
            if (initialMinClusterSize > 0) {
                if (firstMember) {
                    final ConcurrentMapManager concurrentMapManager = factory.node.concurrentMapManager;
                    concurrentMapManager.enqueueAndReturn(new Processable() {
                        public void process() {
                            concurrentMapManager.partitionManager.quickBlockRearrangement();
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
            throw new RuntimeException(t);
        }
    }

    public int hashCode() {
        return name.hashCode();
    }

    public boolean equals(Object o) {
        if (o == null) return false;
        return o == this || name.equals(((FactoryImpl) o).name);
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

    public static void shutdown(HazelcastInstanceProxy hazelcastInstanceProxy) {
        FactoryImpl factory = hazelcastInstanceProxy.getFactory();
        factory.managementService.unregister();
        factory.proxies.clear();
        if (factory.managementCenterService != null) {
            factory.managementCenterService.shutdown();
        }
        for (ExecutorServiceProxy esp : factory.executorServiceProxies.values()) {
            esp.shutdown();
        }
        factory.node.shutdown();
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
        globalProxies = new MProxyImpl(Prefix.MAP_HAZELCAST + "Proxies", this);
        logger = node.getLogger(FactoryImpl.class.getName());
        lifecycleService = new LifecycleServiceImpl(FactoryImpl.this);
        transactionFactory = new TransactionFactory(this);
        hazelcastInstanceProxy = new HazelcastInstanceProxy(this);
        locksMapProxy = new MProxyImpl(Prefix.MAP_HAZELCAST + "Locks", this);
        idGeneratorMapProxy = new MProxyImpl(Prefix.MAP_HAZELCAST + "IdGenerator", this);
        lifecycleService.fireLifecycleEvent(STARTING);
        node.start();
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
        ManagementCenterService managementCenterServiceTmp = null;
        if (node.groupProperties.MANCENTER_ENABLED.getBoolean()) {
            try {
                managementCenterServiceTmp = new ManagementCenterService(FactoryImpl.this);
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        managementCenterService = managementCenterServiceTmp;
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
        ExecutorServiceProxy executorServiceProxy = executorServiceProxies.get(name);
        if (executorServiceProxy == null) {
            executorServiceProxy = new ExecutorServiceProxy(node, name);
            ExecutorServiceProxy old = executorServiceProxies.putIfAbsent(name, executorServiceProxy);
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
            txn = transactionFactory.newTransaction();
            threadContext.getCallContext().setTransaction(txn);
        }
        return txn;
    }

    public PartitionService getPartitionService() {
        return node.concurrentMapManager.partitionManager.partitionServiceImpl;
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
            proxy = createInstanceClusterWide(proxyKey);
        }
        return proxy;
    }

    private void checkInitialization(Object proxy) {
        if (proxy instanceof MProxy) {
            MProxy mProxy = (MProxy) proxy;
            CMap cmap = node.concurrentMapManager.getMap(mProxy.getLongName());
            if (!cmap.isMapForQueue() && !cmap.initialized) {
                synchronized (cmap.getInitLock()) {
                    if (!cmap.initialized) {
                        if (cmap.loader != null) {
                            try {
                                if (getAtomicNumber(name).compareAndSet(0, 1)) {
                                    ExecutorService es = getExecutorService();
                                    MultiTask task = new MultiTask(new InitializeMap(mProxy.getName()), getCluster().getMembers());
                                    es.execute(task);
                                }
                                Set keys = cmap.loader.loadAllKeys();
                                if (keys != null) {
                                    int count = 0;
                                    PartitionService partitionService = getPartitionService();
                                    Set ownedKeys = new HashSet();
                                    for (Object key : keys) {
                                        if (partitionService.getPartition(key).getOwner().localMember()) {
                                            ownedKeys.add(key);
                                            count++;
                                            if (ownedKeys.size() >= node.groupProperties.MAP_LOAD_CHUNK_SIZE.getInteger()) {
                                                loadKeys(mProxy, cmap, ownedKeys);
                                                ownedKeys.clear();
                                            }
                                        }
                                    }
                                    loadKeys(mProxy, cmap, ownedKeys);
                                    logger.log(Level.INFO, node.address + "[" + mProxy.getName() + "] loaded " + count);
                                }
                            } catch (Throwable e) {
                                if (node.isActive()) {
                                    logger.log(Level.SEVERE, e.getMessage(), e);
                                }
                            }
                        }
                    }
                    cmap.initialized = true;
                }
            }
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
        private transient HazelcastInstance hazelcast = null;

        public InitializeMap(String name) {
            this.name = name;
        }

        public InitializeMap() {
        }

        public Boolean call() throws Exception {
            hazelcast.getMap(name).getName();
            return Boolean.TRUE;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcast = hazelcastInstance;
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
                proxy = new QProxyImpl(name, this);
            } else if (name.startsWith(Prefix.TOPIC)) {
                proxy = new TopicProxyImpl(name, this);
            } else if (name.startsWith(Prefix.MAP)) {
                proxy = new MProxyImpl(name, this);
                node.concurrentMapManager.getOrCreateMap(name);
            } else if (name.startsWith(Prefix.AS_LIST)) {
                proxy = new ListProxyImpl(name, this);
            } else if (name.startsWith(Prefix.MULTIMAP)) {
                proxy = new MultiMapProxyImpl(name, this);
            } else if (name.startsWith(Prefix.SET)) {
                proxy = new SetProxyImpl(name, this);
            } else if (name.startsWith(Prefix.ATOMIC_NUMBER)) {
                proxy = new AtomicNumberProxyImpl(name, this);
            } else if (name.startsWith(Prefix.IDGEN)) {
                proxy = new IdGeneratorProxy(name, this);
            } else if (name.startsWith(Prefix.SEMAPHORE)) {
                proxy = new SemaphoreProxyImpl(name, this);
            } else if (name.startsWith(Prefix.COUNT_DOWN_LATCH)) {
                proxy = new CountDownLatchProxyImpl(name, this);
            } else if (name.equals("lock")) {
                proxy = new LockProxyImpl(this, proxyKey.key);
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

    public static class LockProxyImpl extends SerializationHelper implements HazelcastInstanceAwareInstance, LockProxy, DataSerializable {

        private Object key = null;
        private transient LockProxy base = null;
        private transient FactoryImpl factory = null;

        public LockProxyImpl() {
        }

        public LockProxyImpl(HazelcastInstance hazelcastInstance, Object key) {
            super();
            this.key = key;
            setHazelcastInstance(hazelcastInstance);
            base = new LockProxyBase();
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.factory = (FactoryImpl) hazelcastInstance;
        }

        private void ensure() {
            factory.initialChecks();
            if (base == null) {
                base = (LockProxy) factory.getLock(key);
            }
        }

        @Override
        public String toString() {
            return "ILock [" + key + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LockProxyImpl lockProxy = (LockProxyImpl) o;
            return !(key != null ? !key.equals(lockProxy.key) : lockProxy.key != null);
        }

        @Override
        public int hashCode() {
            return key != null ? key.hashCode() : 0;
        }

        public void writeData(DataOutput out) throws IOException {
            writeObject(out, key);
        }

        public void readData(DataInput in) throws IOException {
            key = readObject(in);
            setHazelcastInstance(ThreadContext.get().getCurrentFactory());
        }

        public void lock() {
            ensure();
            base.lock();
        }

        public void lockInterruptibly() throws InterruptedException {
            ensure();
            base.lockInterruptibly();
        }

        public boolean tryLock() {
            ensure();
            return base.tryLock();
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            ensure();
            return base.tryLock(time, unit);
        }

        public void unlock() {
            ensure();
            base.unlock();
        }

        public Condition newCondition() {
            ensure();
            return base.newCondition();
        }

        public InstanceType getInstanceType() {
            ensure();
            return InstanceType.LOCK;
        }

        public void destroy() {
            ensure();
            base.destroy();
        }

        public Object getLockObject() {
            return key;
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        public LocalLockStats getLocalLockStats() {
            ensure();
            return base.getLocalLockStats();
        }

        public LockOperationsCounter getLockOperationCounter() {
            ensure();
            return base.getLockOperationCounter();
        }

        private class LockProxyBase implements LockProxy {
            private LockOperationsCounter lockOperationsCounter = new LockOperationsCounter();

            public void lock() {
                factory.locksMapProxy.lock(key);
                lockOperationsCounter.incrementLocks();
            }

            public void lockInterruptibly() throws InterruptedException {
                throw new UnsupportedOperationException("lockInterruptibly is not implemented!");
            }

            public Condition newCondition() {
                return null;
            }

            public boolean tryLock() {
                if (factory.locksMapProxy.tryLock(key)) {
                    lockOperationsCounter.incrementLocks();
                    return true;
                }
                lockOperationsCounter.incrementFailedLocks();
                return false;
            }

            public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
                try {
                    if (factory.locksMapProxy.tryLock(key, time, unit)) {
                        lockOperationsCounter.incrementLocks();
                        return true;
                    }
                } catch (RuntimeInterruptedException e) {
                    lockOperationsCounter.incrementFailedLocks();
                    throw new InterruptedException();
                }
                lockOperationsCounter.incrementFailedLocks();
                return false;
            }

            public void unlock() {
                factory.locksMapProxy.unlock(key);
                lockOperationsCounter.incrementUnlocks();
            }

            public void destroy() {
                factory.destroyInstanceClusterWide("lock", key);
            }

            public InstanceType getInstanceType() {
                return InstanceType.LOCK;
            }

            public Object getLockObject() {
                return key;
            }

            public Object getId() {
                return new ProxyKey("lock", key);
            }

            public LocalLockStats getLocalLockStats() {
                LocalLockStatsImpl localLockStats = new LocalLockStatsImpl();
                localLockStats.setOperationStats(lockOperationsCounter.getPublishedStats());
                return localLockStats;
            }

            public LockOperationsCounter getLockOperationCounter() {
                return lockOperationsCounter;
            }
        }
    }

    Object createInstanceClusterWide(final ProxyKey proxyKey) {
        final BlockingQueue<Object> result = ResponseQueueFactory.newResponseQueue();
        node.clusterService.enqueueAndReturn(new Processable() {
            public void process() {
                try {
                    result.put(createProxy(proxyKey));
                } catch (InterruptedException ignored) {
                }
            }
        });
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
            } else if (name.startsWith(Prefix.IDGEN)) {
                idGeneratorMapProxy.remove(name);
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

    public static class TopicProxyImpl extends FactoryAwareNamedProxy implements TopicProxy, DataSerializable {
        private transient TopicProxy base = null;
        private TopicManager topicManager = null;
        private ListenerManager listenerManager = null;

        public TopicProxyImpl() {
        }

        public TopicProxyImpl(String name, FactoryImpl factory) {
            set(name, factory);
            base = new TopicProxyReal();
        }

        private void ensure() {
            factory.initialChecks();
            if (base == null) {
                base = (TopicProxy) factory.getOrCreateProxyByName(name);
            }
        }

        public void set(String name, FactoryImpl factory) {
            setName(name);
            setHazelcastInstance(factory);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            super.setHazelcastInstance(hazelcastInstance);
            topicManager = factory.node.topicManager;
            listenerManager = factory.node.listenerManager;
        }

        public String getLongName() {
            return base.getLongName();
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        @Override
        public String toString() {
            return "Topic [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicProxyImpl that = (TopicProxyImpl) o;
            return !(name != null ? !name.equals(that.name) : that.name != null);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public void publish(Object msg) {
            ensure();
            base.publish(msg);
        }

        public void addMessageListener(MessageListener listener) {
            ensure();
            base.addMessageListener(listener);
        }

        public void removeMessageListener(MessageListener listener) {
            ensure();
            base.removeMessageListener(listener);
        }

        public void destroy() {
            ensure();
            base.destroy();
        }

        public InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public String getName() {
            ensure();
            return base.getName();
        }

        public LocalTopicStats getLocalTopicStats() {
            ensure();
            return base.getLocalTopicStats();
        }

        public TopicOperationsCounter getTopicOperationCounter() {
            return base.getTopicOperationCounter();
        }

        class TopicProxyReal implements TopicProxy {
            TopicOperationsCounter topicOperationsCounter = new TopicOperationsCounter();

            public void publish(Object msg) {
                check(msg);
                topicOperationsCounter.incrementPublishes();
                topicManager.doPublish(name, msg);
            }

            public void addMessageListener(MessageListener listener) {
                listenerManager.addListener(name, listener, null, true,
                        getInstanceType());
            }

            public void removeMessageListener(MessageListener listener) {
                listenerManager.removeListener(name, listener, null);
            }

            public void destroy() {
                factory.destroyInstanceClusterWide(name, null);
            }

            public InstanceType getInstanceType() {
                return InstanceType.TOPIC;
            }

            public String getName() {
                return name.substring(Prefix.TOPIC.length());
            }

            public String getLongName() {
                return name;
            }

            public Object getId() {
                return name;
            }

            public LocalTopicStats getLocalTopicStats() {
                LocalTopicStatsImpl localTopicStats = topicManager.getTopicInstance(name).getTopicStats();
                localTopicStats.setOperationStats(topicOperationsCounter.getPublishedStats());
                return localTopicStats;
            }

            public TopicOperationsCounter getTopicOperationCounter() {
                return topicOperationsCounter;
            }
        }
    }

    public static class AtomicNumberProxyImpl extends FactoryAwareNamedProxy implements AtomicNumberProxy {
        private transient AtomicNumberProxy base = null;
        Data nameAsData = null;

        public AtomicNumberProxyImpl() {
        }

        public AtomicNumberProxyImpl(String name, FactoryImpl factory) {
            setName(name);
            setHazelcastInstance(factory);
            base = new AtomicNumberProxyReal();
        }

        Data getNameAsData() {
            if (nameAsData == null) {
                nameAsData = toData(name);
            }
            return nameAsData;
        }

        private void ensure() {
            factory.initialChecks();
            if (base == null) {
                base = (AtomicNumberProxy) factory.getOrCreateProxyByName(name);
            }
        }

        @Override
        public String toString() {
            return "AtomicLong [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            AtomicNumberProxyImpl that = (AtomicNumberProxyImpl) o;
            return !(name != null ? !name.equals(that.name) : that.name != null);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public void destroy() {
            ensure();
            base.destroy();
        }

        public InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        public String getName() {
            ensure();
            return base.getName();
        }

        public String getLongName() {
            return name;
        }

        public long addAndGet(long delta) {
            ensure();
            return base.addAndGet(delta);
        }

        public boolean compareAndSet(long expect, long update) {
            ensure();
            return base.compareAndSet(expect, update);
        }

        public long decrementAndGet() {
            ensure();
            return base.decrementAndGet();
        }

        public long get() {
            ensure();
            return base.get();
        }

        public long getAndAdd(long delta) {
            ensure();
            return base.getAndAdd(delta);
        }

        public long getAndSet(long newValue) {
            ensure();
            return base.getAndSet(newValue);
        }

        public long incrementAndGet() {
            ensure();
            return base.incrementAndGet();
        }

        public void set(long newValue) {
            ensure();
            base.set(newValue);
        }

        public AtomicNumberOperationsCounter getOperationsCounter() {
            ensure();
            return base.getOperationsCounter();
        }

        public LocalAtomicNumberStats getLocalAtomicNumberStats() {
            ensure();
            return base.getLocalAtomicNumberStats();
        }

        @Deprecated
        public void lazySet(long newValue) {
            set(newValue);
        }

        @Deprecated
        public boolean weakCompareAndSet(long expect, long update) {
            return compareAndSet(expect, update);
        }

        private class AtomicNumberProxyReal implements AtomicNumberProxy {
            AtomicNumberOperationsCounter operationsCounter = new AtomicNumberOperationsCounter();

            public AtomicNumberProxyReal() {
            }

            public String getName() {
                return name.substring(Prefix.ATOMIC_NUMBER.length());
            }

            public String getLongName() {
                return name;
            }

            public Object getId() {
                return name;
            }

            public long addAndGet(long delta) {
                return newMAtomicNumber().addAndGet(getNameAsData(), delta);
            }

            public boolean compareAndSet(long expect, long update) {
                return newMAtomicNumber().compareAndSet(getNameAsData(), expect, update);
            }

            public long decrementAndGet() {
                return addAndGet(-1L);
            }

            public long get() {
                return addAndGet(0L);
            }

            public long getAndAdd(long delta) {
                return newMAtomicNumber().getAndAdd(getNameAsData(), delta);
            }

            public long getAndSet(long newValue) {
                return newMAtomicNumber().getAndSet(getNameAsData(), newValue);
            }

            public long incrementAndGet() {
                return addAndGet(1L);
            }

            public void set(long newValue) {
                getAndSet(newValue);
            }

            public InstanceType getInstanceType() {
                return InstanceType.ATOMIC_NUMBER;
            }

            public void destroy() {
                newMAtomicNumber().destroy(getNameAsData());
                factory.destroyInstanceClusterWide(name, null);
            }

            public AtomicNumberOperationsCounter getOperationsCounter() {
                return operationsCounter;
            }

            public LocalAtomicNumberStats getLocalAtomicNumberStats() {
                LocalAtomicNumberStatsImpl localAtomicStats = new LocalAtomicNumberStatsImpl();
                localAtomicStats.setOperationStats(operationsCounter.getPublishedStats());
                return localAtomicStats;
            }

            @Deprecated
            public void lazySet(long newValue) {
                set(newValue);
            }

            @Deprecated
            public boolean weakCompareAndSet(long expect, long update) {
                return compareAndSet(expect, update);
            }

            MAtomicNumber newMAtomicNumber() {
                MAtomicNumber mAtomicNumber = factory.node.concurrentMapManager.new MAtomicNumber();
                mAtomicNumber.setOperationsCounter(operationsCounter);
                return mAtomicNumber;
            }
        }
    }

    public class CountDownLatchProxyImpl extends FactoryAwareNamedProxy implements CountDownLatchProxy {
        private transient CountDownLatchProxy base = null;
        Data nameAsData = null;

        public CountDownLatchProxyImpl(String name, FactoryImpl factory) {
            set(name, factory);
            base = new CountDownLatchProxyReal();
        }

        public void set(String name, FactoryImpl factory) {
            setName(name);
            setHazelcastInstance(factory);
        }

        public InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public void destroy() {
            ensure();
            base.destroy();
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        public String getName() {
            ensure();
            return base.getName();
        }

        public String getLongName() {
            return name;
        }

        @Override
        public String toString() {
            return "CountDownLatch [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CountDownLatchProxyImpl that = (CountDownLatchProxyImpl) o;
            return !(name != null ? !name.equals(that.name) : that.name != null);
        }

        @Override
        public int hashCode() {
            int result = base != null ? base.hashCode() : 0;
            result = 31 * result + (nameAsData != null ? nameAsData.hashCode() : 0);
            return result;
        }

        public void await() throws InstanceDestroyedException, MemberLeftException, InterruptedException {
            ensure();
            base.await();
        }

        public boolean await(long timeout, TimeUnit unit) throws InstanceDestroyedException, MemberLeftException, InterruptedException {
            ensure();
            return base.await(timeout, unit);
        }

        public void countDown() {
            ensure();
            base.countDown();
        }

        public int getCount() {
            ensure();
            return base.getCount();
        }

        public Member getOwner() {
            ensure();
            return base.getOwner();
        }

        public boolean hasCount() {
            ensure();
            return base.hasCount();
        }

        public boolean setCount(int count) {
            ensure();
            return base.setCount(count);
        }

        public boolean setCount(int count, Address ownerAddress) {
            ensure();
            return base.setCount(count, ownerAddress);
        }

        public LocalCountDownLatchStats getLocalCountDownLatchStats() {
            ensure();
            return base.getLocalCountDownLatchStats();
        }

        public CountDownLatchOperationsCounter getCountDownLatchOperationsCounter() {
            ensure();
            return base.getCountDownLatchOperationsCounter();
        }

        private Data getNameAsData() {
            if (nameAsData == null) {
                nameAsData = toData(name);
            }
            return nameAsData;
        }

        private void ensure() {
            factory.initialChecks();
            if (base == null) {
                base = (CountDownLatchProxy) factory.getOrCreateProxyByName(name);
            }
        }

        private class CountDownLatchProxyReal implements CountDownLatchProxy {
            CountDownLatchOperationsCounter operationsCounter = new CountDownLatchOperationsCounter();

            public CountDownLatchProxyReal() {
            }

            public String getName() {
                return name.substring(Prefix.COUNT_DOWN_LATCH.length());
            }

            public String getLongName() {
                return name;
            }

            public Object getId() {
                return name;
            }

            public void await() throws InstanceDestroyedException, MemberLeftException, InterruptedException {
                await(-1, TimeUnit.MILLISECONDS);
            }

            public boolean await(long timeout, TimeUnit unit) throws InstanceDestroyedException, MemberLeftException, InterruptedException {
                if (Thread.interrupted())
                    throw new InterruptedException();
                return newMCountDownLatch().await(getNameAsData(), timeout, unit);
            }

            public void countDown() {
                newMCountDownLatch().countDown(getNameAsData());
            }

            public void destroy() {
                newMCountDownLatch().destroy(getNameAsData());
                factory.destroyInstanceClusterWide(name, null);
            }

            public int getCount() {
                return newMCountDownLatch().getCount(getNameAsData());
            }

            public Member getOwner() {
                final Address owner = newMCountDownLatch().getOwnerAddress(getNameAsData());
                final Address local = node.baseVariables.thisAddress;
                return owner != null ? new MemberImpl(owner, local.equals(owner)) : null;
            }

            public boolean hasCount() {
                return newMCountDownLatch().getCount(getNameAsData()) > 0;
            }

            public boolean setCount(int count) {
                return setCount(count, node.getThisAddress());
            }

            public boolean setCount(int count, Address ownerAddress) {
                return newMCountDownLatch().setCount(getNameAsData(), count, ownerAddress);
            }

            public InstanceType getInstanceType() {
                return InstanceType.COUNT_DOWN_LATCH;
            }

            public CountDownLatchOperationsCounter getCountDownLatchOperationsCounter() {
                return operationsCounter;
            }

            public LocalCountDownLatchStats getLocalCountDownLatchStats() {
                LocalCountDownLatchStatsImpl localCountDownLatchStats = new LocalCountDownLatchStatsImpl();
                localCountDownLatchStats.setOperationStats(operationsCounter.getPublishedStats());
                return localCountDownLatchStats;
            }

            ConcurrentMapManager.MCountDownLatch newMCountDownLatch() {
                ConcurrentMapManager.MCountDownLatch mcdl = factory.node.concurrentMapManager.new MCountDownLatch();
                mcdl.setOperationsCounter(operationsCounter);
                return mcdl;
            }
        }
    }

    public class SemaphoreProxyImpl extends FactoryAwareNamedProxy implements SemaphoreProxy {
        private transient SemaphoreProxy base = null;
        Data nameAsData = null;

        public SemaphoreProxyImpl(String name, FactoryImpl factory) {
            setName(name);
            setHazelcastInstance(factory);
            base = new SemaphoreProxyReal();
        }

        private void ensure() {
            factory.initialChecks();
            if (base == null) {
                base = (SemaphoreProxy) factory.getOrCreateProxyByName(name);
            }
        }

        public String getLongName() {
            return name;
        }

        public String getName() {
            return name.substring(Prefix.SEMAPHORE.length());
        }

        Data getNameAsData() {
            if (nameAsData == null) {
                nameAsData = toData(getName());
            }
            return nameAsData;
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        @Override
        public String toString() {
            return "Semaphore [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            SemaphoreProxyImpl that = (SemaphoreProxyImpl) o;
            return !(name != null ? !name.equals(that.name) : that.name != null);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public LocalSemaphoreStats getLocalSemaphoreStats() {
            ensure();
            return base.getLocalSemaphoreStats();
        }

        public SemaphoreOperationsCounter getOperationsCounter() {
            ensure();
            return base.getOperationsCounter();
        }

        public void acquire() throws InstanceDestroyedException, InterruptedException {
            ensure();
            base.acquire();
        }

        public void acquire(int permits) throws InstanceDestroyedException, InterruptedException {
            check(permits);
            ensure();
            base.acquire(permits);
        }

        public Future acquireAsync() {
            return doAsyncAcquire(1, false);
        }

        public Future acquireAsync(int permits) {
            check(permits);
            return doAsyncAcquire(permits, false);
        }

        public void acquireAttach() throws InstanceDestroyedException, InterruptedException {
            ensure();
            base.acquireAttach();
        }

        public void acquireAttach(int permits) throws InstanceDestroyedException, InterruptedException {
            check(permits);
            ensure();
            base.acquireAttach(permits);
        }

        public Future acquireAttachAsync() {
            return doAsyncAcquire(1, true);
        }

        public Future acquireAttachAsync(int permits) {
            check(permits);
            return doAsyncAcquire(permits, true);
        }

        public void attach() {
            ensure();
            base.attach();
        }

        public void attach(int permits) {
            check(permits);
            ensure();
            base.attach(permits);
        }

        public int attachedPermits() {
            ensure();
            return base.attachedPermits();
        }

        public int availablePermits() {
            ensure();
            return base.availablePermits();
        }

        public void detach() {
            ensure();
            base.detach();
        }

        public void detach(int permits) {
            check(permits);
            ensure();
            base.detach(permits);
        }

        public void destroy() {
            ensure();
            base.destroy();
        }

        public int drainPermits() {
            ensure();
            return base.drainPermits();
        }

        public void reducePermits(int permits) {
            check(permits);
            ensure();
            base.reducePermits(permits);
        }

        public void release() {
            ensure();
            base.release();
        }

        public void release(int permits) {
            check(permits);
            ensure();
            base.release(permits);
        }

        public void releaseDetach() {
            ensure();
            base.releaseDetach();
        }

        public void releaseDetach(int permits) {
            check(permits);
            ensure();
            base.releaseDetach(permits);
        }

        public boolean tryAcquire() {
            ensure();
            return base.tryAcquire();
        }

        public boolean tryAcquire(int permits) {
            check(permits);
            ensure();
            return base.tryAcquire(permits);
        }

        public boolean tryAcquire(long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
            ensure();
            return base.tryAcquire(timeout, unit);
        }

        public boolean tryAcquire(int permits, long timeout, TimeUnit timeunit) throws InstanceDestroyedException, InterruptedException {
            check(permits, timeout, timeunit);
            ensure();
            return base.tryAcquire(permits, timeout, timeunit);
        }

        public boolean tryAcquireAttach() {
            ensure();
            return base.tryAcquireAttach();
        }

        public boolean tryAcquireAttach(int permits) {
            check(permits);
            ensure();
            return base.tryAcquireAttach(permits);
        }

        public boolean tryAcquireAttach(long timeout, TimeUnit timeunit) throws InstanceDestroyedException, InterruptedException {
            ensure();
            return base.tryAcquireAttach(timeout, timeunit);
        }

        public boolean tryAcquireAttach(int permits, long timeout, TimeUnit timeunit) throws InstanceDestroyedException, InterruptedException {
            check(permits, timeout, timeunit);
            ensure();
            return base.tryAcquireAttach(permits, timeout, timeunit);
        }

        private void check(int permits) {
            if (permits < 0)
                throw new IllegalArgumentException("Number of permits can not be negative: " + permits);
        }

        private void check(int permits, long timeout, TimeUnit timeunit) {
            check(permits);
            if (timeout < -1)
                throw new IllegalArgumentException("Invalid timeout value: " + timeout);
            if (timeunit == null) {
                throw new NullPointerException("TimeUnit can not be null.");
            }
        }

        private Future doAsyncAcquire(final Integer permits, final Boolean attach) {
            final SemaphoreProxyImpl semaphoreProxy = SemaphoreProxyImpl.this;
            AsyncCall call = new AsyncCall() {
                @Override
                protected void call() {
                    try {
                        if (attach)
                            semaphoreProxy.acquireAttach(permits);
                        else
                            semaphoreProxy.acquire(permits);
                        setResult(null);
                    } catch (InterruptedException e) {
                        setResult(e);
                    } catch (InstanceDestroyedException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    ConcurrentMapManager.MSemaphore msemaphore = factory.node.concurrentMapManager.new MSemaphore();
                    return msemaphore.cancelAcquire(getNameAsData());
                }
            };
            factory.node.executorManager.executeAsync(call);
            return call;
        }

        private class SemaphoreProxyReal implements SemaphoreProxy {
            SemaphoreOperationsCounter operationsCounter = new SemaphoreOperationsCounter();

            public Object getId() {
                return name;
            }

            public InstanceType getInstanceType() {
                return InstanceType.SEMAPHORE;
            }

            public String getLongName() {
                return name;
            }

            public String getName() {
                return name.substring(Prefix.SEMAPHORE.length());
            }

            public void destroy() {
                newMSemaphore().destroy(getNameAsData());
                factory.destroyInstanceClusterWide(name, null);
            }

            public void acquire() throws InstanceDestroyedException, InterruptedException {
                acquire(1);
            }

            public void acquire(int permits) throws InstanceDestroyedException, InterruptedException {
                if (Thread.interrupted())
                    throw new InterruptedException();
                try {
                    doTryAcquire(permits, false, -1);
                } catch (RuntimeInterruptedException e) {
                    throw new InterruptedException();
                }
            }

            public Future acquireAsync() {
                throw new UnsupportedOperationException();
            }

            public Future acquireAsync(int permits) {
                throw new UnsupportedOperationException();
            }

            public void acquireAttach() throws InstanceDestroyedException, InterruptedException {
                acquireAttach(1);
            }

            public void acquireAttach(int permits) throws InstanceDestroyedException, InterruptedException {
                if (Thread.interrupted())
                    throw new InterruptedException();
                try {
                    doTryAcquire(permits, true, -1);
                } catch (RuntimeInterruptedException e) {
                    throw new InterruptedException();
                }
            }

            public Future acquireAttachAsync() {
                throw new UnsupportedOperationException();
            }

            public Future acquireAttachAsync(int permits) {
                throw new UnsupportedOperationException();
            }

            public void attach() {
                attach(1);
            }

            public void attach(int permits) {
                newMSemaphore().attachDetach(getNameAsData(), permits);
            }

            public int attachedPermits() {
                return newMSemaphore().getAttached(getNameAsData());
            }

            public int availablePermits() {
                return newMSemaphore().getAvailable(getNameAsData());
            }

            public void detach() {
                detach(1);
            }

            public void detach(int permits) {
                newMSemaphore().attachDetach(getNameAsData(), -permits);
            }

            public int drainPermits() {
                return newMSemaphore().drainPermits(getNameAsData());
            }

            public void release() {
                release(1);
            }

            public void release(int permits) {
                newMSemaphore().release(getNameAsData(), permits, false);
            }

            public void releaseDetach() {
                releaseDetach(1);
            }

            public void releaseDetach(int permits) {
                newMSemaphore().release(getNameAsData(), permits, true);
            }

            public boolean tryAcquire() {
                return tryAcquire(1);
            }

            public boolean tryAcquire(int permits) {
                try {
                    return doTryAcquire(permits, false, -1);
                } catch (Throwable e) {
                    return false;
                }
            }

            public boolean tryAcquire(long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
                return tryAcquire(1, timeout, unit);
            }

            public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
                if (Thread.interrupted())
                    throw new InterruptedException();
                try {
                    return doTryAcquire(permits, false, unit.toMillis(timeout));
                } catch (RuntimeInterruptedException e) {
                    throw new InterruptedException();
                }
            }

            public boolean tryAcquireAttach() {
                return tryAcquireAttach(1);
            }

            public boolean tryAcquireAttach(int permits) {
                try {
                    return doTryAcquire(permits, true, -1);
                } catch (Throwable e) {
                    return false;
                }
            }

            public boolean tryAcquireAttach(long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
                return tryAcquireAttach(1, timeout, unit);
            }

            public boolean tryAcquireAttach(int permits, long timeout, TimeUnit unit) throws InstanceDestroyedException, InterruptedException {
                if (Thread.interrupted())
                    throw new InterruptedException();
                try {
                    return doTryAcquire(permits, true, unit.toMillis(timeout));
                } catch (RuntimeInterruptedException e) {
                    throw new InterruptedException();
                }
            }

            public void reducePermits(int permits) {
                newMSemaphore().reduce(getNameAsData(), permits);
            }

            public LocalSemaphoreStats getLocalSemaphoreStats() {
                LocalSemaphoreStatsImpl localSemaphoreStats = new LocalSemaphoreStatsImpl();
                localSemaphoreStats.setOperationStats(operationsCounter.getPublishedStats());
                return localSemaphoreStats;
            }

            public SemaphoreOperationsCounter getOperationsCounter() {
                return operationsCounter;
            }

            private ConcurrentMapManager.MSemaphore newMSemaphore() {
                ConcurrentMapManager.MSemaphore msemaphore = factory.node.concurrentMapManager.new MSemaphore();
                msemaphore.setOperationsCounter(operationsCounter);
                return msemaphore;
            }

            private boolean doTryAcquire(int permits, boolean attach, long timeout) throws InstanceDestroyedException {
                return newMSemaphore().tryAcquire(getNameAsData(), permits, attach, timeout);
            }
        }
    }

    public static class SetProxyImpl extends AbstractCollection implements ISet, DataSerializable, HazelcastInstanceAwareInstance {
        String name = null;
        private transient ISet base = null;
        private transient FactoryImpl factory = null;

        public SetProxyImpl() {
        }

        public SetProxyImpl(String name, FactoryImpl factory) {
            this.name = name;
            this.factory = factory;
            this.base = new SetProxyReal();
        }

        public ISet getBase() {
            return base;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.factory = (FactoryImpl) hazelcastInstance;
        }

        private void ensure() {
            factory.initialChecks();
            if (base == null) {
                base = (ISet) factory.getOrCreateProxyByName(name);
            }
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        @Override
        public String toString() {
            ensure();
            return "Set [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SetProxyImpl that = (SetProxyImpl) o;
            return !(name != null ? !name.equals(that.name) : that.name != null);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public int size() {
            ensure();
            return base.size();
        }

        public boolean contains(Object o) {
            ensure();
            return base.contains(o);
        }

        public Iterator iterator() {
            ensure();
            return base.iterator();
        }

        public boolean add(Object o) {
            ensure();
            return base.add(o);
        }

        public boolean remove(Object o) {
            ensure();
            return base.remove(o);
        }

        public void clear() {
            ensure();
            base.clear();
        }

        public InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public void destroy() {
            factory.destroyInstanceClusterWide(name, null);
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(name);
        }

        public void readData(DataInput in) throws IOException {
            name = in.readUTF();
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            writeData(out);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            readData(in);
        }

        public String getName() {
            ensure();
            return base.getName();
        }

        public void addItemListener(ItemListener itemListener, boolean includeValue) {
            ensure();
            base.addItemListener(itemListener, includeValue);
        }

        public void removeItemListener(ItemListener itemListener) {
            ensure();
            base.removeItemListener(itemListener);
        }

        class SetProxyReal extends AbstractCollection implements ISet {

            final MProxy mapProxy;

            public SetProxyReal() {
                mapProxy = new MProxyImpl(name, factory);
            }

            public Object getId() {
                return name;
            }

            @Override
            public boolean equals(Object o) {
                return SetProxyImpl.this.equals(o);
            }

            @Override
            public int hashCode() {
                return SetProxyImpl.this.hashCode();
            }

            public InstanceType getInstanceType() {
                return BaseManager.getInstanceType(name);
            }

            public void addItemListener(ItemListener listener, boolean includeValue) {
                mapProxy.addGenericListener(listener, null, includeValue,
                        getInstanceType());
            }

            public void removeItemListener(ItemListener listener) {
                mapProxy.removeGenericListener(listener, null);
            }

            public String getName() {
                return name.substring(Prefix.SET.length());
            }

            @Override
            public boolean add(Object obj) {
                return mapProxy.add(obj);
            }

            @Override
            public boolean remove(Object obj) {
                return mapProxy.removeKey(obj);
            }

            @Override
            public boolean contains(Object obj) {
                return mapProxy.containsKey(obj);
            }

            @Override
            public Iterator iterator() {
                return mapProxy.keySet().iterator();
            }

            @Override
            public int size() {
                return mapProxy.size();
            }

            @Override
            public void clear() {
                mapProxy.clear();
            }

            public void destroy() {
                factory.destroyInstanceClusterWide(name, null);
            }
        }
    }

    public static class QProxyImpl extends AbstractQueue implements QProxy, HazelcastInstanceAwareInstance, DataSerializable {
        private transient QProxy qproxyReal = null;
        private transient FactoryImpl factory = null;
        private String name = null;
        private BlockingQueueManager blockingQueueManager = null;
        private ListenerManager listenerManager = null;

        public QProxyImpl() {
        }

        private QProxyImpl(String name, FactoryImpl factory) {
            this.name = name;
            qproxyReal = new QProxyReal();
            setHazelcastInstance(factory);
        }

        public FactoryImpl getFactory() {
            return factory;
        }

        public String getLongName() {
            ensure();
            return qproxyReal.getLongName();
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.factory = (FactoryImpl) hazelcastInstance;
            this.blockingQueueManager = factory.node.blockingQueueManager;
            this.listenerManager = factory.node.listenerManager;
        }

        private void ensure() {
            ThreadContext.get().setCurrentFactory(factory);
            factory.initialChecks();
            if (qproxyReal == null) {
                qproxyReal = (QProxy) factory.getOrCreateProxyByName(name);
            }
        }

        public Object getId() {
            ensure();
            return qproxyReal.getId();
        }

        @Override
        public String toString() {
            return "Queue [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QProxyImpl qProxy = (QProxyImpl) o;
            return !(name != null ? !name.equals(qProxy.name) : qProxy.name != null);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(name);
        }

        public void readData(DataInput in) throws IOException {
            name = in.readUTF();
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            writeData(out);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            readData(in);
        }

        public LocalQueueStats getLocalQueueStats() {
            ensure();
            return qproxyReal.getLocalQueueStats();
        }

        public Iterator iterator() {
            ensure();
            return qproxyReal.iterator();
        }

        public int size() {
            ensure();
            return qproxyReal.size();
        }

        public void addItemListener(ItemListener listener, boolean includeValue) {
            ensure();
            qproxyReal.addItemListener(listener, includeValue);
        }

        public void removeItemListener(ItemListener listener) {
            ensure();
            qproxyReal.removeItemListener(listener);
        }

        public String getName() {
            ensure();
            return qproxyReal.getName();
        }

        public int drainTo(Collection c) {
            ensure();
            return qproxyReal.drainTo(c);
        }

        public int drainTo(Collection c, int maxElements) {
            ensure();
            return qproxyReal.drainTo(c, maxElements);
        }

        public void destroy() {
            ensure();
            qproxyReal.destroy();
        }

        public InstanceType getInstanceType() {
            ensure();
            return qproxyReal.getInstanceType();
        }

        public boolean offer(Object o) {
            ensure();
            return qproxyReal.offer(o);
        }

        public boolean offer(Object obj, long timeout, TimeUnit unit) throws InterruptedException {
            ensure();
            return qproxyReal.offer(obj, timeout, unit);
        }

        public void put(Object obj) throws InterruptedException {
            ensure();
            qproxyReal.put(obj);
        }

        public Object poll() {
            ensure();
            return qproxyReal.poll();
        }

        public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
            ensure();
            return qproxyReal.poll(timeout, unit);
        }

        public Object take() throws InterruptedException {
            ensure();
            return qproxyReal.take();
        }

        public int remainingCapacity() {
            ensure();
            return qproxyReal.remainingCapacity();
        }

        public Object peek() {
            ensure();
            return qproxyReal.peek();
        }

        public QueueOperationsCounter getQueueOperationCounter() {
            return qproxyReal.getQueueOperationCounter();
        }

        private class QProxyReal extends AbstractQueue implements QProxy {
            private final QueueOperationsCounter operationsCounter = new QueueOperationsCounter();

            public QProxyReal() {
            }

            public LocalQueueStats getLocalQueueStats() {
                operationsCounter.incrementOtherOperations();
                LocalQueueStatsImpl localQueueStats = blockingQueueManager.getOrCreateBQ(name).getQueueStats();
                localQueueStats.setOperationStats(operationsCounter.getPublishedStats());
                return localQueueStats;
            }

            public String getLongName() {
                return name;
            }

            public boolean offer(Object obj) {
                check(obj);
                try {
                    return offer(obj, 0, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                    return false;
                }
            }

            public boolean offer(Object obj, long timeout, TimeUnit unit) throws InterruptedException {
                check(obj);
                if (timeout < 0) {
                    timeout = 0;
                }
                boolean result = blockingQueueManager.offer(name, obj, unit.toMillis(timeout));
                if (!result) {
                    operationsCounter.incrementRejectedOffers();
                }
                operationsCounter.incrementOffers();
                return result;
            }

            public void put(Object obj) throws InterruptedException {
                check(obj);
                blockingQueueManager.offer(name, obj, -1);
                operationsCounter.incrementOffers();
            }

            public Object peek() {
                operationsCounter.incrementOtherOperations();
                return blockingQueueManager.peek(name);
            }

            public Object poll() {
                try {
                    Object result = blockingQueueManager.poll(name, 0);
                    if (result == null) {
                        operationsCounter.incrementEmptyPolls();
                    }
                    operationsCounter.incrementPolls();
                    return result;
                } catch (InterruptedException e) {
                    return null;
                }
            }

            public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
                if (timeout < 0) {
                    timeout = 0;
                }
                Object result = blockingQueueManager.poll(name, unit.toMillis(timeout));
                if (result == null) {
                    operationsCounter.incrementEmptyPolls();
                }
                operationsCounter.incrementPolls();
                return result;
            }

            public Object take() throws InterruptedException {
                Object result = blockingQueueManager.poll(name, -1);
                if (result == null) {
                    operationsCounter.incrementEmptyPolls();
                }
                operationsCounter.incrementPolls();
                return result;
            }

            public int remainingCapacity() {
                operationsCounter.incrementOtherOperations();
                BlockingQueueManager.BQ q = blockingQueueManager.getOrCreateBQ(name);
                int maxSizePerJVM = q.getMaxSizePerJVM();
                if (maxSizePerJVM <= 0) {
                    return Integer.MAX_VALUE;
                } else {
                    int size = size();
                    int numberOfMembers = factory.node.getClusterImpl().getMembers().size();
                    int totalCapacity = numberOfMembers * maxSizePerJVM;
                    return totalCapacity - size;
                }
            }

            @Override
            public Iterator iterator() {
                operationsCounter.incrementOtherOperations();
                return blockingQueueManager.iterate(name);
            }

            @Override
            public int size() {
                operationsCounter.incrementOtherOperations();
                return blockingQueueManager.size(name);
            }

            public void addItemListener(ItemListener listener, boolean includeValue) {
                blockingQueueManager.addItemListener(name, listener, includeValue);
            }

            public void removeItemListener(ItemListener listener) {
                blockingQueueManager.removeItemListener(name, listener);
            }

            public String getName() {
                return name.substring(Prefix.QUEUE.length());
            }

            @Override
            public boolean remove(Object obj) {
                throw new UnsupportedOperationException();
            }

            public int drainTo(Collection c) {
                return drainTo(c, Integer.MAX_VALUE);
            }

            public int drainTo(Collection c, int maxElements) {
                if (c == null) throw new NullPointerException("drainTo null!");
                if (maxElements < 0) throw new IllegalArgumentException("Negative maxElements:" + maxElements);
                if (maxElements == 0) return 0;
                if (c instanceof QProxy) {
                    QProxy q = (QProxy) c;
                    if (q.getName().equals(getName())) {
                        throw new IllegalArgumentException("Cannot drainTo self!");
                    }
                }
                operationsCounter.incrementOtherOperations();
                int added = 0;
                Object value = null;
                do {
                    value = poll();
                    if (value != null) {
                        if (!c.add(value)) {
                            throw new RuntimeException("drainTo is not able to add!");
                        }
                        added++;
                    }
                } while (added < maxElements && value != null);
                return added;
            }

            public void destroy() {
                operationsCounter.incrementOtherOperations();
                factory.destroyInstanceClusterWide(name, null);
                factory.destroyInstanceClusterWide(Prefix.MAP + name, null);
            }

            public InstanceType getInstanceType() {
                return InstanceType.QUEUE;
            }

            public Object getId() {
                return name;
            }

            public QueueOperationsCounter getQueueOperationCounter() {
                return operationsCounter;
            }
        }
    }

    public static class MultiMapProxyImpl extends FactoryAwareNamedProxy implements MultiMapProxy, DataSerializable, IGetAwareProxy {

        private transient MultiMapProxy base = null;

        public MultiMapProxyImpl() {
        }

        public MultiMapProxyImpl(String name, FactoryImpl factory) {
            setName(name);
            setHazelcastInstance(factory);
            this.base = new MultiMapReal();
        }

        private void ensure() {
            factory.initialChecks();
            if (base == null) {
                base = (MultiMapProxy) factory.getOrCreateProxyByName(name);
            }
        }

        public MultiMapReal getBase() {
            return (MultiMapReal) base;
        }

        public MProxy getMProxy() {
            ensure();
            return base.getMProxy();
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        @Override
        public String toString() {
            return "MultiMap [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MultiMapProxyImpl that = (MultiMapProxyImpl) o;
            return !(name != null ? !name.equals(that.name) : that.name != null);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public void destroy() {
            factory.destroyInstanceClusterWide(name, null);
        }

        public String getName() {
            ensure();
            return base.getName();
        }

        public boolean put(Object key, Object value) {
            ensure();
            return base.put(key, value);
        }

        public Collection get(Object key) {
            ensure();
            return base.get(key);
        }

        public boolean remove(Object key, Object value) {
            ensure();
            return base.remove(key, value);
        }

        public Collection remove(Object key) {
            ensure();
            return base.remove(key);
        }

        public Set localKeySet() {
            ensure();
            return base.localKeySet();
        }

        public Set keySet() {
            ensure();
            return base.keySet();
        }

        public Collection values() {
            ensure();
            return base.values();
        }

        public Set entrySet() {
            ensure();
            return base.entrySet();
        }

        public boolean containsKey(Object key) {
            ensure();
            return base.containsKey(key);
        }

        public boolean containsValue(Object value) {
            ensure();
            return base.containsValue(value);
        }

        public boolean containsEntry(Object key, Object value) {
            ensure();
            return base.containsEntry(key, value);
        }

        public int size() {
            ensure();
            return base.size();
        }

        public void clear() {
            ensure();
            base.clear();
        }

        public int valueCount(Object key) {
            ensure();
            return base.valueCount(key);
        }

        public void addLocalEntryListener(EntryListener entryListener) {
            ensure();
            base.addLocalEntryListener(entryListener);
        }

        public void addEntryListener(EntryListener entryListener, boolean includeValue) {
            ensure();
            base.addEntryListener(entryListener, includeValue);
        }

        public void removeEntryListener(EntryListener entryListener) {
            ensure();
            base.removeEntryListener(entryListener);
        }

        public void addEntryListener(EntryListener entryListener, Object key, boolean includeValue) {
            ensure();
            base.addEntryListener(entryListener, key, includeValue);
        }

        public void removeEntryListener(EntryListener entryListener, Object key) {
            ensure();
            base.removeEntryListener(entryListener, key);
        }

        public void lock(Object key) {
            ensure();
            base.lock(key);
        }

        public boolean tryLock(Object key) {
            ensure();
            return base.tryLock(key);
        }

        public boolean tryLock(Object key, long time, TimeUnit timeunit) {
            ensure();
            return base.tryLock(key, time, timeunit);
        }

        public void unlock(Object key) {
            ensure();
            base.unlock(key);
        }

        public boolean lockMap(long time, TimeUnit timeunit) {
            ensure();
            return base.lockMap(time, timeunit);
        }

        public void unlockMap() {
            ensure();
            base.unlockMap();
        }

        class MultiMapReal implements MultiMapProxy, IGetAwareProxy {
            final MProxy mapProxy;

            private MultiMapReal() {
                mapProxy = new MProxyImpl(name, factory);
            }

            public MProxy getMProxy() {
                return mapProxy;
            }

            public String getName() {
                return name.substring(Prefix.MULTIMAP.length());
            }

            public void clear() {
                mapProxy.clear();
            }

            public boolean containsEntry(Object key, Object value) {
                return mapProxy.containsEntry(key, value);
            }

            public boolean containsKey(Object key) {
                return mapProxy.containsKey(key);
            }

            public boolean containsValue(Object value) {
                return mapProxy.containsValue(value);
            }

            public Collection get(Object key) {
                return (Collection) mapProxy.get(key);
            }

            public boolean put(Object key, Object value) {
                return mapProxy.putMulti(key, value);
            }

            public boolean remove(Object key, Object value) {
                return mapProxy.removeMulti(key, value);
            }

            public Collection remove(Object key) {
                return (Collection) mapProxy.remove(key);
            }

            public int size() {
                return mapProxy.size();
            }

            public Set localKeySet() {
                return mapProxy.localKeySet();
            }

            public Set keySet() {
                return mapProxy.keySet();
            }

            public Collection values() {
                return mapProxy.values();
            }

            public Set entrySet() {
                return mapProxy.entrySet();
            }

            public int valueCount(Object key) {
                return mapProxy.valueCount(key);
            }

            public InstanceType getInstanceType() {
                return InstanceType.MULTIMAP;
            }

            public void destroy() {
                mapProxy.destroy();
            }

            public Object getId() {
                return name;
            }

            public void addLocalEntryListener(EntryListener entryListener) {
                mapProxy.addLocalEntryListener(entryListener);
            }

            public void addEntryListener(EntryListener entryListener, boolean includeValue) {
                mapProxy.addEntryListener(entryListener, includeValue);
            }

            public void removeEntryListener(EntryListener entryListener) {
                mapProxy.removeEntryListener(entryListener);
            }

            public void addEntryListener(EntryListener entryListener, Object key, boolean includeValue) {
                mapProxy.addEntryListener(entryListener, key, includeValue);
            }

            public void removeEntryListener(EntryListener entryListener, Object key) {
                mapProxy.removeEntryListener(entryListener, key);
            }

            public void lock(Object key) {
                mapProxy.lock(key);
            }

            public boolean tryLock(Object key) {
                return mapProxy.tryLock(key);
            }

            public boolean tryLock(Object key, long time, TimeUnit timeunit) {
                return mapProxy.tryLock(key, time, timeunit);
            }

            public void unlock(Object key) {
                mapProxy.unlock(key);
            }

            public boolean lockMap(long time, TimeUnit timeunit) {
                return mapProxy.lockMap(time, timeunit);
            }

            public void unlockMap() {
                mapProxy.unlockMap();
            }
        }
    }

    private static void check(Object obj) {
        if (obj == null) {
            throw new NullPointerException("Object cannot be null.");
        }
        if (!(obj instanceof Serializable)) {
            throw new IllegalArgumentException(obj.getClass().getName() + " is not Serializable.");
        }
    }

    public static class MProxyImpl extends FactoryAwareNamedProxy implements MProxy, DataSerializable {

        private transient MProxy mproxyReal = null;

        private transient ConcurrentMapManager concurrentMapManager = null;

        private transient ListenerManager listenerManager = null;

        private volatile transient MProxy dynamicProxy;

        public MProxyImpl() {
        }

        class DynamicInvoker implements InvocationHandler {
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                beforeCall();
                try {
                    return method.invoke(mproxyReal, args);
                } catch (Throwable e) {
                    if (e instanceof InvocationTargetException) {
                        InvocationTargetException ite = (InvocationTargetException) e;
                        throw ite.getCause();
                    } else if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    } else {
                        throw new RuntimeException(e);
                    }
                } finally {
                    afterCall();
                }
            }
        }

        MProxyImpl(String name, FactoryImpl factory) {
            setName(name);
            setHazelcastInstance(factory);
            mproxyReal = new MProxyReal();
        }

        public MapOperationsCounter getMapOperationCounter() {
            return mproxyReal.getMapOperationCounter();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            super.setHazelcastInstance(hazelcastInstance);
            this.concurrentMapManager = factory.node.concurrentMapManager;
            this.listenerManager = factory.node.listenerManager;
            ClassLoader cl = MProxy.class.getClassLoader();
            dynamicProxy = (MProxy) Proxy.newProxyInstance(cl, new Class[]{MProxy.class}, new DynamicInvoker());
        }

        private void beforeCall() {
            ThreadContext.get().setCurrentFactory(factory);
            factory.initialChecks();
            if (mproxyReal == null) {
                mproxyReal = (MProxy) factory.getOrCreateProxyByName(name);
            }
        }

        private void afterCall() {
        }

        public Object get(Object key) {
            beforeCall();
            try {
                return mproxyReal.get(key);
            } catch (Throwable e) {
                if (factory.restarted) {
                    return get(key);
                } else if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            } finally {
                afterCall();
            }
        }

        public Object put(Object key, Object value) {
            return put(key, value, 0, TimeUnit.SECONDS);
        }

        public Future getAsync(Object key) {
            final MProxyImpl mProxy = MProxyImpl.this;
            final Data dataKey = toData(key);
            AsyncCall call = new AsyncCall() {
                @Override
                protected void call() {
                    setResult(mProxy.get(dataKey));
                }
            };
            factory.node.executorManager.executeAsync(call);
            return call;
        }

        public Future putAsync(Object key, Object value) {
            final MProxyImpl mProxy = MProxyImpl.this;
            final Data dataKey = toData(key);
            final Data dataValue = toData(value);
            AsyncCall call = new AsyncCall() {
                @Override
                protected void call() {
                    setResult(mProxy.put(dataKey, dataValue));
                }
            };
            factory.node.executorManager.executeAsync(call);
            return call;
        }

        public Future removeAsync(Object key) {
            final MProxyImpl mProxy = MProxyImpl.this;
            final Data dataKey = toData(key);
            AsyncCall call = new AsyncCall() {
                @Override
                protected void call() {
                    setResult(mProxy.remove(dataKey));
                }
            };
            factory.node.executorManager.executeAsync(call);
            return call;
        }

        public Object put(Object key, Object value, long ttl, TimeUnit timeunit) {
            beforeCall();
            try {
                return mproxyReal.put(key, value, ttl, timeunit);
            } catch (Throwable e) {
                if (factory.restarted) {
                    return put(key, value);
                } else if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            } finally {
                afterCall();
            }
        }

        public Object remove(Object key) {
            beforeCall();
            try {
                return mproxyReal.remove(key);
            } catch (Throwable e) {
                if (factory.restarted) {
                    return remove(key);
                } else if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            } finally {
                afterCall();
            }
        }

        public Object tryRemove(Object key, long time, TimeUnit timeunit) throws TimeoutException {
            beforeCall();
            try {
                return mproxyReal.tryRemove(key, time, timeunit);
            } catch (Throwable e) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else if (e instanceof TimeoutException) {
                    throw (TimeoutException) e;
                } else {
                    throw new RuntimeException(e);
                }
            } finally {
                afterCall();
            }
        }

        public void putAndUnlock(Object key, Object value) {
            dynamicProxy.putAndUnlock(key, value);
        }

        public Object tryLockAndGet(Object key, long time, TimeUnit timeunit) throws TimeoutException {
            return dynamicProxy.tryLockAndGet(key, time, timeunit);
        }

        public Map getAll(Set keys) {
            return dynamicProxy.getAll(keys);
        }

        public void flush() {
            dynamicProxy.flush();
        }

        public void putForSync(Object key, Object value) {
            dynamicProxy.putForSync(key, value);
        }

        public void removeForSync(Object key) {
            dynamicProxy.removeForSync(key);
        }

        public void putTransient(Object key, Object value, long time, TimeUnit timeunit) {
            dynamicProxy.putTransient(key, value, time, timeunit);
        }

        public boolean tryPut(Object key, Object value, long time, TimeUnit timeunit) {
            return dynamicProxy.tryPut(key, value, time, timeunit);
        }

        public Object putIfAbsent(Object key, Object value, long ttl, TimeUnit timeunit) {
            return dynamicProxy.putIfAbsent(key, value, ttl, timeunit);
        }

        public Object putIfAbsent(Object key, Object value) {
            return dynamicProxy.putIfAbsent(key, value);
        }

        public LocalMapStats getLocalMapStats() {
            return dynamicProxy.getLocalMapStats();
        }

        public void addIndex(String attribute, boolean ordered) {
            dynamicProxy.addIndex(attribute, ordered);
        }

        public void addIndex(Expression expression, boolean ordered) {
            dynamicProxy.addIndex(expression, ordered);
        }

        public Object getId() {
            return dynamicProxy.getId();
        }

        @Override
        public String toString() {
            return "Map [" + getName() + "] " + factory;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MProxyImpl mProxy = (MProxyImpl) o;
            return name.equals(mProxy.name);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public void destroy() {
            dynamicProxy.destroy();
        }

        public InstanceType getInstanceType() {
            return dynamicProxy.getInstanceType();
        }

        public boolean removeKey(Object key) {
            return dynamicProxy.removeKey(key);
        }

        public int size() {
            return dynamicProxy.size();
        }

        public boolean isEmpty() {
            return dynamicProxy.isEmpty();
        }

        public boolean containsKey(Object key) {
            return dynamicProxy.containsKey(key);
        }

        public boolean containsValue(Object value) {
            return dynamicProxy.containsValue(value);
        }

        public MapEntry getMapEntry(Object key) {
            return dynamicProxy.getMapEntry(key);
        }

        public void putAll(Map t) {
            dynamicProxy.putAll(t);
        }

        public void clear() {
            dynamicProxy.clear();
        }

        public int valueCount(Object key) {
            return dynamicProxy.valueCount(key);
        }

        public Set allKeys() {
            return dynamicProxy.allKeys();
        }

        public Set localKeySet() {
            return dynamicProxy.localKeySet();
        }

        public Set localKeySet(Predicate predicate) {
            return dynamicProxy.localKeySet(predicate);
        }

        public Set keySet() {
            return dynamicProxy.keySet();
        }

        public Collection values() {
            return dynamicProxy.values();
        }

        public Set entrySet() {
            return dynamicProxy.entrySet();
        }

        public Set keySet(Predicate predicate) {
            return dynamicProxy.keySet(predicate);
        }

        public Collection values(Predicate predicate) {
            return dynamicProxy.values(predicate);
        }

        public Set entrySet(Predicate predicate) {
            return dynamicProxy.entrySet(predicate);
        }

        public boolean remove(Object key, Object value) {
            return dynamicProxy.remove(key, value);
        }

        public boolean replace(Object key, Object oldValue, Object newValue) {
            return dynamicProxy.replace(key, oldValue, newValue);
        }

        public Object replace(Object key, Object value) {
            return dynamicProxy.replace(key, value);
        }

        public String getName() {
            return name.substring(Prefix.MAP.length());
        }

        public boolean lockMap(long time, TimeUnit timeunit) {
            return dynamicProxy.lockMap(time, timeunit);
        }

        public void unlockMap() {
            dynamicProxy.unlockMap();
        }

        public void lock(Object key) {
            dynamicProxy.lock(key);
        }

        public boolean tryLock(Object key) {
            return dynamicProxy.tryLock(key);
        }

        public boolean tryLock(Object key, long time, TimeUnit timeunit) {
            return dynamicProxy.tryLock(key, time, timeunit);
        }

        public void unlock(Object key) {
            dynamicProxy.unlock(key);
        }

        public String getLongName() {
            return dynamicProxy.getLongName();
        }

        public void addGenericListener(Object listener, Object key, boolean includeValue,
                                       InstanceType instanceType) {
            dynamicProxy.addGenericListener(listener, key, includeValue, instanceType);
        }

        public void removeGenericListener(Object listener, Object key) {
            dynamicProxy.removeGenericListener(listener, key);
        }

        public void addLocalEntryListener(EntryListener entryListener) {
            dynamicProxy.addLocalEntryListener(entryListener);
        }

        public void addEntryListener(EntryListener listener, boolean includeValue) {
            dynamicProxy.addEntryListener(listener, includeValue);
        }

        public void addEntryListener(EntryListener listener, Object key, boolean includeValue) {
            dynamicProxy.addEntryListener(listener, key, includeValue);
        }

        public void removeEntryListener(EntryListener listener) {
            dynamicProxy.removeEntryListener(listener);
        }

        public void removeEntryListener(EntryListener listener, Object key) {
            dynamicProxy.removeEntryListener(listener, key);
        }

        public boolean containsEntry(Object key, Object value) {
            return dynamicProxy.containsEntry(key, value);
        }

        public boolean putMulti(Object key, Object value) {
            return dynamicProxy.putMulti(key, value);
        }

        public boolean removeMulti(Object key, Object value) {
            return dynamicProxy.removeMulti(key, value);
        }

        public boolean add(Object value) {
            return dynamicProxy.add(value);
        }

        public boolean evict(Object key) {
            return dynamicProxy.evict(key);
        }

        private class MProxyReal implements MProxy {
            private final transient MapOperationsCounter mapOperationCounter = new MapOperationsCounter();

            public MProxyReal() {
                super();
            }

            @Override
            public String toString() {
                return "Map [" + getName() + "]";
            }

            public InstanceType getInstanceType() {
                return InstanceType.MAP;
            }

            public Object getId() {
                return name;
            }

            @Override
            public boolean equals(Object o) {
                return MProxyImpl.this.equals(o);
            }

            @Override
            public int hashCode() {
                return MProxyImpl.this.hashCode();
            }

            public String getLongName() {
                return name;
            }

            public String getName() {
                return name.substring(Prefix.MAP.length());
            }

            public void addIndex(final String attribute, final boolean ordered) {
                addIndex(Predicates.get(attribute), ordered);
            }

            public void addIndex(final Expression expression, final boolean ordered) {
                final CountDownLatch latch = new CountDownLatch(1);
                concurrentMapManager.enqueueAndReturn(new Processable() {
                    public void process() {
                        AddMapIndex addMapIndexProcess = new AddMapIndex(name, expression, ordered);
                        concurrentMapManager.sendProcessableToAll(addMapIndexProcess, true);
                        latch.countDown();
                    }
                });
                try {
                    latch.await();
                } catch (InterruptedException ignored) {
                }
            }

            public void flush() {
                concurrentMapManager.flush(name);
            }

            public MapEntry getMapEntry(Object key) {
                long begin = System.currentTimeMillis();
                check(key);
                MGetMapEntry mgetMapEntry = concurrentMapManager.new MGetMapEntry();
                MapEntry mapEntry = mgetMapEntry.get(name, key);
                mapOperationCounter.incrementGets(System.currentTimeMillis() - begin);
                return mapEntry;
            }

            public boolean putMulti(Object key, Object value) {
                check(key);
                check(value);
                MPutMulti mput = concurrentMapManager.new MPutMulti();
                return mput.put(name, key, value);
            }

            public Object put(Object key, Object value) {
                long begin = System.currentTimeMillis();
                check(key);
                check(value);
                MPut mput = ThreadContext.get().getCallCache(factory).getMPut();
                Object result = mput.put(name, key, value, -1, -1);
                mapOperationCounter.incrementPuts(System.currentTimeMillis() - begin);
                return result;
            }

            public void putForSync(Object key, Object value) {
                long begin = System.currentTimeMillis();
                check(key);
                check(value);
                MPut mput = ThreadContext.get().getCallCache(factory).getMPut();
                mput.putForSync(name, key, value);
                mapOperationCounter.incrementPuts(System.currentTimeMillis() - begin);
            }

            public void removeForSync(Object key) {
                long begin = System.currentTimeMillis();
                check(key);
                MRemove mremove = ThreadContext.get().getCallCache(factory).getMRemove();
                mremove.removeForSync(name, key);
                mapOperationCounter.incrementRemoves(System.currentTimeMillis() - begin);
            }

            public Map getAll(Set keys) {
                if (keys == null) {
                    throw new NullPointerException();
                }
                return concurrentMapManager.getAll(name, keys);
            }

            public Future getAsync(Object key) {
                throw new UnsupportedOperationException();
            }

            public Future putAsync(Object key, Object value) {
                throw new UnsupportedOperationException();
            }

            public Future removeAsync(Object key) {
                throw new UnsupportedOperationException();
            }

            public Object put(Object key, Object value, long ttl, TimeUnit timeunit) {
                if (ttl < 0) {
                    throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
                }
                if (ttl == 0) {
                    ttl = -1;
                } else {
                    ttl = toMillis(ttl, timeunit);
                }
                return put(key, value, -1, ttl);
            }

            public void putTransient(Object key, Object value, long ttl, TimeUnit timeunit) {
                if (ttl < 0) {
                    throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
                }
                if (ttl == 0) {
                    ttl = -1;
                } else {
                    ttl = toMillis(ttl, timeunit);
                }
                concurrentMapManager.putTransient(name, key, value, -1, ttl);
            }

            public Object put(Object key, Object value, long timeout, long ttl) {
                long begin = System.currentTimeMillis();
                check(key);
                check(value);
                MPut mput = ThreadContext.get().getCallCache(factory).getMPut();
                Object result = mput.put(name, key, value, timeout, ttl);
                mapOperationCounter.incrementPuts(System.currentTimeMillis() - begin);
                return result;
            }

            public boolean tryPut(Object key, Object value, long timeout, TimeUnit timeunit) {
                long begin = System.currentTimeMillis();
                if (timeout < 0) {
                    throw new IllegalArgumentException("timeout value cannot be negative. " + timeout);
                }
                if (timeout == 0) {
                    timeout = -1;
                } else {
                    timeout = toMillis(timeout, timeunit);
                }
                check(key);
                check(value);
                MPut mput = ThreadContext.get().getCallCache(factory).getMPut();
                Boolean result = mput.tryPut(name, key, value, timeout, -1);
                mapOperationCounter.incrementPuts(System.currentTimeMillis() - begin);
                return result;
            }

            public Object tryLockAndGet(Object key, long timeout, TimeUnit timeunit) throws TimeoutException {
                long begin = System.currentTimeMillis();
                if (timeout < 0) {
                    throw new IllegalArgumentException("timeout value cannot be negative. " + timeout);
                }
                timeout = toMillis(timeout, timeunit);
                check(key);
                Object result = concurrentMapManager.tryLockAndGet(name, key, timeout);
                mapOperationCounter.incrementGets(System.currentTimeMillis() - begin);
                return result;
            }

            public void putAndUnlock(Object key, Object value) {
                long begin = System.currentTimeMillis();
                check(key);
                check(value);
                concurrentMapManager.putAndUnlock(name, key, value);
                mapOperationCounter.incrementPuts(System.currentTimeMillis() - begin);
            }

            public Object putIfAbsent(Object key, Object value) {
                return putIfAbsent(key, value, -1, -1);
            }

            public Object putIfAbsent(Object key, Object value, long ttl, TimeUnit timeunit) {
                if (ttl < 0) {
                    throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
                }
                if (ttl == 0) {
                    ttl = -1;
                } else {
                    ttl = toMillis(ttl, timeunit);
                }
                return putIfAbsent(key, value, -1, timeunit.toMillis(ttl));
            }

            public Object putIfAbsent(Object key, Object value, long timeout, long ttl) {
                long begin = System.currentTimeMillis();
                check(key);
                check(value);
                MPut mput = concurrentMapManager.new MPut();
                Object result = mput.putIfAbsent(name, key, value, timeout, ttl);
                mapOperationCounter.incrementPuts(System.currentTimeMillis() - begin);
                return result;
            }

            public Object get(Object key) {
                check(key);
                long begin = System.currentTimeMillis();
                MGet mget = ThreadContext.get().getCallCache(factory).getMGet();
                Object result = mget.get(name, key, -1);
                if (result == null && name.contains("testConcurrentLockPrimitive")) {
                    boolean isClient = ThreadContext.get().isClient();
                    Object txn = ThreadContext.get().getTransaction();
                    throw new RuntimeException(result + " testConcurrentLockPrimitive returns null " + isClient + "  " + txn);
                }
                mapOperationCounter.incrementGets(System.currentTimeMillis() - begin);
                return result;
            }

            public Object remove(Object key) {
                long begin = System.currentTimeMillis();
                check(key);
                MRemove mremove = ThreadContext.get().getCallCache(factory).getMRemove();
                Object result = mremove.remove(name, key, -1);
                mapOperationCounter.incrementRemoves(System.currentTimeMillis() - begin);
                return result;
            }

            public Object tryRemove(Object key, long timeout, TimeUnit timeunit) throws TimeoutException {
                long begin = System.currentTimeMillis();
                check(key);
                MRemove mremove = ThreadContext.get().getCallCache(factory).getMRemove();
                Object result = mremove.tryRemove(name, key, toMillis(timeout, timeunit));
                mapOperationCounter.incrementRemoves(System.currentTimeMillis() - begin);
                return result;
            }

            public int size() {
                mapOperationCounter.incrementOtherOperations();
                MSize msize = concurrentMapManager.new MSize(name);
                return msize.getSize();
            }

            public int valueCount(Object key) {
                int count;
                mapOperationCounter.incrementOtherOperations();
                MValueCount mcount = concurrentMapManager.new MValueCount();
                count = ((Number) mcount.count(name, key, -1)).intValue();
                return count;
            }

            public boolean removeMulti(Object key, Object value) {
                check(key);
                check(value);
                MRemoveMulti mremove = concurrentMapManager.new MRemoveMulti();
                return mremove.remove(name, key, value);
            }

            public boolean remove(Object key, Object value) {
                long begin = System.currentTimeMillis();
                check(key);
                check(value);
                MRemove mremove = concurrentMapManager.new MRemove();
                boolean result = (mremove.removeIfSame(name, key, value, -1) != null);
                mapOperationCounter.incrementRemoves(System.currentTimeMillis() - begin);
                return result;
            }

            public Object replace(Object key, Object value) {
                long begin = System.currentTimeMillis();
                check(key);
                check(value);
                MPut mput = concurrentMapManager.new MPut();
                Object result = mput.replace(name, key, value, -1, -1);
                mapOperationCounter.incrementPuts(System.currentTimeMillis() - begin);
                return result;
            }

            public boolean replace(Object key, Object oldValue, Object newValue) {
                long begin = System.currentTimeMillis();
                check(key);
                check(oldValue);
                check(newValue);
                MPut mput = concurrentMapManager.new MPut();
                Boolean result = mput.replace(name, key, oldValue, newValue, -1);
                mapOperationCounter.incrementPuts(System.currentTimeMillis() - begin);
                return result;
            }

            public boolean lockMap(long time, TimeUnit timeunit) {
                if (factory.locksMapProxy.tryLock("map_lock_" + name, time, timeunit)) {
                    MLockMap mLockMap = concurrentMapManager.new MLockMap(name, true);
                    mLockMap.call();
                    return true;
                }
                return false;
            }

            public void unlockMap() {
                MLockMap mLockMap = concurrentMapManager.new MLockMap(name, false);
                mLockMap.call();
                factory.locksMapProxy.unlock("map_lock_" + name);
            }

            public void lock(Object key) {
                check(key);
                mapOperationCounter.incrementOtherOperations();
                MLock mlock = concurrentMapManager.new MLock();
                mlock.lock(name, key, -1);
            }

            public boolean tryLock(Object key) {
                check(key);
                mapOperationCounter.incrementOtherOperations();
                MLock mlock = concurrentMapManager.new MLock();
                return mlock.lock(name, key, 0);
            }

            public boolean tryLock(Object key, long time, TimeUnit timeunit) {
                check(key);
                if (time < 0)
                    throw new IllegalArgumentException("Time cannot be negative. time = " + time);
                mapOperationCounter.incrementOtherOperations();
                long timeoutMillis = timeunit.toMillis(time);
                MLock mlock = concurrentMapManager.new MLock();
                return mlock.lock(name, key, (timeoutMillis < 0 || timeoutMillis == Long.MAX_VALUE) ? -1 : timeoutMillis);
            }

            public void unlock(Object key) {
                check(key);
                mapOperationCounter.incrementOtherOperations();
                MLock mlock = concurrentMapManager.new MLock();
                mlock.unlock(name, key, 0);
            }

            public LocalMapStats getLocalMapStats() {
                mapOperationCounter.incrementOtherOperations();
                LocalMapStatsImpl localMapStats = concurrentMapManager.getLocalMapStats(name);
                localMapStats.setOperationStats(mapOperationCounter.getPublishedStats());
                return localMapStats;
            }

            public void addGenericListener(Object listener, Object key, boolean includeValue,
                                           InstanceType instanceType) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                listenerManager.addListener(name, listener, key, includeValue, instanceType);
            }

            public void removeGenericListener(Object listener, Object key) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                listenerManager.removeListener(name, listener, key);
            }

            public void addLocalEntryListener(EntryListener listener) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                listenerManager.addLocalListener(name, listener, getInstanceType());
            }

            public void addEntryListener(EntryListener listener, boolean includeValue) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                addGenericListener(listener, null, includeValue, getInstanceType());
            }

            public void addEntryListener(EntryListener listener, Object key, boolean includeValue) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                check(key);
                addGenericListener(listener, key, includeValue, getInstanceType());
            }

            public void removeEntryListener(EntryListener listener) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                removeGenericListener(listener, null);
            }

            public void removeEntryListener(EntryListener listener, Object key) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                check(key);
                removeGenericListener(listener, key);
            }

            public boolean containsEntry(Object key, Object value) {
                check(key);
                check(value);
                mapOperationCounter.incrementOtherOperations();
                TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
                if (txn != null) {
                    if (txn.has(name, key)) {
                        Object v = txn.get(name, key);
                        return v != null;
                    }
                }
                MContainsKey mContainsKey = concurrentMapManager.new MContainsKey();
                return mContainsKey.containsEntry(name, key, value);
            }

            public boolean containsKey(Object key) {
                check(key);
                mapOperationCounter.incrementOtherOperations();
                TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
                if (txn != null) {
                    if (txn.has(name, key)) {
                        Object value = txn.get(name, key);
                        return value != null;
                    }
                }
                MContainsKey mContainsKey = concurrentMapManager.new MContainsKey();
                return mContainsKey.containsKey(name, key);
            }

            public boolean containsValue(Object value) {
                check(value);
                mapOperationCounter.incrementOtherOperations();
                TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
                if (txn != null) {
                    if (txn.containsValue(name, value))
                        return true;
                }
                MContainsValue mContainsValue = concurrentMapManager.new MContainsValue(name, value);
                return mContainsValue.call();
            }

            public boolean isEmpty() {
                mapOperationCounter.incrementOtherOperations();
                MEmpty mempty = concurrentMapManager.new MEmpty();
                return mempty.isEmpty(name);
            }

            public void putAll(Map map) {
                Set<Entry> entries = map.entrySet();
                TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
                if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                    for (final Entry entry : entries) {
                        put(entry.getKey(), entry.getValue());
                    }
                } else {
                    concurrentMapManager.doPutAll(name, map);
                }
            }

            public boolean add(Object value) {
                check(value);
                Object old = putIfAbsent(value, toData(Boolean.TRUE));
                return old == null;
            }

            public boolean removeKey(Object key) {
                long begin = System.currentTimeMillis();
                check(key);
                MRemoveItem mRemoveItem = concurrentMapManager.new MRemoveItem();
                boolean result = mRemoveItem.removeItem(name, key);
                mapOperationCounter.incrementRemoves(System.currentTimeMillis() - begin);
                return result;
            }

            public void clear() {
                Set keys = keySet();
                for (Object key : keys) {
                    removeKey(key);
                }
            }

            public Set localKeySet() {
                return localKeySet(null);
            }

            public Set localKeySet(Predicate predicate) {
                mapOperationCounter.incrementOtherOperations();
                MIterateLocal miterate = concurrentMapManager.new MIterateLocal(name, predicate);
                return miterate.iterate();
            }

            public Set entrySet(Predicate predicate) {
                return (Set) iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_ENTRIES, predicate);
            }

            public Set keySet(Predicate predicate) {
                return (Set) iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS, predicate);
            }

            public Collection values(Predicate predicate) {
                return iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_VALUES, predicate);
            }

            public Set entrySet() {
                return (Set) iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_ENTRIES, null);
            }

            public Set keySet() {
                return (Set) iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS, null);
            }

            public Set allKeys() {
                return (Set) iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS_ALL, null);
            }

            public MapOperationsCounter getMapOperationCounter() {
                return mapOperationCounter;
            }

            public Collection values() {
                return iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_VALUES, null);
            }

            private Collection iterate(ClusterOperation iteratorType, Predicate predicate) {
                mapOperationCounter.incrementOtherOperations();
                MIterate miterate = concurrentMapManager.new MIterate(iteratorType, name, predicate);
                return (Collection) miterate.call();
            }

            public void destroy() {
                factory.destroyInstanceClusterWide(name, null);
            }

            public boolean evict(Object key) {
                mapOperationCounter.incrementOtherOperations();
                MEvict mevict = ThreadContext.get().getCallCache(factory).getMEvict();
                return mevict.evict(name, key);
            }
        }
    }

    public static class IdGeneratorProxy extends FactoryAwareNamedProxy implements IdGenerator, DataSerializable {

        private transient IdGenerator base = null;

        public IdGeneratorProxy() {
        }

        public IdGeneratorProxy(String name, FactoryImpl factory) {
            setName(name);
            setHazelcastInstance(factory);
            base = new IdGeneratorBase();
        }

        private void ensure() {
            factory.initialChecks();
            if (base == null) {
                base = (IdGenerator) factory.getOrCreateProxyByName(name);
            }
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        @Override
        public String toString() {
            return "IdGenerator [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IdGeneratorProxy that = (IdGeneratorProxy) o;
            return !(name != null ? !name.equals(that.name) : that.name != null);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public void destroy() {
            ensure();
            base.destroy();
        }

        public String getName() {
            ensure();
            return base.getName();
        }

        public long newId() {
            ensure();
            return base.newId();
        }

        private class IdGeneratorBase implements IdGenerator {

            private static final long MILLION = 1000L * 1000L;

            final AtomicLong million = new AtomicLong(-1);

            final AtomicLong currentId = new AtomicLong(2 * MILLION);

            public String getName() {
                return name.substring(Prefix.IDGEN.length());
            }

            public long newId() {
                long idAddition = currentId.incrementAndGet();
                if (idAddition >= MILLION) {
                    synchronized (IdGeneratorBase.this) {
                        idAddition = currentId.get();
                        if (idAddition >= MILLION) {
                            Long idMillion = getNewMillion();
                            long newMillion = idMillion * MILLION;
                            million.set(newMillion);
                            currentId.set(0L);
                        }
                        return newId();
                    }
                }
                long millionNow = million.get();
                return millionNow + idAddition;
            }

            private Long getNewMillion() {
                return factory.getAtomicNumber("__idGen" + name).incrementAndGet() - 1;
            }

            public InstanceType getInstanceType() {
                return InstanceType.ID_GENERATOR;
            }

            public void destroy() {
                currentId.set(2 * MILLION);
                synchronized (IdGeneratorBase.this) {
                    factory.destroyInstanceClusterWide(name, null);
                    factory.getAtomicNumber("__idGen" + name).destroy();
                    currentId.set(2 * MILLION);
                    million.set(-1);
                }
            }

            public Object getId() {
                return name;
            }
        }
    }
}
