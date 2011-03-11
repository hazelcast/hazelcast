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
import com.hazelcast.impl.management.ManagementConsoleService;
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.SerializationHelper;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.util.ResponseQueueFactory;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;

public class FactoryImpl implements HazelcastInstance {

    final static ConcurrentMap<String, FactoryImpl> factories = new ConcurrentHashMap<String, FactoryImpl>(5);

    final static Object factoryLock = new Object();

    final static String ATOMIC_NUMBER_MAP_NAME = "c:hz_AtomicNumber";

    private static int nextFactoryId = 0;

    private final ConcurrentMap<String, HazelcastInstanceAwareInstance> proxiesByName = new ConcurrentHashMap<String, HazelcastInstanceAwareInstance>(1000);

    private final ConcurrentMap<ProxyKey, HazelcastInstanceAwareInstance> proxies = new ConcurrentHashMap<ProxyKey, HazelcastInstanceAwareInstance>(1000);

    private final MProxy locksMapProxy;

    private final MProxy idGeneratorMapProxy;

    private final MProxyImpl globalProxies;

    private final TopicProxyImpl memberStatsTopicProxy;

    private final MultiMapProxy memberStatsMultimapProxy;

    private final ConcurrentMap<String, ExecutorServiceProxy> executorServiceProxies = new ConcurrentHashMap<String, ExecutorServiceProxy>(2);

    private final CopyOnWriteArrayList<InstanceListener> lsInstanceListeners = new CopyOnWriteArrayList<InstanceListener>();

    private final String name;

    private final TransactionFactory transactionFactory;

    private final HazelcastInstanceProxy hazelcastInstanceProxy;

    public final Node node;

    volatile boolean restarted = false;

    private final ManagementService managementService;

    private final MemberStatePublisher memberStatePublisher;

    private final ILogger logger;

    final LifecycleServiceImpl lifecycleService;

    final ManagementConsoleService managementConsoleService;

    public static HazelcastInstanceProxy newHazelcastInstanceProxy(Config config) {
        FactoryImpl factory = null;
        try {
            synchronized (factoryLock) {
                if (config == null) {
                    config = new XmlConfigBuilder().build();
                }
                String name = "_hzInstance_" + nextFactoryId++ + "_" + config.getGroupConfig().getName();
                factory = new FactoryImpl(name, config);
                FactoryImpl old = factories.put(name, factory);
                if (old != null) {
                    factory.logger.log(Level.SEVERE, "HazelcastInstance with [" + name + "] already exist!");
                    throw new RuntimeException();
                } else {
                }
            }
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
        synchronized (factoryLock) {
            Collection<FactoryImpl> colFactories = factories.values();
            for (FactoryImpl factory : colFactories) {
                factory.shutdown();
            }
            factories.clear();
            shutdownManagementService();
            ThreadContext.shutdownAll();
        }
    }

    public static void shutdown(HazelcastInstanceProxy hazelcastInstanceProxy) {
        synchronized (factoryLock) {
            FactoryImpl factory = hazelcastInstanceProxy.getFactory();
            try {
                /**
                 * if JMX service cannot unregister
                 * just printStackTrace and continue
                 * shutting down.
                 *
                 */
                factory.managementService.unregister();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            factory.proxies.clear();
            factory.node.shutdown();
            factories.remove(factory.getName());
            if (factories.size() == 0) {
                shutdownManagementService();
            }
        }
    }

    private static void shutdownManagementService() {
        try {
            ManagementService.shutdown();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public FactoryImpl(String name, Config config) {
        this.name = name;
        node = new Node(this, config);
        globalProxies = new MProxyImpl(Prefix.MAP + "__hz_Proxies", this);
        logger = node.getLogger(FactoryImpl.class.getName());
        lifecycleService = new LifecycleServiceImpl(FactoryImpl.this);
        transactionFactory = new TransactionFactory(this);
        hazelcastInstanceProxy = new HazelcastInstanceProxy(this);
        locksMapProxy = new MProxyImpl(Prefix.MAP + "__hz_Locks", this);
        idGeneratorMapProxy = new MProxyImpl(Prefix.MAP + "__hz_IdGenerator", this);
        memberStatsMultimapProxy = new MultiMapProxy(Prefix.MULTIMAP + MemberStatePublisher.STATS_MULTIMAP_NAME, this);
        memberStatsTopicProxy = new TopicProxyImpl(Prefix.TOPIC + MemberStatePublisher.STATS_TOPIC_NAME, this);
        lifecycleService.fireLifecycleEvent(STARTING);
        node.start();
        memberStatePublisher = new MemberStatePublisher(memberStatsTopicProxy, memberStatsMultimapProxy, node);
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
        ManagementConsoleService managementConsoleServiceTmp = null;
        try {
            managementConsoleServiceTmp = new ManagementConsoleService(FactoryImpl.this);
        } catch (Exception e) {
            logger.log(Level.FINEST, e.getMessage(), e);
        }
        managementConsoleService = managementConsoleServiceTmp;
    }

    public Set<String> getLongInstanceNames() {
        return proxiesByName.keySet();
    }

    public MemberStateImpl createMemberState() {
        final MemberStateImpl memberState = new MemberStateImpl();
        createMemberState(memberState);
        return memberState;
    }

    public void createMemberState(MemberStateImpl memberStats) {
        memberStats.setAddress(((MemberImpl) node.getClusterImpl().getLocalMember()).getAddress());
        memberStats.getMemberHealthStats().setOutOfMemory(node.isOutOfMemory());
        memberStats.getMemberHealthStats().setActive(node.isActive());
        memberStats.getMemberHealthStats().setServiceThreadStats(node.getCpuUtilization().serviceThread);
        memberStats.getMemberHealthStats().setOutThreadStats(node.getCpuUtilization().outThread);
        memberStats.getMemberHealthStats().setInThreadStats(node.getCpuUtilization().inThread);
        PartitionService partitionService = getPartitionService();
        Set<Partition> partitions = partitionService.getPartitions();
        memberStats.clearPartitions();
        for (Partition partition : partitions) {
            if (partition.getOwner() != null && partition.getOwner().localMember()) {
                memberStats.addPartition(partition.getPartitionId());
            }
        }
        Collection<HazelcastInstanceAwareInstance> proxyObjects = proxies.values();
        for (HazelcastInstanceAwareInstance proxyObject : proxyObjects) {
            if (proxyObject.getInstanceType() == Instance.InstanceType.MAP) {
                MProxy mapProxy = (MProxy) proxyObject;
                memberStats.putLocalMapStats(mapProxy.getName(), (LocalMapStatsImpl) mapProxy.getLocalMapStats());
            } else if (proxyObject.getInstanceType() == Instance.InstanceType.QUEUE) {
                QProxy qProxy = (QProxy) proxyObject;
                memberStats.putLocalQueueStats(qProxy.getName(), (LocalQueueStatsImpl) qProxy.getLocalQueueStats());
            } else if (proxyObject.getInstanceType() == Instance.InstanceType.TOPIC) {
                TopicProxy topicProxy = (TopicProxy) proxyObject;
                memberStats.putLocalTopicStats(topicProxy.getName(), (LocalTopicStatsImpl) topicProxy.getLocalTopicStats());
            }
        }
    }

    /**
     * Returns milliseconds by taking care of
     * the overflow issues.
     * TimeUnit.SECONDS.toMillis(Long.MAX_VALUE) would be negative
     * for example. -1 means infinite.
     *
     * @param time
     * @param unit
     * @return
     */
    public static long toMillis(long time, TimeUnit unit) {
        if (time == 0) {
            return 0;
        } else if (time < 0) {
            return -1;
        } else {
            long millis = unit.toMillis(time);
            if (millis < 1) {
                millis = -1;
            }
            return millis;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FactoryImpl factory = (FactoryImpl) o;
        if (!name.equals(factory.name)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
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

    public Transaction getTransaction() {
        initialChecks();
        ThreadContext threadContext = ThreadContext.get();
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
        if (managementConsoleService != null) {
            managementConsoleService.shutdown();
        }
        lifecycleService.shutdown();
        for (ExecutorServiceProxy esp : executorServiceProxies.values()) {
            esp.shutdown();
        }
    }

    public <K, V> IMap<K, V> getMap(String name) {
        return (IMap<K, V>) getOrCreateProxyByName(Prefix.MAP + name);
    }

    public <E> IQueue<E> getQueue(String name) {
        return (IQueue) getOrCreateProxyByName(Prefix.QUEUE + name);
    }

    public <E> ITopic<E> getTopic(String name) {
        return (ITopic) getOrCreateProxyByName(Prefix.TOPIC + name);
    }

    public <E> ISet<E> getSet(String name) {
        return (ISet) getOrCreateProxyByName(Prefix.SET + name);
    }

    public <E> IList<E> getList(String name) {
        return (IList) getOrCreateProxyByName(Prefix.LIST + name);
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

    public void initialChecks() {
//        while (Runtime.getRuntime().freeMemory() < 10000000) {
//            try {
//                logger.log(Level.SEVERE, "Not Enough Memory");
//                Thread.sleep(2000);
//            } catch (InterruptedException e) {
//                return;
//            }
//        }
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
            } else if (name.startsWith(Prefix.MAP_BASED)) {
                if (BaseManager.getInstanceType(name) == Instance.InstanceType.MULTIMAP) {
                    proxy = new MultiMapProxy(name, this);
                } else {
                    proxy = new CollectionProxyImpl(name, this);
                }
            } else if (name.startsWith(Prefix.ATOMIC_NUMBER)) {
                proxy = new AtomicNumberImpl(name, this);
            } else if (name.startsWith(Prefix.IDGEN)) {
                proxy = new IdGeneratorProxy(name, this);
            } else if (name.equals("lock")) {
                proxy = new LockProxy(this, proxyKey.key);
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

    public static class LockProxy extends SerializationHelper implements HazelcastInstanceAwareInstance, ILock, DataSerializable {

        private Object key = null;
        private transient ILock base = null;
        private transient FactoryImpl factory = null;

        public LockProxy() {
        }

        public LockProxy(HazelcastInstance hazelcastInstance, Object key) {
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
                base = factory.getLock(key);
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
            LockProxy lockProxy = (LockProxy) o;
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

        private class LockProxyBase implements ILock {
            public void lock() {
                factory.locksMapProxy.lock(key);
            }

            public void lockInterruptibly() throws InterruptedException {
            }

            public Condition newCondition() {
                return null;
            }

            public boolean tryLock() {
                return factory.locksMapProxy.tryLock(key);
            }

            public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
                try {
                    return factory.locksMapProxy.tryLock(key, time, unit);
                } catch (RuntimeInterruptedException e) {
                    throw new InterruptedException();
                }
            }

            public void unlock() {
                factory.locksMapProxy.unlock(key);
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
            ProxyKey proxyKey = (ProxyKey) o;
            if (name != null ? !name.equals(proxyKey.name) : proxyKey.name != null) return false;
            return !(key != null ? !key.equals(proxyKey.key) : proxyKey.key != null);
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

            public Instance.InstanceType getInstanceType() {
                return Instance.InstanceType.TOPIC;
            }

            public String getName() {
                return name.substring(Prefix.TOPIC.length());
            }

            public Object getId() {
                return name;
            }

            public LocalTopicStats getLocalTopicStats() {
                LocalTopicStatsImpl localTopicStats = topicManager.getTopicInstance(name).getTopicSats();
                localTopicStats.setOperationsStats(topicOperationsCounter.getPublishedStats());
                return localTopicStats;
            }

            public TopicOperationsCounter getTopicOperationCounter() {
                return topicOperationsCounter;
            }
        }
    }

    public static class CollectionProxyImpl extends BaseCollection implements CollectionProxy, HazelcastInstanceAwareInstance, DataSerializable {
        String name = null;
        private transient CollectionProxy base = null;
        private transient FactoryImpl factory = null;

        public CollectionProxyImpl() {
        }

        public CollectionProxyImpl(String name, FactoryImpl factory) {
            this.name = name;
            this.factory = factory;
            this.base = new CollectionProxyReal();
        }

        public FactoryImpl getFactory() {
            return factory;
        }

        public CollectionProxy getBase() {
            return base;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.factory = (FactoryImpl) hazelcastInstance;
        }

        private void ensure() {
            factory.initialChecks();
            if (base == null) {
                base = (CollectionProxy) factory.getOrCreateProxyByName(name);
            }
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        @Override
        public String toString() {
            ensure();
            if (getInstanceType() == InstanceType.SET) {
                return "Set [" + getName() + "]";
            } else {
                return "List [" + getName() + "]";
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CollectionProxyImpl that = (CollectionProxyImpl) o;
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

        public boolean removeKey(Object key) {
            ensure();
            return base.removeKey(key);
        }

        class CollectionProxyReal extends BaseCollection implements CollectionProxy {

            final MProxy mapProxy;

            public CollectionProxyReal() {
                mapProxy = new MProxyImpl(name, factory);
            }

            public Object getId() {
                return name;
            }

            @Override
            public boolean equals(Object o) {
                return CollectionProxyImpl.this.equals(o);
            }

            @Override
            public int hashCode() {
                return CollectionProxyImpl.this.hashCode();
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
                return name.substring(Prefix.LIST.length());
            }

            @Override
            public boolean add(Object obj) {
                return mapProxy.add(obj);
            }

            @Override
            public boolean remove(Object obj) {
                return mapProxy.removeKey(obj);
            }

            public boolean removeKey(Object obj) {
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

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.factory = (FactoryImpl) hazelcastInstance;
            this.blockingQueueManager = factory.node.blockingQueueManager;
            this.listenerManager = factory.node.listenerManager;
        }

        private void ensure() {
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
            }

            public Instance.InstanceType getInstanceType() {
                return Instance.InstanceType.QUEUE;
            }

            public Object getId() {
                return name;
            }

            public QueueOperationsCounter getQueueOperationCounter() {
                return operationsCounter;
            }
        }
    }

    public static class MultiMapProxy extends FactoryAwareNamedProxy implements MultiMap, DataSerializable, IGetAwareProxy {

        private transient MultiMap base = null;

        public MultiMapProxy() {
        }

        public MultiMapProxy(String name, FactoryImpl factory) {
            setName(name);
            setHazelcastInstance(factory);
            this.base = new MultiMapBase();
        }

        private void ensure() {
            factory.initialChecks();
            if (base == null) {
                base = (MultiMap) factory.getOrCreateProxyByName(name);
            }
        }

        public MultiMapBase getBase() {
            return (MultiMapBase) base;
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
            MultiMapProxy that = (MultiMapProxy) o;
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
            Instance instance = factory.proxies.remove(name);
            if (instance != null) {
                ensure();
                base.destroy();
            }
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

        class MultiMapBase implements MultiMap, IGetAwareProxy {
            final MProxy mapProxy;

            private MultiMapBase() {
                mapProxy = new MProxyImpl(name, factory);
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
                    if (e instanceof RuntimeException) {
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
            final Data dataKey = IOUtil.toData(key);
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
            final Data dataKey = IOUtil.toData(key);
            final Data dataValue = IOUtil.toData(value);
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
            final Data dataKey = IOUtil.toData(key);
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

        public Instance.InstanceType getInstanceType() {
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
                check(key);
                mapOperationCounter.incrementRemoves();
                MRemove mremove = ThreadContext.get().getCallCache(factory).getMRemove();
                return mremove.remove(name, key, -1);
            }

            public Object tryRemove(Object key, long timeout, TimeUnit timeunit) throws TimeoutException {
                check(key);
                mapOperationCounter.incrementRemoves();
                MRemove mremove = ThreadContext.get().getCallCache(factory).getMRemove();
                return mremove.tryRemove(name, key, toMillis(timeout, timeunit));
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
                check(key);
                check(value);
                mapOperationCounter.incrementRemoves();
                MRemove mremove = concurrentMapManager.new MRemove();
                return (mremove.removeIfSame(name, key, value, -1) != null);
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
                    final ExecutorService es = Executors.newFixedThreadPool(10);
                    final CountDownLatch latch = new CountDownLatch(entries.size());
                    final List<Throwable> throwables = new CopyOnWriteArrayList<Throwable>();
                    for (final Entry entry : entries) {
                        es.execute(new Runnable() {
                            public void run() {
                                try {
                                    put(entry.getKey(), entry.getValue());
                                } catch (Throwable e) {
                                    throwables.add(e);
                                } finally {
                                    latch.countDown();
                                }
                            }
                        });
                    }
                    try {
                        latch.await();
                        es.shutdown();
                    } catch (InterruptedException ignored) {
                    }
                    if (!throwables.isEmpty()) {
                        final Throwable throwable = throwables.get(0);
                        throw new RuntimeException(throwable.getMessage(), throwable);
                    }
                }
            }

            public boolean add(Object value) {
                check(value);
                MAdd madd = concurrentMapManager.new MAdd();
                InstanceType type = concurrentMapManager.getInstanceType(name);
                if (type == InstanceType.LIST) {
                    return madd.addToList(name, value);
                } else {
                    return madd.addToSet(name, value);
                }
            }

            public boolean removeKey(Object key) {
                check(key);
                mapOperationCounter.incrementRemoves();
                MRemoveItem mRemoveItem = concurrentMapManager.new MRemoveItem();
                return mRemoveItem.removeItem(name, key);
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

        public Instance.InstanceType getInstanceType() {
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
                    synchronized (this) {
                        try {
                            idAddition = currentId.get();
                            if (idAddition >= MILLION) {
                                Long idMillion = getNewMillion();
                                long newMillion = idMillion * MILLION;
                                million.set(newMillion);
                                currentId.set(0L);
                            }
                            return newId();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                }
                long millionNow = million.get();
                return millionNow + idAddition;
            }

            private Long getNewMillion() {
                try {
                    DistributedTask<Long> task = new DistributedTask<Long>(new IncrementTask(name, factory), name);
                    factory.getExecutorService("default").execute(task);
                    return task.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

            public InstanceType getInstanceType() {
                return InstanceType.ID_GENERATOR;
            }

            public void destroy() {
                factory.destroyInstanceClusterWide(name, null);
            }

            public Object getId() {
                return name;
            }
        }
    }

    public static class IncrementTask implements Callable<Long>, DataSerializable, HazelcastInstanceAware {
        String name = null;
        transient HazelcastInstance hazelcastInstance = null;

        public IncrementTask() {
            super();
        }

        public IncrementTask(String uuidName, HazelcastInstance hazelcastInstance) {
            super();
            this.name = uuidName;
            this.hazelcastInstance = hazelcastInstance;
        }

        public Long call() {
            MProxy map = ((FactoryImpl) hazelcastInstance).idGeneratorMapProxy;
            map.lock(name);
            try {
                Long max = (Long) map.get(name);
                if (max == null) {
                    max = 0L;
                    map.put(name, 0L);
                    return max;
                } else {
                    Long newMax = max + 1;
                    map.put(name, newMax);
                    return newMax;
                }
            } finally {
                map.unlock(name);
            }
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(name);
        }

        public void readData(DataInput in) throws IOException {
            this.name = in.readUTF();
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }
}
