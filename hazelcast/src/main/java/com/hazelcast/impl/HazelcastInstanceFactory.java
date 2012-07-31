/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.util.Util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;

@SuppressWarnings("SynchronizationOnStaticField")
public class HazelcastInstanceFactory {

    final static ConcurrentMap<String, HazelcastInstance> INSTANCE_MAP = new ConcurrentHashMap<String, HazelcastInstance>(5);

    private static final AtomicInteger factoryIdGen = new AtomicInteger();

    private static final Object INIT_LOCK = new Object();

    public static Set<HazelcastInstance> getAllHazelcastInstances() {
        return new HashSet<HazelcastInstance>(INSTANCE_MAP.values());
    }

    public static HazelcastInstance getHazelcastInstance(String instanceName) {
        return INSTANCE_MAP.get(instanceName);
    }

    public static HazelcastInstance newHazelcastInstance(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        String name = config.getInstanceName();
        if (name == null || name.trim().length() == 0) {
            name = "_hzInstance_" + factoryIdGen.incrementAndGet() + "_" + config.getGroupConfig().getName();
            return newHazelcastInstance(config, name);
        } else {
            synchronized (INIT_LOCK) {
                if (INSTANCE_MAP.containsKey(name)) {
                    throw new DuplicateInstanceNameException("HazelcastInstance with name '" + name + "' already exists!");
                }
                factoryIdGen.incrementAndGet();
                return newHazelcastInstance(config, name);
            }
        }
    }

    private static HazelcastInstance newHazelcastInstance(Config config, String instanceName) {
        HazelcastInstanceImpl hazelcastInstance = null;
        try {
            hazelcastInstance = new HazelcastInstanceImpl(instanceName, config);
            INSTANCE_MAP.put(instanceName, hazelcastInstance);
            final Node node = hazelcastInstance.node;
            boolean firstMember = (node.getClusterImpl().getMembers().iterator().next().localMember());
            int initialWaitSeconds = node.groupProperties.INITIAL_WAIT_SECONDS.getInteger();
            if (initialWaitSeconds > 0) {
                try {
                    Thread.sleep(initialWaitSeconds * 1000);
                    if (firstMember) {
                        node.partitionManager.firstArrangement();
                    } else {
                        Thread.sleep(4 * 1000);
                    }
                } catch (InterruptedException ignored) {
                }
            }
            int initialMinClusterSize = node.groupProperties.INITIAL_MIN_CLUSTER_SIZE.getInteger();
            while (node.getClusterImpl().getMembers().size() < initialMinClusterSize) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
            if (initialMinClusterSize > 0) {
                if (firstMember) {
                    node.partitionManager.firstArrangement();
                } else {
                    Thread.sleep(4 * 1000);
                }
                hazelcastInstance.logger.log(Level.INFO, "HazelcastInstance starting after waiting for cluster size of "
                        + initialMinClusterSize);
            }
            hazelcastInstance.lifecycleService.fireLifecycleEvent(STARTED);
            return hazelcastInstance;
        } catch (Throwable t) {
            if (hazelcastInstance != null) {
                hazelcastInstance.logger.log(Level.SEVERE, t.getMessage(), t);
            }
            Util.throwUncheckedException(t);
            return null;
        }
    }

    public static void shutdownAll() {
        Collection<HazelcastInstance> instances = INSTANCE_MAP.values();
        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().shutdown();
        }
        INSTANCE_MAP.clear();
        ManagementService.shutdown();
        ThreadContext.shutdownAll();
    }

    public static void remove(HazelcastInstance instance) {
        INSTANCE_MAP.remove(instance.getName());
        if (INSTANCE_MAP.size() == 0) {
            ManagementService.shutdown();
        }
    }

//    public HazelcastInstanceFactory(String name, Config config) throws Exception {
//        this.name = name;
//        lifecycleService = new LifecycleServiceImpl(HazelcastInstanceFactory.this);
//        registerConfigSerializers(config);
//        managedContext = new HazelcastManagedContext(this, config.getManagedContext());
//        node = new Node(this, config);
//        lifecycleService.fireLifecycleEvent(STARTING);
//        proxyFactory = node.initializer.getProxyFactory();
//        logger = node.getLogger(HazelcastInstanceFactory.class.getName());
//        hazelcastInstanceProxy = new HazelcastInstanceProxy(this);
//        locksMapProxy = proxyFactory.createMapProxy(Prefix.LOCKS_MAP_HAZELCAST);
//        node.start();
//        if (!node.isActive()) {
//            node.connectionManager.shutdown();
//            throw new IllegalStateException("Node failed to start!");
//        }
//
//        final Set<Member> members = node.getClusterImpl().getMembers();
//        if (members.size() > 1) {
//            Member target = null;
//            for (Member member : members) {
//                if (!member.isLiteMember() && !member.localMember()) {
//                    target = member;
//                    break;
//                }
//            }
//            if (target != null) {
////                DistributedTask task = new DistributedTask(new GetAllProxyKeysCallable(), target);
////                Future f = getExecutorService().submit(task);
////                try {
////                    final Set<ProxyKey> proxyKeys = (Set<ProxyKey>) f.get(10, TimeUnit.SECONDS);
////                    for (final ProxyKey proxyKey : proxyKeys) {
////                        if (!proxies.containsKey(proxyKey)) {
////                            node.clusterService.enqueueAndReturn(new Processable() {
////                                public void process() {
////                                    createProxy(proxyKey);
////                                }
////                            });
////                        }
////                    }
////                } catch (Exception e) {
////                    logger.log(Level.WARNING, e.getMessage(), e);
////                }
//            }
//        }
//        managementService = new ManagementService(this);
//        managementService.register();
//    }
//
//    @Override
//    public String toString() {
//        return "HazelcastInstance {name='" + name + "'}";
//    }
//
//    public Config getConfig() {
//        return node.getConfig();
//    }
//
//    public Collection<Instance> getInstances() {
//        return new ArrayList<Instance>(proxies.values());
//    }
//
//    public Collection<HazelcastInstanceAwareInstance> getProxies() {
//        initialChecks();
//        return proxies.values();
//    }
//
//    public ExecutorService getExecutorService() {
//        initialChecks();
//        return getExecutorService("default");
//    }
//
//    public ExecutorService getExecutorService(String name) {
//        if (name == null) throw new IllegalArgumentException("ExecutorService name cannot be null");
//        initialChecks();
//        name = Prefix.EXECUTOR_SERVICE + name;
//        ExecutorService executorServiceProxy = executorServiceProxies.get(name);
//        if (executorServiceProxy == null) {
////            executorServiceProxy = proxyFactory.createExecutorServiceProxy(name);
//            ExecutorService old = executorServiceProxies.putIfAbsent(name, executorServiceProxy);
//            if (old != null) {
//                executorServiceProxy = old;
//            }
//        }
//        return executorServiceProxy;
//    }
//
//    public ClusterImpl getCluster() {
//        initialChecks();
//        return node.getClusterImpl();
//    }
//
//    public IdGenerator getIdGenerator(String name) {
//        return (IdGenerator) getOrCreateProxyByName(Prefix.IDGEN + name);
//    }
//
//    public AtomicNumber getAtomicNumber(String name) {
//        return (AtomicNumber) getOrCreateProxyByName(Prefix.ATOMIC_NUMBER + name);
//    }
//
//    public ICountDownLatch getCountDownLatch(String name) {
//        return (ICountDownLatch) getOrCreateProxyByName(Prefix.COUNT_DOWN_LATCH + name);
//    }
//
//    public ISemaphore getSemaphore(String name) {
//        return (ISemaphore) getOrCreateProxyByName(Prefix.SEMAPHORE + name);
//    }
//
//    public Transaction getTransaction() {
//        initialChecks();
//        ThreadContext threadContext = ThreadContext.get();
//        threadContext.setCurrentInstance(this);
//        TransactionImpl txn = threadContext.getCallContext().getTransaction();
//        if (txn == null) {
//            txn = proxyFactory.createTransaction();
//            threadContext.getCallContext().setTransaction(txn);
//        }
//        return txn;
//    }
//
//    public PartitionService getPartitionService() {
//        return node.partitionManager.partitionServiceImpl;
//    }
//
//    public ClientService getClientService() {
////        return node.clientService;
//        return null;
//    }
//
//    public LoggingService getLoggingService() {
//        return node.loggingService;
//    }
//
//    public LifecycleService getLifecycleService() {
//        return lifecycleService;
//    }
//
//    public void restart() {
//        lifecycleService.restart();
//    }
//
//    public void shutdown() {
//        lifecycleService.shutdown();
//    }
//
//    public <K, V> IMap<K, V> getMap(String name) {
//        return (IMap<K, V>) getOrCreateProxyByName(Prefix.MAP + name);
//    }
//
//    public <E> IQueue<E> getQueue(String name) {
//        return (IQueue) getOrCreateProxyByName(Prefix.QUEUE + name);
//    }
//
//    public <E> ITopic<E> getTopic(String name) {
//        return (ITopic<E>) getOrCreateProxyByName(Prefix.TOPIC + name);
//    }
//
//    public <E> ISet<E> getSet(String name) {
//        return (ISet<E>) getOrCreateProxyByName(Prefix.SET + name);
//    }
//
//    public <E> IList<E> getList(String name) {
//        return (IList<E>) getOrCreateProxyByName(Prefix.AS_LIST + name);
//    }
//
//    public <K, V> MultiMap<K, V> getMultiMap(String name) {
//        return (MultiMap<K, V>) getOrCreateProxyByName(Prefix.MULTIMAP + name);
//    }
//
//    public ILock getLock(Object key) {
//        return (ILock) getOrCreateProxy(new ProxyKey("lock", key));
//    }
//
//    public Object getOrCreateProxyByName(final String name) {
//        Object proxy = proxiesByName.get(name);
//        if (proxy == null) {
//            proxy = getOrCreateProxy(new ProxyKey(name, null));
//        }
////        checkInitialization(proxy);
//        return proxy;
//    }
//
//    public Object getOrCreateProxy(final ProxyKey proxyKey) {
//        initialChecks();
//        Object proxy = proxies.get(proxyKey);
//        if (proxy == null) {
//            proxyFactory.checkProxy(proxyKey);
//            proxy = createInstanceClusterWide(proxyKey);
//        }
//        return proxy;
//    }
//
////    private void checkInitialization(Object proxy) {
////        if (proxy instanceof MProxy) {
////            MProxy mProxy = (MProxy) proxy;
////            CMap cmap = node.concurrentMapManager.getMap(mProxy.getLongName());
////            if (cmap == null) {
////                logger.log(Level.WARNING, "CMap[" + mProxy.getLongName() + "] has not been created yet! Initialization attempt failed!");
////                return;
////            }
////            if (!cmap.isMapForQueue() && cmap.notInitialized()) {
////                while (!node.concurrentMapManager.partitionServiceImpl.allPartitionsOwned()) {
////                    try {
////                        Thread.sleep(250);
////                        logger.log(Level.FINEST, "Waiting for all partitions to be owned...");
////                    } catch (InterruptedException e) {
////                        return;
////                    }
////                }
////                synchronized (cmap.getInitLock()) {
////                    if (cmap.notInitialized()) {
////                        final MapStoreConfig mapStoreConfig = cmap.getMapConfig().getMapStoreConfig();
////                        if (mapStoreConfig != null && mapStoreConfig.isEnabled()) {
////                            cmap.setInitState(InitializationState.INITIALIZING);
////                            try {
////                                ExecutorService es = getExecutorService();
////                                final Set<Member> members = new HashSet<Member>(getCluster().getMembers());
////                                members.remove(node.localMember);
////                                final MultiTask task = new MultiTask(new InitializeMap(mProxy.getName()), members);
////                                es.execute(task);
////                                if (cmap.loader != null) {
////                                    Set keys = cmap.loader.loadAllKeys();
////                                    if (keys != null) {
////                                        int count = 0;
////                                        Queue<Set> chunks = new LinkedList<Set>();
////                                        Set ownedKeys = new HashSet();
////                                        PartitionService partitionService = getPartitionService();
////                                        for (Object key : keys) {
////                                            Member owner = partitionService.getPartition(key).getOwner();
////                                            if (owner == null || owner.localMember()) {
////                                                ownedKeys.add(key);
////                                                count++;
////                                                if (ownedKeys.size() >= node.groupProperties.MAP_LOAD_CHUNK_SIZE.getInteger()) {
////                                                    chunks.add(ownedKeys);
////                                                    ownedKeys = new HashSet();
////                                                }
////                                            }
////                                        }
////                                        chunks.add(ownedKeys);
////                                        loadChunks(mProxy, cmap, chunks);
////                                        logger.log(Level.INFO, node.address + "[" + mProxy.getName() + "] loaded " + count + " in total.");
////                                    }
////                                }
////                                task.get();
////                            } catch (Throwable e) {
////                                if (node.isActive()) {
////                                    logger.log(Level.SEVERE, e.getMessage(), e);
////                                }
////                            }
////                        }
////                    }
////                    cmap.setInitState(InitializationState.INITIALIZED);
////                }
////            }
////        }
////    }
////
////    private void loadChunks(final MProxy mProxy, final CMap cmap, final Queue<Set> chunks) throws InterruptedException {
////        if (chunks.size() > 0) {
////            ParallelExecutor es = node.executorManager.getMapLoaderExecutorService();
////            final CountDownLatch latch = new CountDownLatch(chunks.size());
////            for (final Set chunk : chunks) {
////                es.execute(new Runnable() {
////                    public void run() {
////                        if (chunk == null) return;
////                        try {
////                            loadKeys(mProxy, cmap, chunk);
////                        } catch (Exception e) {
////                            logger.log(Level.SEVERE, "Initial loading failed.", e);
////                        } finally {
////                            latch.countDown();
////                        }
////                    }
////                });
////            }
////            latch.await();
////        }
////    }
////
////    private void loadKeys(MProxy mProxy, CMap cmap, Set keys) {
////        if (keys.size() > 0) {
////            Map map = cmap.loader.loadAll(keys);
////            if (map != null && map.size() > 0) {
////                Set<Map.Entry> entries = map.entrySet();
////                for (Map.Entry entry : entries) {
////                    mProxy.putTransient(entry.getKey(), entry.getValue(), 0, null);
////                }
////            }
////        }
////    }
////
////    public static class InitializeMap implements Callable<Boolean>, DataSerializable, HazelcastInstanceAware {
////        String name;
////        private transient FactoryImpl factory = null;
////
////        public InitializeMap(String name) {
////            this.name = name;
////        }
////
////        public InitializeMap() {
////        }
////
////        public Boolean call() throws Exception {
////            factory.getMap(name).getName();
////            return Boolean.TRUE;
////        }
////
////        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
////            this.factory = (FactoryImpl) hazelcastInstance;
////        }
////
////        public void writeData(DataOutput out) throws IOException {
////            out.writeUTF(name);
////        }
////
////        public void readData(DataInput in) throws IOException {
////            name = in.readUTF();
////        }
////    }
//
//    public void initialChecks() {
//        ThreadContext.get().setCurrentInstance(HazelcastInstanceFactory.this);
//        while (node.isActive() && lifecycleService.paused.get()) {
//            try {
//                //noinspection BusyWait
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                return;
//            }
//        }
//        if (!node.isActive()) throw new IllegalStateException("Hazelcast Instance is not active!");
//    }
//
//    public void destroyProxy(final ProxyKey proxyKey) {
//        proxiesByName.remove(proxyKey.name);
//        Instance proxy = proxies.remove(proxyKey);
//        if (proxy != null) {
//            String name = proxyKey.name;
//            if (name.startsWith(Prefix.QUEUE)) {
////                node.blockingQueueManager.destroy(name);
//            } else if (name.startsWith(Prefix.MAP)) {
////                node.concurrentMapManager.destroy(name);
//            } else if (name.startsWith(Prefix.MAP_BASED)) {
////                node.concurrentMapManager.destroy(name);
//            } else if (name.startsWith(Prefix.TOPIC)) {
////                node.topicManager.destroy(name);
//            }
//            logger.log(Level.FINEST, "Instance destroyed " + proxyKey);
//            fireInstanceDestroyEvent(proxy);
//        }
//    }
//
//    // should only be called from service thread!!
//    public Object createProxy(ProxyKey proxyKey) {
////        node.clusterManager.checkServiceThread();
//        boolean created = false;
//        Instance proxy = proxies.get(proxyKey);
//        if (proxy == null) {
//            created = true;
//            String name = proxyKey.name;
//            if (name.startsWith(Prefix.QUEUE)) {
////                proxy = proxyFactory.createQueueProxy(name);
////                node.blockingQueueManager.getOrCreateBQ(name);
//            } else if (name.startsWith(Prefix.TOPIC)) {
////                proxy = proxyFactory.createTopicProxy(name);
////                node.topicManager.getTopicInstance(name);
//            } else if (name.startsWith(Prefix.MAP)) {
//                proxy = proxyFactory.createMapProxy(name);
////                node.concurrentMapManager.getOrCreateMap(name);
////            } else if (name.startsWith(Prefix.AS_LIST)) {
////                proxy = proxyFactory.createListProxy(name);
//            } else if (name.startsWith(Prefix.MULTIMAP)) {
////                proxy = proxyFactory.createMultiMapProxy(name);
////                node.concurrentMapManager.getOrCreateMap(name);
////            } else if (name.startsWith(Prefix.SET)) {
////                proxy = proxyFactory.createSetProxy(name);
////            } else if (name.startsWith(Prefix.ATOMIC_NUMBER)) {
////                proxy = proxyFactory.createAtomicNumberProxy(name);
////            } else if (name.startsWith(Prefix.IDGEN)) {
////                proxy = proxyFactory.createIdGeneratorProxy(name);
////            } else if (name.startsWith(Prefix.SEMAPHORE)) {
////                proxy = proxyFactory.createSemaphoreProxy(name);
////            } else if (name.startsWith(Prefix.COUNT_DOWN_LATCH)) {
////                proxy = proxyFactory.createCountDownLatchProxy(name);
////            } else if (name.equals("lock")) {
////                proxy = proxyFactory.createLockProxy(proxyKey.key);
//            }
////            final HazelcastInstanceAwareInstance anotherProxy = proxies.putIfAbsent(proxyKey, proxy);
////            if (anotherProxy != null) {
////                created = false;
////                proxy = anotherProxy;
////            }
////            if (proxyKey.key == null) {
////                proxiesByName.put(proxyKey.name, proxy);
////            }
//        }
//        if (created) {
//            logger.log(Level.FINEST, "Instance created " + proxyKey);
//            fireInstanceCreateEvent(proxy);
//        }
//        return proxy;
//    }
//
//    public void addInstanceListener(InstanceListener instanceListener) {
//        lsInstanceListeners.add(instanceListener);
//    }
//
//    public void removeInstanceListener(InstanceListener instanceListener) {
//        lsInstanceListeners.remove(instanceListener);
//    }
//
//    public void registerFallbackSerializer(final TypeSerializer serializer) {
//        serializerRegistry.registerFallback(serializer);
//    }
//
//    public void registerSerializer(final TypeSerializer serializer, final Class type) {
//        serializerRegistry.register(serializer, type);
//    }
//
//    private void registerConfigSerializers(Config config) throws Exception {
//        final Collection<SerializerConfig> serializerConfigs = config.getSerializerConfigs();
//        if (serializerConfigs != null) {
//            for (SerializerConfig serializerConfig : serializerConfigs) {
//                TypeSerializer factory = serializerConfig.getImplementation();
//                if (factory == null) {
//                    factory = (TypeSerializer) ClassLoaderUtil.newInstance(serializerConfig.getClassName());
//                }
//                if (serializerConfig.isGlobal()) {
//                    serializerRegistry.registerFallback(factory);
//                } else {
//                    Class typeClass = serializerConfig.getTypeClass();
//                    if (typeClass == null) {
//                        typeClass = ClassLoaderUtil.loadClass(serializerConfig.getTypeClassName()) ;
//                    }
//                    serializerRegistry.register(factory, typeClass);
//                }
//            }
//        }
//    }
//
//    void fireInstanceCreateEvent(Instance instance) {
//        if (lsInstanceListeners.size() > 0) {
//            final InstanceEvent instanceEvent = new InstanceEvent(InstanceEvent.InstanceEventType.CREATED, instance);
//            for (final InstanceListener instanceListener : lsInstanceListeners) {
//                node.nodeService.getExecutorService().execute(new Runnable() {
//                    public void run() {
//                        instanceListener.instanceCreated(instanceEvent);
//                    }
//                });
//            }
//        }
//    }
//
//    void fireInstanceDestroyEvent(Instance instance) {
//        if (lsInstanceListeners.size() > 0) {
//            final InstanceEvent instanceEvent = new InstanceEvent(InstanceEvent.InstanceEventType.DESTROYED, instance);
//            for (final InstanceListener instanceListener : lsInstanceListeners) {
//                node.nodeService.getExecutorService().execute(new Runnable() {
//                    public void run() {
//                        instanceListener.instanceDestroyed(instanceEvent);
//                    }
//                });
//            }
//        }
//    }
//
//    Object createInstanceClusterWide(final ProxyKey proxyKey) {
////        final BlockingQueue<Object> result = ResponseQueueFactory.newResponseQueue();
////        node.clusterService.enqueueAndWait(new Processable() {
////            public void process() {
////                try {
////                    result.put(createProxy(proxyKey));
////                } catch (InterruptedException ignored) {
////                }
////            }
////        }, 10);
////        Object proxy = null;
////        try {
////            proxy = result.take();
////        } catch (InterruptedException e) {
////        }
////        node.clusterManager.sendProcessableToAll(new CreateOrDestroyInstanceProxy(proxyKey, true), false);
////        return proxy;
//        return createProxy(proxyKey);
//    }
//
//    void destroyInstanceClusterWide(String name, Object key) {
//        final ProxyKey proxyKey = new ProxyKey(name, key);
//        if (proxies.containsKey(proxyKey)) {
//            if (name.equals("lock")) {
//                locksMapProxy.remove(key);
//            }
////            node.clusterImpl.sendProcessableToAll(new CreateOrDestroyInstanceProxy(proxyKey, false), true);
//        } else {
//            logger.log(Level.WARNING, "Destroying unknown instance name: " + name);
//        }
//    }
//
//    public static class CreateOrDestroyInstanceProxy extends AbstractRemotelyProcessable {
//        private ProxyKey proxyKey;
//        private boolean create;
//
//        public CreateOrDestroyInstanceProxy() {
//        }
//
//        public CreateOrDestroyInstanceProxy(final ProxyKey proxyKey, final boolean create) {
//            this.proxyKey = proxyKey;
//            this.create = create;
//        }
//
//        public void process() {
//            if (create) {
//                node.instance.createProxy(proxyKey);
//            } else {
//                node.instance.destroyInstance(proxyKey);
//            }
//        }
//
//        public void readData(final DataInput in) throws IOException {
//            super.readData(in);
//            create = in.readBoolean();
//            proxyKey = new ProxyKey();
//            proxyKey.readData(in);
//        }
//
//        public void writeData(final DataOutput out) throws IOException {
//            super.writeData(out);
//            out.writeBoolean(create);
//            proxyKey.writeData(out);
//        }
//    }
//
    public static class ProxyKey  {
        String name;
        Object key;

        public ProxyKey() {
        }

        public ProxyKey(String name, Object key) {
            this.name = name;
            this.key = key;
        }

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
//
//    public static class GetAllProxyKeysCallable extends HazelcastInstanceAwareObject
//            implements Callable<Set<ProxyKey>>, DataSerializable {
//        public Set<ProxyKey> call() throws Exception {
//            final HazelcastInstanceFactory factory = (HazelcastInstanceFactory) hazelcastInstance;
//            return new HashSet<ProxyKey>(factory.proxies.keySet());
//        }
//
//        public void readData(final DataInput in) throws IOException {
//        }
//
//        public void writeData(final DataOutput out) throws IOException {
//        }
//    }
}
