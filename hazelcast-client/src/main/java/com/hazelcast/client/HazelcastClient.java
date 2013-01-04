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

package com.hazelcast.client;

import com.hazelcast.client.impl.ListenerManager;
import com.hazelcast.client.util.TransactionUtil;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.serialization.SerializerRegistry;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.core.PartitionService;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceProxy;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;

/**
 * Hazelcast Client enables you to do all Hazelcast operations without
 * being a member of the cluster. It connects to one of the
 * cluster members and delegates all cluster wide operations to it.
 * When the connected cluster member dies, client will
 * automatically switch to another live member.
 */
public class HazelcastClient implements HazelcastInstance {

    private final static ILogger logger = Logger.getLogger(HazelcastClient.class.getName());

    private final static AtomicInteger clientIdCounter = new AtomicInteger();

    private final static List<HazelcastClient> lsClients = new CopyOnWriteArrayList<HazelcastClient>();

    private final int id;
    private final ClientConfig config;
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final Map<Long, Call> calls = new ConcurrentHashMap<Long, Call>(100);
    private final ListenerManager listenerManager;
    private final OutRunnable out;
    private final InRunnable in;
    private final Map<Object, Object> mapProxies = new ConcurrentHashMap<Object, Object>(100);
    private final ConcurrentMap<String, ExecutorServiceClientProxy> mapExecutors = new ConcurrentHashMap<String, ExecutorServiceClientProxy>(2);
    private final ClusterClientProxy clusterClientProxy;
    private final PartitionClientProxy partitionClientProxy;
    private final LifecycleServiceClientImpl lifecycleService;
    private final ConnectionManager connectionManager;
    private final SerializerRegistry serializerRegistry = new SerializerRegistry();

    private HazelcastClient(ClientConfig config) {
        if (config.getAddressList().size() == 0) {
            config.addAddress("localhost");
        }
        if (config.getCredentials() == null) {
            config.setCredentials(new UsernamePasswordCredentials(config.getGroupConfig().getName(),
                    config.getGroupConfig().getPassword()));
        }
        this.config = config;
        this.id = clientIdCounter.incrementAndGet();
        lifecycleService = new LifecycleServiceClientImpl(this);
        lifecycleService.fireLifecycleEvent(STARTING);
        //empty check
        connectionManager = new ConnectionManager(this, config, lifecycleService);
        connectionManager.setBinder(new DefaultClientBinder(this));
        out = new OutRunnable(this, calls, new ProtocolWriter());
        in = new InRunnable(this, out, calls, new ProtocolReader());
        listenerManager = new ListenerManager(this, serializerRegistry);

        try {
            final Connection c = connectionManager.getInitConnection();
            if (c == null) {
                connectionManager.shutdown();
                lifecycleService.destroy();
                throw new IllegalStateException("Unable to connect to cluster");
            }
        } catch (IOException e) {
            connectionManager.shutdown();
            lifecycleService.destroy();
            throw new ClusterClientException(e.getMessage(), e);
        }
        final String prefix = "hz.client." + this.id + ".";
        new Thread(out, prefix + "OutThread").start();
        new Thread(in, prefix + "InThread").start();
        new Thread(listenerManager, prefix + "Listener").start();
        clusterClientProxy = new ClusterClientProxy(this);
        partitionClientProxy = new PartitionClientProxy(this);
        if (config.isUpdateAutomatic()) {
            this.getCluster().addMembershipListener(connectionManager);
            connectionManager.updateMembers();
        }
        lifecycleService.fireLifecycleEvent(STARTED);
        connectionManager.scheduleHeartbeatTimerTask();
        lsClients.add(HazelcastClient.this);
    }

    GroupConfig groupConfig() {
        return config.getGroupConfig();
    }

    public InRunnable getInRunnable() {
        return in;
    }

    public OutRunnable getOutRunnable() {
        return out;
    }

    ListenerManager getListenerManager() {
        return listenerManager;
    }

    /**
     * @param config
     * @return
     */

    public static HazelcastClient newHazelcastClient(ClientConfig config) {
        if (config == null)
            config = new ClientConfig();
        return new HazelcastClient(config);
    }

    public Config getConfig() {
        throw new UnsupportedOperationException();
    }

    public PartitionService getPartitionService() {
        return partitionClientProxy;
    }

    public ClientService getClientService() {
        return null;
    }

    public LoggingService getLoggingService() {
        throw new UnsupportedOperationException();
    }

    public <K, V> IMap<K, V> getMap(String name) {
        return (IMap<K, V>) getClientProxy(Prefix.MAP + name);
    }

    public <K, V, E> Object getClientProxy(Object o) {
        Object proxy = mapProxies.get(o);
        if (proxy == null) {
            synchronized (mapProxies) {
                proxy = mapProxies.get(o);
                if (proxy == null) {
                    if (o instanceof String) {
                        String name = (String) o;
                        if (name.startsWith(Prefix.MAP)) {
                            proxy = new MapClientProxy<K, V>(this, name);
                        } else if (name.startsWith(Prefix.AS_LIST)) {
                            proxy = new ListClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.SET)) {
                            proxy = new SetClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.QUEUE)) {
                            proxy = new QueueClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.TOPIC)) {
                            proxy = new TopicClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.ATOMIC_NUMBER)) {
                            proxy = new AtomicNumberClientProxy(this, name);
                        } else if (name.startsWith(Prefix.COUNT_DOWN_LATCH)) {
                            proxy = new CountDownLatchClientProxy(this, name);
                        } else if (name.startsWith(Prefix.IDGEN)) {
                            proxy = new IdGeneratorClientProxy(this, name);
                        } else if (name.startsWith(Prefix.MULTIMAP)) {
                            proxy = new MultiMapClientProxy(this, name);
                        } else if (name.startsWith(Prefix.SEMAPHORE)) {
                            proxy = new SemaphoreClientProxy(this, name);
                        } else {
                            proxy = new LockClientProxy(o, this);
                        }
                    } else {
                        proxy = new LockClientProxy(o, this);
                    }
                    mapProxies.put(o, proxy);
                }
            }
        }
        return mapProxies.get(o);
    }

    public com.hazelcast.core.Transaction getTransaction() {
        return TransactionUtil.getTransaction(this);
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    SerializerRegistry getSerializerRegistry() {
        return serializerRegistry;
    }

    public void addInstanceListener(InstanceListener instanceListener) {
        clusterClientProxy.addInstanceListener(instanceListener);
    }

    public Cluster getCluster() {
        return clusterClientProxy;
    }

    public ExecutorService getExecutorService() {
        return getExecutorService("default");
    }

    public ExecutorService getExecutorService(String name) {
        if (name == null) throw new IllegalArgumentException("ExecutorService name cannot be null");
//        name = Prefix.EXECUTOR_SERVICE + name;
        ExecutorServiceClientProxy executorServiceProxy = mapExecutors.get(name);
        if (executorServiceProxy == null) {
            executorServiceProxy = new ExecutorServiceClientProxy(this, name);
            ExecutorServiceClientProxy old = mapExecutors.putIfAbsent(name, executorServiceProxy);
            if (old != null) {
                executorServiceProxy = old;
            }
        }
        return executorServiceProxy;
    }

    public IdGenerator getIdGenerator(String name) {
        return (IdGenerator) getClientProxy(Prefix.IDGEN + name);
    }

    public AtomicNumber getAtomicNumber(String name) {
        return (AtomicNumber) getClientProxy(Prefix.ATOMIC_NUMBER + name);
    }

    public ICountDownLatch getCountDownLatch(String name) {
        return (ICountDownLatch) getClientProxy(Prefix.COUNT_DOWN_LATCH + name);
    }

    public ISemaphore getSemaphore(String name) {
        return (ISemaphore) getClientProxy(Prefix.SEMAPHORE + name);
    }

    public Collection<Instance> getInstances() {
        return clusterClientProxy.getInstances();
    }

    public <E> IList<E> getList(String name) {
        return (IList<E>) getClientProxy(Prefix.AS_LIST + name);
    }

    public ILock getLock(Object obj) {
        return new LockClientProxy(obj, this);
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return (MultiMap<K, V>) getClientProxy(Prefix.MULTIMAP + name);
    }

    public String getName() {
        return config.getGroupConfig().getName();
    }

    public <E> IQueue<E> getQueue(String name) {
        return (IQueue<E>) getClientProxy(Prefix.QUEUE + name);
    }

    public <E> ISet<E> getSet(String name) {
        return (ISet<E>) getClientProxy(Prefix.SET + name);
    }

    public <E> ITopic<E> getTopic(String name) {
        return (ITopic) getClientProxy(Prefix.TOPIC + name);
    }

    public void removeInstanceListener(InstanceListener instanceListener) {
        clusterClientProxy.removeInstanceListener(instanceListener);
    }

    public static void shutdownAll() {
        for (HazelcastClient hazelcastClient : lsClients) {
            try {
                hazelcastClient.shutdown();
            } catch (Exception ignored) {
            }
        }
        lsClients.clear();
    }

    public static Collection<HazelcastClient> getAllHazelcastClients() {
        return Collections.unmodifiableCollection(lsClients);
    }

    public void shutdown() {
        lifecycleService.shutdown();
    }

    void doShutdown() {
        if (active.compareAndSet(true, false)) {
            logger.log(Level.INFO, "HazelcastClient[" + this.id + "] is shutting down.");
            connectionManager.shutdown();
            out.shutdown();
            in.shutdown();
            listenerManager.shutdown();
            lsClients.remove(this);
            serializerRegistry.destroy();
        }
    }

    public boolean isActive() {
        return active.get();
    }

    protected void destroy(String proxyName) {
        mapProxies.remove(proxyName);
    }

    public void restart() {
        lifecycleService.restart();
    }

    public LifecycleService getLifecycleService() {
        return lifecycleService;
    }

    public <S extends ServiceProxy> S getServiceProxy(Class<? extends RemoteService> serviceClass, String name) {
        return null;
    }

    public <S extends ServiceProxy> S getServiceProxy(Class<? extends RemoteService> serviceClass, Object... params) {
        return null;
    }

    public <S extends ServiceProxy> S getServiceProxy(String serviceName, String name) {
        return null;
    }

    public <S extends ServiceProxy> S getServiceProxy(final Class<? extends ManagedService> serviceClass) {
        return null;
    }

    public <S extends ServiceProxy> S getServiceProxy(final String serviceName) {
        return null;
    }

    static void runAsyncAndWait(final Runnable runnable) {
        callAsyncAndWait(new Callable<Boolean>() {
            public Boolean call() throws Exception {
                runnable.run();
                return true;
            }
        });
    }

    static <V> V callAsyncAndWait(final Callable<V> callable) {
        final ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            Future<V> future = es.submit(callable);
            try {
                return future.get();
            } catch (Throwable e) {
                logger.log(Level.WARNING, e.getMessage(), e);
                return null;
            }
        } finally {
            es.shutdown();
        }
    }

    public ClientConfig getClientConfig() {
        return config;
    }

    public void registerFallbackSerializer(final TypeSerializer serializer) {
        serializerRegistry.registerFallback(serializer);
    }

    public void registerSerializer(final TypeSerializer serializer, final Class type) {
        serializerRegistry.register(serializer, type);
    }
}
