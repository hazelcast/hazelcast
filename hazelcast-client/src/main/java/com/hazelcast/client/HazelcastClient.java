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

package com.hazelcast.client;

import com.hazelcast.client.impl.ListenerManager;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.impl.executor.ParallelExecutor;
import com.hazelcast.impl.executor.ParallelExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.partition.PartitionService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hazelcast Client enables you to do all Hazelcast operations without
 * being a member of the cluster. It connects to one of the
 * cluster members and delegates all cluster wide operations to it.
 * When the relied cluster member dies, client will
 * transparently switch to another live member.
 */
public class HazelcastClient implements HazelcastInstance {

    final Map<Long, Call> calls = new ConcurrentHashMap<Long, Call>();

    final private ListenerManager listenerManager;
    final private OutRunnable out;
    final InRunnable in;
    final ConnectionManager connectionManager;
    final Map<Object, Object> mapProxies = new ConcurrentHashMap<Object, Object>(100);
    final ConcurrentMap<String, ExecutorServiceClientProxy> mapExecutors = new ConcurrentHashMap<String, ExecutorServiceClientProxy>(2);
    final IMap mapLockProxy;
    final ClusterClientProxy clusterClientProxy;
    final PartitionClientProxy partitionClientProxy;
    final String groupName;
    final ExecutorService executor;
    final ParallelExecutorService parallelExecutorService;
    final ParallelExecutor parallelExecutorDefault;
    final LifecycleServiceClientImpl lifecycleService;
    final static ILogger logger = Logger.getLogger(HazelcastClient.class.getName());

    private HazelcastClient(String groupName, String groupPassword, boolean shuffle, InetSocketAddress[] clusterMembers, boolean automatic) {
        this.groupName = groupName;
        final ThreadFactory threadFactory = new ThreadFactory() {
            final AtomicInteger atomicInteger = new AtomicInteger();

            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "hz.client." + HazelcastClient.this.groupName + "_cached_thread_" + atomicInteger.incrementAndGet());
                if (t.isDaemon()) {
                    t.setDaemon(false);
                }
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        };
        executor = Executors.newCachedThreadPool(threadFactory);
        lifecycleService = new LifecycleServiceClientImpl(this);
        lifecycleService.fireLifecycleEvent(new LifecycleEvent(LifecycleEvent.LifecycleState.STARTING));
        parallelExecutorService = new ParallelExecutorService(executor);
        parallelExecutorDefault = parallelExecutorService.newParallelExecutor(10);
        if (automatic) {
            this.connectionManager = new ConnectionManager(this, clusterMembers[0]);
        } else {
            this.connectionManager = new ConnectionManager(this, clusterMembers, shuffle);
        }
        this.connectionManager.setBinder(new DefaultClientBinder(this));
        final Connection aliveConnection = connectionManager.getAliveConnection();
        if (aliveConnection == null){
            throw new IllegalStateException("Unable to connect to cluster");
        }
        out = new OutRunnable(this, calls, new PacketWriter());
        new Thread(out, "hz.client.OutThread").start();
        in = new InRunnable(this, calls, new PacketReader());
        new Thread(in, "hz.client.InThread").start();
        listenerManager = new ListenerManager(this);
        new Thread(listenerManager, "hz.client.Listener").start();
        mapLockProxy = getMap("__hz_Locks");
        clusterClientProxy = new ClusterClientProxy(this);
        partitionClientProxy = new PartitionClientProxy(this);
        
        Boolean authenticate = clusterClientProxy.authenticate(groupName, groupPassword);
        if (!authenticate) {
            this.shutdown();
            throw new AuthenticationException("Wrong group name and password.");
        }
        
        try {
            this.connectionManager.bindConnection(aliveConnection);
        } catch (IOException e) {
            this.shutdown();
            throw new ClusterClientException(e.getMessage(), e);
        }
        
        if (automatic) {
            this.getCluster().addMembershipListener(connectionManager);
            connectionManager.updateMembers();
        }
        lifecycleService.fireLifecycleEvent(new LifecycleEvent(LifecycleEvent.LifecycleState.STARTED));
    }

    public ParallelExecutor getDefaultParallelExecutor() {
        return parallelExecutorDefault;
    }

    public OutRunnable getOutRunnable() {
        return out;
    }

    ListenerManager getListenerManager() {
        return listenerManager;
    }

    private HazelcastClient(String groupName, String groupPassword, InetSocketAddress address) {
        this(groupName, groupPassword, false, new InetSocketAddress[]{address}, true);
    }

    /**
     * Returns a new HazelcastClient. It will shuffle the given address list and pick one address to connect.
     * If the connected member will die, client will pick another from given addresses.
     *
     * @param groupName     Group name of a cluster that client will connect
     * @param groupPassword Group Password of a cluster that client will connect.
     * @param addresses     Addresses of Cluster Members that client will choose one to connect. If the connected member
     *                      dies client will switch to the next one in the list.
     *                      An address is in the form ip:port. If you will not specify the port, it will assume the default one, 5701.
     *                      ex: "10.90.0.1", "10.90.0.2:5702"
     * @return Returns a new Hazelcast Client instance.
     */
    public static HazelcastClient newHazelcastClient(String groupName, String groupPassword, String... addresses) {
        return newHazelcastClient(groupName, groupPassword, true, addresses);
    }

    /**
     * Returns a new HazelcastClient.
     * If the connected member will die, client will pick next live address from given addresses.
     *
     * @param groupName     Group name of a cluster that client will connect
     * @param groupPassword Group Password of a cluster that client will connect.
     * @param shuffle       Specifies whether to shuffle the list of addresses
     * @param addresses     Addresses of Cluster Members that client will choose one to connect. If the connected member
     *                      dies client will switch to the next one in the list.
     *                      An address is in the form ip:port. If you will not specify the port, it will assume the default one, 5701.
     *                      ex: "10.90.0.1", "10.90.0.2:5702"
     * @return Returns a new Hazelcast Client instance.
     */
    public static HazelcastClient newHazelcastClient(String groupName, String groupPassword, boolean shuffle, String... addresses) {
        InetSocketAddress[] socketAddressArr = new InetSocketAddress[addresses.length];
        for (int i = 0; i < addresses.length; i++) {
            InetSocketAddress inetSocketAddress = parse(addresses[i]);
            socketAddressArr[i] = inetSocketAddress;
        }
        return newHazelcastClient(groupName, groupPassword, shuffle, socketAddressArr);
    }

    private static InetSocketAddress parse(String address) {
        String[] separated = address.split(":");
        int port = (separated.length > 1) ? Integer.valueOf(separated[1]) : 5701;
        InetSocketAddress inetSocketAddress = new InetSocketAddress(separated[0], port);
        return inetSocketAddress;
    }

    /**
     * Returns a new HazelcastClient.
     * If the connected member will die, client will pick next live address from given addresses.
     *
     * @param groupName     Group name of a cluster that client will connect
     * @param groupPassword Group Password of a cluster that client will connect.
     * @param shuffle       Specifies whether to shuffle the list of addresses
     * @param addresses     InetSocketAddress of Cluster Members that client will choose one to connect. If the connected member
     *                      dies client will switch to the next one in the list.
     * @return Returns a new Hazelcast Client instance.
     */
    public static HazelcastClient newHazelcastClient(String groupName, String groupPassword, boolean shuffle, InetSocketAddress... addresses) {
        return new HazelcastClient(groupName, groupPassword, shuffle, addresses, false);
    }

    /**
     * Returns a new HazelcastClient.
     * Giving address of one member is enough. It will connect to that member and will get addresses of all members
     * in the cluster. If the connected member will die or leave the cluster, client will automatically
     * switch to another member in the cluster.
     *
     * @param groupName
     * @param groupPassword
     * @param address
     * @return
     */
    public static HazelcastClient newHazelcastClient(String groupName, String groupPassword, String address) {
        InetSocketAddress inetSocketAddress = parse(address);
        return new HazelcastClient(groupName, groupPassword, inetSocketAddress);
    }

    public Config getConfig() {
        throw new UnsupportedOperationException();
    }

    public PartitionService getPartitionService() {
        return partitionClientProxy;
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
                        } else if (name.startsWith(Prefix.LIST)) {
                            proxy = new ListClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.SET)) {
                            proxy = new SetClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.QUEUE)) {
                            proxy = new QueueClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.TOPIC)) {
                            proxy = new TopicClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.ATOMIC_NUMBER)) {
                            proxy = new AtomicNumberClientProxy(this, name);
                        } else if (name.startsWith(Prefix.IDGEN)) {
                            proxy = new IdGeneratorClientProxy(this, name);
                        } else if (name.startsWith(Prefix.MULTIMAP)) {
                            proxy = new MultiMapClientProxy(this, name);
                        } else {
                            proxy = new LockClientProxy(o, this);
                        }
                        mapProxies.put(o, proxy);
                    } else {
                        proxy = new LockClientProxy(o, this);
                    }
                }
            }
        }
        return mapProxies.get(o);
    }

    public com.hazelcast.core.Transaction getTransaction() {
        ThreadContext trc = ThreadContext.get();
        TransactionClientProxy proxy = (TransactionClientProxy) trc.getTransaction(this);
        return proxy;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
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
        name = Prefix.EXECUTOR_SERVICE + name;
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

    public Collection<Instance> getInstances() {
        return clusterClientProxy.getInstances();
    }

    public <E> IList<E> getList(String name) {
        return (IList<E>) getClientProxy(Prefix.LIST + name);
    }

    public ILock getLock(Object obj) {
        return new LockClientProxy(obj, this);
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return (MultiMap<K, V>) getClientProxy(Prefix.MULTIMAP + name);
    }

    public String getName() {
        return groupName;
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

    public void shutdown() {
        lifecycleService.shutdown();
    }

    void doShutdown() {
        out.shutdown();
        listenerManager.shutdown();
        in.shutdown();
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {
        }
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
}
