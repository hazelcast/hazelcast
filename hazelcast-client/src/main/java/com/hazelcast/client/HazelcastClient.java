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

import com.hazelcast.client.ClientProperties.ClientPropertyName;
import com.hazelcast.client.impl.ListenerManager;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.util.AddressUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
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

    private final static AtomicInteger clientIdCounter = new AtomicInteger();

    final Map<Long, Call> calls = new ConcurrentHashMap<Long, Call>(100);

    final ListenerManager listenerManager;
    final private OutRunnable out;
    final InRunnable in;
    final ConnectionManager connectionManager;
    final Map<Object, Object> mapProxies = new ConcurrentHashMap<Object, Object>(100);
    final ConcurrentMap<String, ExecutorServiceClientProxy> mapExecutors = new ConcurrentHashMap<String, ExecutorServiceClientProxy>(2);
    final IMap mapLockProxy;
    final ClusterClientProxy clusterClientProxy;
    final PartitionClientProxy partitionClientProxy;
    final LifecycleServiceClientImpl lifecycleService;
    final static ILogger logger = Logger.getLogger(HazelcastClient.class.getName());

    final int id;

    private final ClientProperties properties;

    volatile boolean active = true;

    private HazelcastClient(ClientProperties properties, boolean shuffle, InetSocketAddress[] clusterMembers, boolean automatic) {
        this.properties = properties;
        this.id = clientIdCounter.incrementAndGet();
        final long timeout = Long.valueOf(properties.getProperty(ClientPropertyName.CONNECTION_TIMEOUT));
        final String prefix = "hz.client." + this.id + ".";
        lifecycleService = new LifecycleServiceClientImpl(this);
        lifecycleService.fireLifecycleEvent(STARTING);
        connectionManager = automatic ?
                new ConnectionManager(this, lifecycleService, clusterMembers[0], timeout) :
                new ConnectionManager(this, lifecycleService, clusterMembers, shuffle, timeout);
        connectionManager.setBinder(new DefaultClientBinder(this));
        out = new OutRunnable(this, calls, new PacketWriter());
        in = new InRunnable(this, out, calls, new PacketReader());
        listenerManager = new ListenerManager(this);
        try {
            final Connection c = connectionManager.getInitConnection();
            if (c == null) {
                throw new IllegalStateException("Unable to connect to cluster");
            }
        } catch (IOException e) {
            throw new ClusterClientException(e.getMessage(), e);
        }
        new Thread(out, prefix + "OutThread").start();
        new Thread(in, prefix + "InThread").start();
        new Thread(listenerManager, prefix + "Listener").start();
        mapLockProxy = getMap(Prefix.HAZELCAST + "Locks");
        clusterClientProxy = new ClusterClientProxy(this);
        partitionClientProxy = new PartitionClientProxy(this);
        if (automatic) {
            this.getCluster().addMembershipListener(connectionManager);
            connectionManager.updateMembers();
        }
        lifecycleService.fireLifecycleEvent(STARTED);
    }

    GroupConfig groupConfig() {
        final String groupName = properties.getProperty(ClientPropertyName.GROUP_NAME);
        final String groupPassword = properties.getProperty(ClientPropertyName.GROUP_PASSWORD);
        return new GroupConfig(groupName, groupPassword);
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

    private HazelcastClient(ClientProperties properties, InetSocketAddress address) {
        this(properties, false, new InetSocketAddress[]{address}, true);
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
        return newHazelcastClient(ClientProperties.createBaseClientProperties(groupName, groupPassword), addresses);
    }

    /**
     * Returns a new HazelcastClient. It will shuffle the given address list and pick one address to connect.
     * If the connected member will die, client will pick another from given addresses.
     *
     * @param properties Client Properties
     * @param addresses  Addresses of Cluster Members that client will choose one to connect. If the connected member
     *                   dies client will switch to the next one in the list.
     *                   An address is in the form ip:port. If you will not specify the port, it will assume the default one, 5701.
     *                   ex: "10.90.0.1", "10.90.0.2:5702"
     * @return Returns a new Hazelcast Client instance.
     */
    public static HazelcastClient newHazelcastClient(ClientProperties properties, String... addresses) {
        return newHazelcastClient(properties, true, addresses);
    }

    /**
     * Returns a new HazelcastClient. It will shuffle the given address list and pick one address to connect.
     * If the connected member will die, client will pick another from given addresses.
     *
     * @param properties Client Properties
     * @param addresses  List of addresses of Cluster Members that client will choose one to connect. If the connected member
     *                   dies client will switch to the next one in the list.
     *                   An address is in the form ip:port. If you will not specify the port, it will assume the default one, 5701.
     *                   ex: "10.90.0.1", "10.90.0.2:5702"
     * @return Returns a new Hazelcast Client instance.
     */
    public static HazelcastClient newHazelcastClient(ClientProperties properties, List<String> addresses) {
        final List<String> handleMembers = AddressUtil.handleMembers(addresses);
        return newHazelcastClient(properties, handleMembers.toArray(new String[0]));
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
        return newHazelcastClient(ClientProperties.createBaseClientProperties(groupName, groupPassword), shuffle, addresses);
    }

    /**
     * Returns a new HazelcastClient.
     * If the connected member will die, client will pick next live address from given addresses.
     *
     * @param properties Client Properties
     * @param shuffle    Specifies whether to shuffle the list of addresses
     * @param addresses  Addresses of Cluster Members that client will choose one to connect. If the connected member
     *                   dies client will switch to the next one in the list.
     *                   An address is in the form ip:port. If you will not specify the port, it will assume the default one, 5701.
     *                   ex: "10.90.0.1", "10.90.0.2:5702"
     * @return Returns a new Hazelcast Client instance.
     */
    public static HazelcastClient newHazelcastClient(ClientProperties properties, boolean shuffle, String... addresses) {
        InetSocketAddress[] socketAddressArr = new InetSocketAddress[addresses.length];
        for (int i = 0; i < addresses.length; i++) {
            InetSocketAddress inetSocketAddress = parse(addresses[i]);
            socketAddressArr[i] = inetSocketAddress;
        }
        return newHazelcastClient(properties, shuffle, socketAddressArr);
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
        return newHazelcastClient(ClientProperties.createBaseClientProperties(groupName, groupPassword), shuffle, addresses);
    }

    /**
     * Returns a new HazelcastClient.
     * If the connected member will die, client will pick next live address from given addresses.
     *
     * @param clientProperties Client Properties
     * @param shuffle          Specifies whether to shuffle the list of addresses
     * @param addresses        InetSocketAddress of Cluster Members that client will choose one to connect. If the connected member
     *                         dies client will switch to the next one in the list.
     * @return Returns a new Hazelcast Client instance.
     */
    public static HazelcastClient newHazelcastClient(ClientProperties clientProperties, boolean shuffle, InetSocketAddress... addresses) {
        return new HazelcastClient(clientProperties, shuffle, addresses, false);
    }

    /**
     * Giving address of one member is enough. It will connect to that member and will get addresses of all members
     * in the cluster. If the connected member will die or leave the cluster, client will automatically
     * switch to another member in the cluster.
     *
     * @param groupName     Group name of a cluster that client will connect
     * @param groupPassword Group Password of a cluster that client will connect.
     * @param address       Address of one of the members
     * @return Returns a new HazelcastClient.
     */
    public static HazelcastClient newHazelcastClient(String groupName, String groupPassword, String address) {
        InetSocketAddress inetSocketAddress = parse(address);
        return new HazelcastClient(ClientProperties.createBaseClientProperties(groupName, groupPassword), inetSocketAddress);
    }

    /**
     * Giving address of one member is enough. It will connect to that member and will get addresses of all members
     * in the cluster. If the connected member will die or leave the cluster, client will automatically
     * switch to another member in the cluster.
     *
     * @param clientProperties Client Properties
     * @param address          Address of one of the members
     * @return Returns a new HazelcastClient.
     */
    public static HazelcastClient newHazelcastClient(ClientProperties clientProperties, String address) {
        InetSocketAddress inetSocketAddress = parse(address);
        return new HazelcastClient(clientProperties, inetSocketAddress);
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
                        } else if (name.startsWith(Prefix.AS_LIST)) {
                            proxy = new ListClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.SET)) {
                            proxy = new SetClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.QUEUE)) {
                            proxy = new QueueClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.TOPIC)) {
                            proxy = new TopicClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.ATOMIC_LONG)) {
                            proxy = new AtomicNumberClientProxy(this, name);
                        } else if (name.startsWith(Prefix.COUNT_DOWN_LATCH)) {
                            //proxy = new CountDownLatchClientProxy(this, name);
                        } else if (name.startsWith(Prefix.IDGEN)) {
                            proxy = new IdGeneratorClientProxy(this, name);
                        } else if (name.startsWith(Prefix.MULTIMAP)) {
                            proxy = new MultiMapClientProxy(this, name);
                        } else if (name.startsWith(Prefix.SEMAPHORE)) {
                            proxy = new SemaphoreClientProxy(this, name);
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
        ClientThreadContext trc = ClientThreadContext.get();
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

    public ClientProperties getProperties() {
        return properties;
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
        return (AtomicNumber) getClientProxy(Prefix.ATOMIC_LONG + name);
    }

    public ISemaphore getSemaphore(String name) {
        ISemaphore semaphore = (ISemaphore) getClientProxy(Prefix.SEMAPHORE + name);
        return semaphore;
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
        return properties.getProperty(ClientPropertyName.GROUP_NAME);
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
        logger.log(Level.INFO, "HazelcastClient[" + this.id + "] is shutting down.");
        connectionManager.shutdown();
        out.shutdown();
        in.shutdown();
        listenerManager.shutdown();
        ClientThreadContext.shutdown();
        active = false;
    }

    public boolean isActive() {
        return active;
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
}
