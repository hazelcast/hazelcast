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

package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.partition.PartitionService;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory for all of the Hazelcast data and execution components such as
 * maps, queues, multimaps, topics and executor service.
 * <p/>
 * If not started already, Hazelcast member (HazelcastInstance) will start
 * automatically if any of the functions is called on Hazelcast.
 */
@SuppressWarnings("SynchronizationOnStaticField")
public final class Hazelcast {
    @Deprecated
    private static final AtomicReference<HazelcastInstance> defaultInstance = new AtomicReference<HazelcastInstance>();
    @Deprecated
    private static final Object initLock = new Object();
    @Deprecated
    private static Config defaultConfig = null;

    private Hazelcast() {
    }

    /**
     * Initializes the default Hazelcast instance with the specified configuration.
     * This method should be called before calling any other methods.
     *
     * @param config configuration for this Hazelcast instance.
     * @return the default instance
     * @throws IllegalStateException if this instance is already initialized
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see #getHazelcastInstanceByName(String)
     * @see #getAllHazelcastInstances()
     */
    @Deprecated
    public static HazelcastInstance init(Config config) {
        if (defaultInstance.get() != null) {
            throw new IllegalStateException("Default Hazelcast instance is already initialized.");
        }
        synchronized (initLock) {
            if (defaultInstance.get() != null) {
                throw new IllegalStateException("Default Hazelcast instance is already initialized.");
            }
            defaultConfig = config;
            HazelcastInstance defaultInstanceObject = com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(config);
            defaultInstance.set(defaultInstanceObject);
            return defaultInstanceObject;
        }
    }

    /**
     * Returns the default Hazelcast instance, starts it with the default
     * configuration, if not already started.
     *
     * @return the default Hazelcast instance
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see #getHazelcastInstanceByName(String)
     * @see #getAllHazelcastInstances()
     */
    @Deprecated
    public static HazelcastInstance getDefaultInstance() {
        HazelcastInstance defaultInstanceObject = defaultInstance.get();
        if (defaultInstanceObject == null
                || !defaultInstanceObject.getLifecycleService().isRunning()) {
            synchronized (initLock) {
                defaultInstanceObject = defaultInstance.get();
                if (defaultInstanceObject == null
                        || !defaultInstanceObject.getLifecycleService().isRunning()) {
                    defaultInstanceObject = com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(defaultConfig);
                    defaultInstance.set(defaultInstanceObject);
                    return defaultInstanceObject;
                } else {
                    return defaultInstanceObject;
                }
            }
        } else {
            return defaultInstanceObject;
        }
    }

    /**
     * Returns the distributed queue instance with the specified name.
     *
     * @param name name of the distributed queue
     * @return distributed queue instance with the specified name
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getQueue(String)
     */
    @Deprecated
    public static <E> IQueue<E> getQueue(String name) {
        return getDefaultInstance().getQueue(name);
    }

    /**
     * Returns the distributed topic instance with the specified name.
     *
     * @param name name of the distributed topic
     * @return distributed topic instance with the specified name
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getTopic(String)
     */
    @Deprecated
    public static <E> ITopic<E> getTopic(String name) {
        return getDefaultInstance().getTopic(name);
    }

    /**
     * Returns the distributed set instance with the specified name.
     *
     * @param name name of the distributed set
     * @return distributed set instance with the specified name
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getSet(String)
     */
    @Deprecated
    public static <E> ISet<E> getSet(String name) {
        return getDefaultInstance().getSet(name);
    }

    /**
     * Returns the distributed list instance with the specified name.
     *
     * @param name name of the distributed list
     * @return distributed list instance with the specified name
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getList(String)
     */
    @Deprecated
    public static <E> IList<E> getList(String name) {
        return getDefaultInstance().getList(name);
    }

    /**
     * Returns the distributed map instance with the specified name.
     *
     * @param name name of the distributed map
     * @return distributed map instance with the specified name
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getMap(String)
     */
    @Deprecated
    public static <K, V> IMap<K, V> getMap(String name) {
        return getDefaultInstance().getMap(name);
    }

    /**
     * Returns the distributed multimap instance with the specified name.
     *
     * @param name name of the distributed multimap
     * @return distributed multimap instance with the specified name
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getMultiMap(String)
     */
    @Deprecated
    public static <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getDefaultInstance().getMultiMap(name);
    }

    /**
     * Returns the distributed lock instance for the specified key object.
     * The specified object is considered to be the key for this lock.
     * So keys are considered equals cluster-wide as long as
     * they are serialized to the same byte array such as String, long,
     * Integer.
     * <p/>
     * Locks are fail-safe. If a member holds a lock and some of the
     * members go down, cluster will keep your locks safe and available.
     * Moreover, when a member leaves the cluster, all the locks acquired
     * by this dead member will be removed so that these locks can be
     * available for live members immediately.
     * <pre>
     * Lock lock = Hazelcast.getLock("PROCESS_LOCK");
     * lock.lock();
     * try {
     *   // process
     * } finally {
     *   lock.unlock();
     * }
     * </pre>
     *
     * @param key key of the lock instance
     * @return distributed lock instance for the specified key.
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getLock(Object)
     */

    @Deprecated
    public static ILock getLock(Object key) {
        return getDefaultInstance().getLock(key);
    }

    /**
     * Returns the Cluster that this Hazelcast instance is part of.
     * Cluster interface allows you to add listener for membership
     * events and learn more about the cluster that this Hazelcast
     * instance is part of.
     *
     * @return cluster that this Hazelcast instance is part of
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getCluster()
     */
    @Deprecated
    public static Cluster getCluster() {
        return getDefaultInstance().getCluster();
    }

    /**
     * Returns the default distributed executor service. Executor
     * service enables you to run your <tt>Runnable</tt>s and <tt>Callable</tt>s
     * on the Hazelcast cluster.
     *
     * Note that it don't support invokeAll/Any and don't have standard shutdown behavior
     *
     * @return distributed executor service of this Hazelcast instance
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getExecutorService()
     */
    @Deprecated
    public static ExecutorService getExecutorService() {
        return getDefaultInstance().getExecutorService();
    }

    /**
     * Returns the distributed executor service for the given
     * name.
     *
     * @param name name of the executor service
     * @return executor service for the given name
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getExecutorService(String)
     */
    @Deprecated
    public static ExecutorService getExecutorService(String name) {
        return getDefaultInstance().getExecutorService(name);
    }

    /**
     * Returns the transaction instance associated with the current thread,
     * creates a new one if it wasn't already.
     * <p/>
     * Transaction doesn't start until you call <tt>transaction.begin()</tt> and
     * if a transaction is started then all transactional Hazelcast operations
     * are automatically transactional.
     * <pre>
     *  Map map = Hazelcast.getMap("mymap");
     *  Transaction txn = Hazelcast.getTransaction();
     *  txn.begin();
     *  try {
     *    map.put ("key", "value");
     *    txn.commit();
     *  }catch (Exception e) {
     *    txn.rollback();
     *  }
     * </pre>
     * Isolation is always <tt>REPEATABLE_READ</tt> . If you are in
     * a transaction, you can read the data in your transaction and the data that
     * is already committed and if not in a transaction, you can only read the
     * committed data. Implementation is different for queue and map/set. For
     * queue operations (offer,poll), offered and/or polled objects are copied to
     * the next member in order to safely commit/rollback. For map/set, Hazelcast
     * first acquires the locks for the write operations (put, remove) and holds
     * the differences (what is added/removed/updated) locally for each transaction.
     * When transaction is set to commit, Hazelcast will release the locks and
     * apply the differences. When rolling back, Hazelcast will simply releases
     * the locks and discard the differences. Transaction instance is attached
     * to the current thread and each Hazelcast operation checks if the current
     * thread holds a transaction, if so, operation will be transaction aware.
     * When transaction is committed, rolled back or timed out, it will be detached
     * from the thread holding it.
     *
     * @return transaction for the current thread
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getTransaction()
     */
    @Deprecated
    public static Transaction getTransaction() {
        return getDefaultInstance().getTransaction();
    }

    /**
     * Creates cluster-wide atomic long. Hazelcast AtomicNumber is a distributed
     * implementation of <tt>java.util.concurrent.atomic.AtomicLong</tt>.
     *
     * @param name of the AtomicNumber proxy
     * @return AtomicNumber proxy instance
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getAtomicNumber(String)
     */
    @Deprecated
    public static AtomicNumber getAtomicNumber(String name) {
        return getDefaultInstance().getAtomicNumber(name);
    }

    /**
     * Creates a cluster-wide CountDownLatch. Hazelcast ICountDownLatch is a distributed
     * implementation of <tt>java.util.concurrent.CountDownLatch</tt>.
     *
     * @param name of the distributed CountDownLatch
     * @return ICountDownLatch proxy instance
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getCountDownLatch(String)
     */
    @Deprecated
    public static ICountDownLatch getCountDownLatch(String name) {
        return getDefaultInstance().getCountDownLatch(name);
    }

    /**
     * Creates a cluster-wide semaphore. Hazelcast ISemaphore is a distributed
     * implementation of <tt>java.util.concurrent.Semaphore</tt>.
     *
     * @param name of the distributed Semaphore
     * @return ISemaphore proxy instance
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getSemaphore(String)
     */
    @Deprecated
    public static ISemaphore getSemaphore(String name) {
        return getDefaultInstance().getSemaphore(name);
    }

    /**
     * Creates cluster-wide unique IDs. Generated IDs are long type primitive values
     * between <tt>0</tt> and <tt>Long.MAX_VALUE</tt> . Id generation occurs almost at the speed of
     * <tt>AtomicLong.incrementAndGet()</tt> . Generated IDs are unique during the life
     * cycle of the cluster. If the entire cluster is restarted, IDs start from <tt>0</tt> again.
     *
     * @param name
     * @return IdGenerator proxy instance
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getIdGenerator(String)
     */
    @Deprecated
    public static IdGenerator getIdGenerator(String name) {
        return getDefaultInstance().getIdGenerator(name);
    }

    /**
     * Detaches this member from the cluster.
     * It doesn't shutdown the entire cluster, it shuts down
     * this local member only.
     *
     * @see HazelcastInstance#getLifecycleService()
     * @see LifecycleService#shutdown()
     * @deprecated as of version 1.9
     */
    @Deprecated
    public static void shutdown() {
        synchronized (initLock) {
            if (defaultInstance.get() != null) {
                getDefaultInstance().shutdown();
                defaultInstance.set(null);
            }
        }
    }

    /**
     * Shuts down all running Hazelcast Instances on this JVM, including the
     * default one if it is running. It doesn't shutdown all members of the
     * cluster but just the ones running on this JVM.
     *
     * @see #newHazelcastInstance(Config)
     */
    public static void shutdownAll() {
        com.hazelcast.impl.FactoryImpl.shutdownAll();
        synchronized (initLock) {
            defaultInstance.set(null);
        }
    }

    /**
     * Detaches this member from the cluster first and then restarts it
     * as a new member.
     *
     * @see HazelcastInstance##getLifecycleService()
     * @see LifecycleService#restart()
     * @deprecated as of version 1.9
     */
    @Deprecated
    public static void restart() {
        synchronized (initLock) {
            if (defaultInstance.get() != null) {
                getLifecycleService().restart();
            } else {
                getDefaultInstance();
            }
        }
    }

    /**
     * Returns all queue, map, set, list, topic, lock, multimap
     * instances created by Hazelcast.
     *
     * @return the collection of instances created by Hazelcast.
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getInstances()
     */
    @Deprecated
    public static Collection<Instance> getInstances() {
        return getDefaultInstance().getInstances();
    }

    /**
     * Add a instance listener which will be notified when a
     * new instance such as map, queue, multimap, topic, lock is
     * added or removed.
     *
     * @param instanceListener instance listener
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#addInstanceListener(InstanceListener)
     */
    @Deprecated
    public static void addInstanceListener(InstanceListener instanceListener) {
        getDefaultInstance().addInstanceListener(instanceListener);
    }

    /**
     * Removes the specified instance listener. Returns silently
     * if specified instance listener doesn't exist.
     *
     * @param instanceListener instance listener to remove
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#removeInstanceListener(InstanceListener)
     */
    @Deprecated
    public static void removeInstanceListener(InstanceListener instanceListener) {
        getDefaultInstance().removeInstanceListener(instanceListener);
    }

    /**
     * Creates a new HazelcastInstance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p/>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param config Configuration for the new HazelcastInstance (member)
     * @return new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastInstanceByName(String)
     */
    public static HazelcastInstance newHazelcastInstance(Config config) {
        return com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(config);
    }

    /**
     * Creates a new HazelcastInstance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p/>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * Hazelcast will look into two places for the configuration file:
     * <ol>
     *     <li>
     *         System property: Hazelcast will first check if "hazelcast.config" system property is set to a file path.
     *         Example: -Dhazelcast.config=C:/myhazelcast.xml.
     *     </li>
     *     <li>
     *         Classpath: If config file is not set as a system property, Hazelcast will check classpath for hazelcast.xml file.
     *     </li>
     * </ol>
     * If Hazelcast doesn't find any config file, it will happily start with default configuration (hazelcast-default.xml)
     * located in hazelcast.jar.
     *
     * @return new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastInstanceByName(String)
     */
    public static HazelcastInstance newHazelcastInstance() {
        return com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(null);
    }

    /**
     * Creates a new HazelcastInstance Lite Member (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p/>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * Hazelcast will look into two places for the configuration file:
      * <ol>
      *     <li>
      *         System property: Hazelcast will first check if "hazelcast.config" system property is set to a file path.
     *         Example: -Dhazelcast.config=C:/myhazelcast.xml.
      *     </li>
      *     <li>
      *         Classpath: If config file is not set as a system property, Hazelcast will check classpath for hazelcast.xml file.
      *     </li>
      * </ol>
     * If Hazelcast doesn't find any config file, it will happily start with default configuration (hazelcast-default.xml)
     * located in hazelcast.jar.
     *
     * @return new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastInstanceByName(String)
     */
    public static HazelcastInstance newLiteMemberHazelcastInstance() {
        return com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(null, true);
    }

    /**
     * Returns an existing HazelcastInstance with instanceName.
     * <p/>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param instanceName Name of the HazelcastInstance (member)
     * @return HazelcastInstance
     * @see #newHazelcastInstance(Config)
     * @see #shutdownAll()
     */
    public static HazelcastInstance getHazelcastInstanceByName(String instanceName) {
        return com.hazelcast.impl.FactoryImpl.getHazelcastInstanceProxy(instanceName);
    }

    /**
     * Returns all active/running HazelcastInstances on this JVM.
     * <p/>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @return all HazelcastInstances
     * @see #newHazelcastInstance(Config)
     * @see #getHazelcastInstanceByName(String)
     * @see #shutdownAll()
     */
    public static Set<HazelcastInstance> getAllHazelcastInstances() {
        return com.hazelcast.impl.FactoryImpl.getAllHazelcastInstanceProxies();
    }

    /**
     * Returns the configuration of this Hazelcast instance.
     *
     * @return configuration of this Hazelcast instance
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getConfig()
     */
    @Deprecated
    public static Config getConfig() {
        return getDefaultInstance().getConfig();
    }

    /**
     * Returns the partition service of this Hazelcast instance.
     * PartitionService allows you to introspect current partitions in the
     * cluster, partition owner members and listen for partition migration events.
     *
     * @return partition service
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getPartitionService()
     */
    @Deprecated
    public static PartitionService getPartitionService() {
        return getDefaultInstance().getPartitionService();
    }

    /**
     * Returns the logging service of this Hazelcast instance.
     * LoggingService allows you to listen for LogEvents
     * generated by Hazelcast runtime. You can log the events somewhere
     * or take action base on the message.
     *
     * @return logging service
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getLoggingService()
     */
    @Deprecated
    public static LoggingService getLoggingService() {
        return getDefaultInstance().getLoggingService();
    }

    /**
     * Returns the lifecycle service for this instance. LifecycleService allows you
     * to shutdown, restart, pause and resume this HazelcastInstance and listen for
     * the lifecycle events.
     *
     * @return lifecycle service
     *
     * @deprecated as of version 2.2
     * @see #newHazelcastInstance(com.hazelcast.config.Config)
     * @see HazelcastInstance#getLifecycleService()
     */
    @Deprecated
    public static LifecycleService getLifecycleService() {
        return getDefaultInstance().getLifecycleService();
    }

    /**
     * Sets <tt>OutOfMemoryHandler</tt> to be used when an <tt>OutOfMemoryError</tt>
     * is caught by Hazelcast threads.
     *
     * <p>
     * <b>Warning: </b> <tt>OutOfMemoryHandler</tt> may not be called although JVM throws
     * <tt>OutOfMemoryError</tt>.
     * Because error may be thrown from an external (user thread) thread
     * and Hazelcast may not be informed about <tt>OutOfMemoryError</tt>.
     * </p>
     *
     * @param outOfMemoryHandler
     *
     * @see OutOfMemoryError
     * @see OutOfMemoryHandler
     */
    public static void setOutOfMemoryHandler(OutOfMemoryHandler outOfMemoryHandler) {
        com.hazelcast.impl.OutOfMemoryErrorDispatcher.setHandler(outOfMemoryHandler);
    }
}
