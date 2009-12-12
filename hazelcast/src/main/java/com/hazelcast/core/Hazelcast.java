/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.impl.FactoryImpl;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * Factory for all of the Hazelcast data and execution components such as
 * maps, queues, multimaps, topics and executor service.
 * <p/>
 * If not started already, Hazelcast member (HazelcastInstance) will start
 * automatically if any of the functions is called on Hazelcast.
 */
public final class Hazelcast {
    private static volatile HazelcastInstance defaultInstance = null;
    private final static Object initLock = new Object();

    private Hazelcast() {
    }

    /**
     * Initilizes Hazelcast instance with the specified configuration.
     * This method should be called before calling any other methods otherwise
     *
     * @param config configuration for this Hazelcast instance.
     * @throws IllegalStateException if this instance is already initilized
     */
    public static void init(Config config) {
        if (defaultInstance != null) {
            throw new IllegalStateException("Default Hazelcast instance is already initilized.");
        }
        synchronized (initLock) {
            if (defaultInstance != null) {
                throw new IllegalStateException("Default Hazelcast instance is already initilized.");
            }
            defaultInstance = com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(config);
        }
    }

    private static HazelcastInstance getDefaultInstance() {
        if (defaultInstance == null) {
            synchronized (initLock) {
                if (defaultInstance == null) {
                    defaultInstance = com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(null);
                }
            }
        }
        return defaultInstance;
    }

    /**
     * Returns the distributed queue instance with the specified name.
     *
     * @param name name of the distributed queue
     * @return distributed queue instance with the specified name
     */
    public static <E> IQueue<E> getQueue(String name) {
        return getDefaultInstance().getQueue(name);
    }

    /**
     * Returns the distributed topic instance with the specified name.
     *
     * @param name name of the distributed topic
     * @return distributed topic instance with the specified name
     */
    public static <E> ITopic<E> getTopic(String name) {
        return getDefaultInstance().getTopic(name);
    }

    /**
     * Returns the distributed set instance with the specified name.
     *
     * @param name name of the distributed set
     * @return distributed set instance with the specified name
     */
    public static <E> ISet<E> getSet(String name) {
        return getDefaultInstance().getSet(name);
    }

    /**
     * Returns the distributed list instance with the specified name.
     * Index based operations on the list are not supported.
     *
     * @param name name of the distributed list
     * @return distributed list instance with the specified name
     */
    public static <E> IList<E> getList(String name) {
        return getDefaultInstance().getList(name);
    }

    /**
     * Returns the distributed map instance with the specified name.
     *
     * @param name name of the distributed map
     * @return distributed map instance with the specified name
     */
    public static <K, V> IMap<K, V> getMap(String name) {
        return getDefaultInstance().getMap(name);
    }

    /**
     * Returns the distributed multimap instance with the specified name.
     *
     * @param name name of the distributed multimap
     * @return distributed mutimap instance with the specified name
     */
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
     * <p/>
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
     */
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
     */
    public static Cluster getCluster() {
        return getDefaultInstance().getCluster();
    }

    /**
     * Returns the distributed executor service. This executor
     * service enables you to run your <tt>Runnable</tt>s and <tt>Callable</tt>s
     * on the Hazelcast cluster.
     *
     * @return distrubuted executor service of this Hazelcast instance
     */
    public static ExecutorService getExecutorService() {
        return getDefaultInstance().getExecutorService();
    }

    /**
     * Returns the transaction instance associated with the current thread,
     * creates a new one if it wasn't already.
     * <p/>
     * Transaction doesn't start until you call <tt>transaction.begin()</tt> and
     * if a transaction is started then all transactional Hazelcast operations
     * are automatically transational.
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
     * Isolation is always <tt>READ_COMMITTED</tt> . If you are in
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
     */
    public static Transaction getTransaction() {
        return getDefaultInstance().getTransaction();
    }

    /**
     * Creates cluster-wide unique IDs. Generated IDs are long type primitive values
     * between <tt>0</tt> and <tt>Long.MAX_VALUE</tt> . Id generation occurs almost at the speed of
     * <tt>AtomicLong.incrementAndGet()</tt> . Generated IDs are unique during the life
     * cycle of the cluster. If the entire cluster is restarted, IDs start from <tt>0</tt> again.
     *
     * @param name
     * @return
     */
    public static IdGenerator getIdGenerator(String name) {
        return getDefaultInstance().getIdGenerator(name);
    }

    /**
     * Detaches this member from the cluster.
     * It doesn't shutdown the entire cluster, it shuts down
     * this local member only.
     */
    public static void shutdown() {
        synchronized (initLock) {
            if (defaultInstance != null) {
                getDefaultInstance().shutdown();
                defaultInstance = null;
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
        defaultInstance = null;
    }

    /**
     * Detaches this member from the cluster first and then restarts it
     * as a new member.
     */
    public static void restart() {
        synchronized (initLock) {
            if (defaultInstance != null) {
                defaultInstance = com.hazelcast.impl.FactoryImpl.restart((FactoryImpl.HazelcastInstanceProxy) defaultInstance);
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
     */
    public static Collection<Instance> getInstances() {
        return getDefaultInstance().getInstances();
    }

    public static void addInstanceListener(InstanceListener instanceListener) {
        getDefaultInstance().addInstanceListener(instanceListener);
    }

    public static void removeInstanceListener(InstanceListener instanceListener) {
        getDefaultInstance().removeInstanceListener(instanceListener);
    }

    /**
     * Creates a new Hazelcast instance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p/>
     * To shutdown all running hazelcast instances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param config Configuration for the new hazelcast instance (member)
     * @return new hazelcast instance
     * @see #shutdownAll()
     */
    public static HazelcastInstance newHazelcastInstance(Config config) {
        return com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(config);
    }
}
