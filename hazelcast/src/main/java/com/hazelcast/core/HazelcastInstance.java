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

package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceProxy;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * Hazelcast instance. Each Hazelcast instance is a member.
 * Multiple Hazelcast instances can be created on a JVM.
 * Each Hazelcast instance has its own socket, threads.
 *
 * @see Hazelcast#newHazelcastInstance(Config config)
 */
public interface HazelcastInstance {

    /**
     * Returns the name of this Hazelcast instance
     *
     * @return name of this Hazelcast instance
     */
    String getName();

    /**
     * Returns the distributed queue instance with the specified name.
     *
     * @param name name of the distributed queue
     * @return distributed queue instance with the specified name
     */
    <E> IQueue<E> getQueue(String name);

    /**
     * Returns the distributed topic instance with the specified name.
     *
     * @param name name of the distributed topic
     * @return distributed topic instance with the specified name
     */
    <E> ITopic<E> getTopic(String name);

    /**
     * Returns the distributed set instance with the specified name.
     *
     * @param name name of the distributed set
     * @return distributed set instance with the specified name
     */
    <E> ISet<E> getSet(String name);

    /**
     * Returns the distributed list instance with the specified name.
     * Index based operations on the list are not supported.
     *
     * @param name name of the distributed list
     * @return distributed list instance with the specified name
     */
    <E> IList<E> getList(String name);

    /**
     * Returns the distributed map instance with the specified name.
     *
     * @param name name of the distributed map
     * @return distributed map instance with the specified name
     */
    <K, V> IMap<K, V> getMap(String name);

    /**
     * Returns the distributed multimap instance with the specified name.
     *
     * @param name name of the distributed multimap
     * @return distributed multimap instance with the specified name
     */
    <K, V> MultiMap<K, V> getMultiMap(String name);

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
     */
    ILock getLock(Object key);

    /**
     * Returns the Cluster that this Hazelcast instance is part of.
     * Cluster interface allows you to add listener for membership
     * events and learn more about the cluster that this Hazelcast
     * instance is part of.
     *
     * @return cluster that this Hazelcast instance is part of
     */
    Cluster getCluster();

    /**
     * Returns the distributed executor service for the given
     * name.
     * Executor service enables you to run your <tt>Runnable</tt>s and <tt>Callable</tt>s
     * on the Hazelcast cluster.
     *
     * <p><b>Note:</b> Note that it don't support invokeAll/Any
     * and don't have standard shutdown behavior</p>
     *
     * @param name name of the executor service
     * @return executor service for the given name
     */
    ExecutorService getExecutorService(String name);

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
     */
    Transaction getTransaction();

    /**
     * Creates cluster-wide unique IDs. Generated IDs are long type primitive values
     * between <tt>0</tt> and <tt>Long.MAX_VALUE</tt> . Id generation occurs almost at the speed of
     * <tt>AtomicLong.incrementAndGet()</tt> . Generated IDs are unique during the life
     * cycle of the cluster. If the entire cluster is restarted, IDs start from <tt>0</tt> again.
     *
     * @param name name of the IdGenerator
     * @return IdGenerator for the given name
     */
    IdGenerator getIdGenerator(String name);

    /**
     * Creates cluster-wide atomic long. Hazelcast AtomicNumber is distributed
     * implementation of <tt>java.util.concurrent.atomic.AtomicLong</tt>.
     *
     * @param name name of the AtomicNumber proxy
     * @return AtomicNumber proxy for the given name
     */
    AtomicNumber getAtomicNumber(String name);

    /**
     * Creates cluster-wide CountDownLatch. Hazelcast ICountDownLatch is distributed
     * implementation of <tt>java.util.concurrent.CountDownLatch</tt>.
     *
     * @param name name of the ICountDownLatch proxy
     * @return ICountDownLatch proxy for the given name
     */
    ICountDownLatch getCountDownLatch(String name);

    /**
     * Creates cluster-wide semaphore. Hazelcast ISemaphore is distributed
     * implementation of <tt>java.util.concurrent.Semaphore</tt>.
     *
     * @param name name of the ISemaphore proxy
     * @return ISemaphore proxy for the given name
     */
    ISemaphore getSemaphore(String name);

    /**
     * Returns all queue, map, set, list, topic, lock, multimap
     * instances created by Hazelcast.
     *
     * @return the collection of instances created by Hazelcast.
     */
    Collection<Instance> getInstances();

    /**
     * Add a instance listener which will be notified when a
     * new instance such as map, queue, multimap, topic, lock is
     * added or removed.
     *
     * @param instanceListener instance listener
     */
    void addInstanceListener(InstanceListener instanceListener);

    /**
     * Removes the specified instance listener. Returns silently
     * if specified instance listener doesn't exist.
     *
     * @param instanceListener instance listener to remove
     */
    void removeInstanceListener(InstanceListener instanceListener);

    /**
     * Returns the configuration of this Hazelcast instance.
     *
     * @return configuration of this Hazelcast instance
     */
    Config getConfig();

    /**
     * Returns the partition service of this Hazelcast instance.
     * PartitionService allows you to introspect current partitions in the
     * cluster, partition owner members and listen for partition migration events.
     *
     * @return partition service
     */
    PartitionService getPartitionService();

    /**
     * Returns the client service of this Hazelcast instance.
     * Client service allows you to get information about connected clients.
     *
     * @return
     */
    ClientService getClientService();

    /**
     * Returns the logging service of this Hazelcast instance.
     * LoggingService allows you to listen for LogEvents
     * generated by Hazelcast runtime. You can log the events somewhere
     * or take action base on the message.
     *
     * @return logging service
     */
    LoggingService getLoggingService();

    /**
     * Returns the lifecycle service for this instance. LifecycleService allows you
     * to shutdown, restart, pause and resume this HazelcastInstance and listen for
     * the lifecycle events.
     *
     * @return lifecycle service
     */
    LifecycleService getLifecycleService();


    <S extends ServiceProxy> S getServiceProxy(Class<? extends RemoteService> serviceClass, String name);


    <S extends ServiceProxy> S getServiceProxy(String serviceName, String name);


    void registerSerializer(final TypeSerializer serializer, Class type) ;


    void registerFallbackSerializer(final TypeSerializer serializer) ;

}
