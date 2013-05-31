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
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

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
    IExecutorService getExecutorService(String name);

    <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException;

    <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException;

    TransactionContext newTransactionContext();

    TransactionContext newTransactionContext(TransactionOptions options);

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
     * Creates cluster-wide atomic long. Hazelcast IAtomicLong is distributed
     * implementation of <tt>java.util.concurrent.atomic.AtomicLong</tt>.
     *
     * @param name name of the IAtomicLong proxy
     * @return IAtomicLong proxy for the given name
     */
    IAtomicLong getAtomicLong(String name);

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
    Collection<DistributedObject> getDistributedObjects();

    /**
     * Add a instance listener which will be notified when a
     * new instance such as map, queue, multimap, topic, lock is
     * added or removed.
     *
     * @param distributedObjectListener instance listener
     *
     * @return returns registration id.
     */
    String addDistributedObjectListener(DistributedObjectListener distributedObjectListener);

    /**
     * Removes the specified instance listener. Returns silently
     * if specified instance listener doesn't exist.
     *
     * @param registrationId Id of listener registration.
     *
     * @return true if registration is removed, false otherwise
     */
    boolean removeDistributedObjectListener(String registrationId);

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
     * to shutdown this HazelcastInstance and listen for
     * the lifecycle events.
     *
     * @return lifecycle service
     */
    LifecycleService getLifecycleService();


    <T extends DistributedObject> T getDistributedObject(String serviceName, Object id);


    void registerSerializer(final TypeSerializer serializer, Class type);


    void registerGlobalSerializer(final TypeSerializer serializer);

    /**
     * Returns a ConcurrentMap that can be used to add user-context to the HazelcastInstance. This can be used
     * for example to store dependencies that otherwise are hard to obtain. The HazelcastInstance can often easily
     * be obtained by making use of the HazelcastInstanceAware functionality, but non Hazelcast dependencies are without
     * this user-context hard to obtain. By storing the dependencies in the user-context, they can be retrieved as soon
     * as you have a reference to the HazelcastInstance.
     *
     * This structure is purely local and Hazelcast remains agnostic abouts its content.
     *
     * @return  the user context.
     */
    ConcurrentMap<String, Object> getUserContext();
}
