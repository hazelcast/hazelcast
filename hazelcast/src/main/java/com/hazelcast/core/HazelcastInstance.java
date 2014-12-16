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
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * Hazelcast instance. Each Hazelcast instance is a member (node) in a cluster.
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
     * Returns the replicated map instance with the specified name.
     *
     * @param name name of the distributed map
     * @return replicated map instance with specified name
     * @since 3.2
     */
    <K, V> ReplicatedMap<K, V> getReplicatedMap(String name);

    /**
     * Returns the job tracker instance with the specified name.
     *
     * @param name name of the job tracker
     * @return job tracker instance with the specified name
     * @since 3.2
     */
    JobTracker getJobTracker(String name);

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
     * members go down, the cluster will keep your locks safe and available.
     * Moreover, when a member leaves the cluster, all the locks acquired
     * by this dead member will be removed so that these locks can be
     * available for live members immediately.
     * <pre>
     * Lock lock = hazelcastInstance.getLock("PROCESS_LOCK");
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
    ILock getLock(String key);

    /**
     * @deprecated will be removed in Hazelcast 3.2. Use {@link #getLock(String)} instead.
     */
    @Deprecated
    ILock getLock(Object key);

    /**
     * Returns the Cluster that this Hazelcast instance is part of.
     * Cluster interface allows you to add a listener for membership
     * events and to learn more about the cluster that this Hazelcast
     * instance is part of.
     *
     * @return the cluster that this Hazelcast instance is part of
     */
    Cluster getCluster();


    /**
     * Returns the local Endpoint which this HazelcastInstance belongs to.
     * <p/>
     *
     * Returned endpoint will be a {@link Member} instance for cluster nodes
     * and a {@link Client} instance for clients.
     *
     * @see Member
     * @see Client
     *
     * @return the local {@link Endpoint} which this HazelcastInstance belongs to
     */
    Endpoint getLocalEndpoint();

    /**
     * Returns the distributed executor service for the given
     * name.
     * Executor service enables you to run your <tt>Runnable</tt>s and <tt>Callable</tt>s
     * on the Hazelcast cluster.
     * <p/>
     * <p><b>Note:</b> Note that it don't support invokeAll/Any
     * and don't have standard shutdown behavior</p>
     *
     * @param name name of the executor service
     * @return the distributed executor service for the given name
     */
    IExecutorService getExecutorService(String name);

    /**
     * Executes the given transactional task in current thread using default options
     * and returns the result of the task.
     *
     * @param task the transactional task to be executed
     * @param <T> return type of task
     * @return result of the transactional task
     *
     * @throws TransactionException if an error occurs during transaction.
     */
    <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException;

    /**
     * Executes the given transactional task in current thread using given options
     * and returns the result of the task.
     *
     * @param options options for this transactional task
     * @param task task to be executed
     * @param <T> return type of task
     * @return result of the transactional task
     *
     * @throws TransactionException if an error occurs during transaction.
     */
    <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException;

    /**
     * Creates a new TransactionContext associated with the current thread using default options.
     *
     * @return new TransactionContext associated with the current thread
     */
    TransactionContext newTransactionContext();

    /**
     * Creates a new TransactionContext associated with the current thread with given options.
     *
     * @param options options for this transaction
     * @return new TransactionContext associated with the current thread
     */
    TransactionContext newTransactionContext(TransactionOptions options);

    /**
     * Creates cluster-wide unique IDs. Generated IDs are long type primitive values
     * between <tt>0</tt> and <tt>Long.MAX_VALUE</tt> . Id generation occurs almost at the speed of
     * <tt>AtomicLong.incrementAndGet()</tt> . Generated IDs are unique during the life
     * cycle of the cluster. If the entire cluster is restarted, IDs start from <tt>0</tt> again.
     *
     * @param name name of the {@link IdGenerator}
     * @return IdGenerator for the given name
     */
    IdGenerator getIdGenerator(String name);

    /**
     * Creates cluster-wide atomic long. Hazelcast {@link IAtomicLong} is distributed
     * implementation of <tt>java.util.concurrent.atomic.AtomicLong</tt>.
     *
     * @param name name of the {@link IAtomicLong} proxy
     * @return IAtomicLong proxy for the given name
     */
    IAtomicLong getAtomicLong(String name);

    /**
     * Creates cluster-wide atomic reference. Hazelcast {@link IAtomicReference} is distributed
     * implementation of <tt>java.util.concurrent.atomic.AtomicReference</tt>.
     *
     * @param name name of the {@link IAtomicReference} proxy
     * @return {@link IAtomicReference} proxy for the given name
     */
    <E> IAtomicReference<E> getAtomicReference(String name);

    /**
     * Creates cluster-wide CountDownLatch. Hazelcast {@link ICountDownLatch} is distributed
     * implementation of <tt>java.util.concurrent.CountDownLatch</tt>.
     *
     * @param name name of the {@link ICountDownLatch} proxy
     * @return {@link ICountDownLatch} proxy for the given name
     */
    ICountDownLatch getCountDownLatch(String name);

    /**
     * Creates cluster-wide semaphore. Hazelcast {@link ISemaphore} is distributed
     * implementation of <tt>java.util.concurrent.Semaphore</tt>.
     *
     * @param name name of the {@link ISemaphore} proxy
     * @return {@link ISemaphore} proxy for the given name
     */
    ISemaphore getSemaphore(String name);

    /**
     * Returns all {@link DistributedObject}'s such as; queue, map, set, list, topic, lock, multimap.
     *
     * @return the collection of instances created by Hazelcast.
     */
    Collection<DistributedObject> getDistributedObjects();

    /**
     * Adds a Distributed Object listener which will be notified when a
     * new {@link DistributedObject} will be created or destroyed.
     *
     * @param distributedObjectListener instance listener
     * @return returns registration id.
     */
    String addDistributedObjectListener(DistributedObjectListener distributedObjectListener);

    /**
     * Removes the specified Distributed Object listener. Returns silently
     * if the specified instance listener does not exist.
     *
     * @param registrationId Id of listener registration.
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
     * InternalPartitionService allows you to introspect current partitions in the
     * cluster, partition the owner members, and listen for partition migration events.
     *
     * @return the partition service of this Hazelcast instance
     */
    PartitionService getPartitionService();

    /**
     * Returns the client service of this Hazelcast instance.
     * Client service allows you to get information about connected clients.
     *
     * @return the {@link ClientService} of this Hazelcast instance.
     */
    ClientService getClientService();

    /**
     * Returns the logging service of this Hazelcast instance.
     * LoggingService allows you to listen for LogEvents
     * generated by Hazelcast runtime. You can log the events somewhere
     * or take action based on the message.
     *
     * @return the logging service of this Hazelcast instance
     */
    LoggingService getLoggingService();

    /**
     * Returns the lifecycle service for this instance. LifecycleService allows you
     * to shutdown this HazelcastInstance and listen for
     * the lifecycle events.
     *
     * @return the lifecycle service for this instance
     */
    LifecycleService getLifecycleService();

    /**
     *
     * @param serviceName name of the service
     * @param id identifier of the object
     * @param <T> type of the DistributedObject
     * @return DistributedObject created by the service
     *
     * @deprecated use {@link #getDistributedObject(String, String)} instead.
     */
    @Deprecated
    <T extends DistributedObject> T getDistributedObject(String serviceName, Object id);

    /**
     *
     * @param serviceName name of the service
     * @param name name of the object
     * @param <T> type of the DistributedObject
     * @return DistributedObject created by the service
     */
    <T extends DistributedObject> T getDistributedObject(String serviceName, String name);

    /**
     * Returns a ConcurrentMap that can be used to add user-context to the HazelcastInstance. This can be used
     * to store dependencies that otherwise are hard to obtain. HazelcastInstance can be
     * obtained by implementing a {@link HazelcastInstanceAware} interface when submitting a Runnable/Callable to
     * Hazelcast ExecutorService. By storing the dependencies in the user-context, they can be retrieved as soon
     * as you have a reference to the HazelcastInstance.
     * <p/>
     * This structure is purely local and Hazelcast remains agnostic abouts its content.
     *
     * @return a ConcurrentMap that can be used to add user-context to the HazelcastInstance.
     */
    ConcurrentMap<String, Object> getUserContext();

    /**
     * Shuts down this HazelcastInstance. For more information see {@link com.hazelcast.core.LifecycleService#shutdown()}.
     */
    void shutdown();
}
