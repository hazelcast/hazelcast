/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.config.Config;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.transaction.HazelcastXAResource;
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
     * Returns the name of this Hazelcast instance.
     *
     * @return name of this Hazelcast instance
     */
    String getName();

    /**
     * Creates or returns the distributed queue instance with the specified name.
     *
     * @param name name of the distributed queue
     * @return distributed queue instance with the specified name
     */
    <E> IQueue<E> getQueue(String name);

    /**
     * Creates or returns the distributed topic instance with the specified name.
     *
     * @param name name of the distributed topic
     * @return distributed topic instance with the specified name
     */
    <E> ITopic<E> getTopic(String name);

    /**
     * Creates or returns the distributed set instance with the specified name.
     *
     * @param name name of the distributed set
     * @return distributed set instance with the specified name
     */
    <E> ISet<E> getSet(String name);

    /**
     * Creates or returns the distributed list instance with the specified name.
     * Index based operations on the list are not supported.
     *
     * @param name name of the distributed list
     * @return distributed list instance with the specified name
     */
    <E> IList<E> getList(String name);

    /**
     * Creates or returns the distributed map instance with the specified name.
     *
     * @param name name of the distributed map
     * @return distributed map instance with the specified name
     */
    <K, V> IMap<K, V> getMap(String name);

    /**
     * Creates or returns the replicated map instance with the specified name.
     *
     * @param name name of the distributed map
     * @return replicated map instance with specified name
     * @throws ReplicatedMapCantBeCreatedOnLiteMemberException if it is called on a lite member
     * @since 3.2
     */
    <K, V> ReplicatedMap<K, V> getReplicatedMap(String name);

    /**
     * Creates or returns the job tracker instance with the specified name.
     *
     * @param name name of the job tracker
     * @return job tracker instance with the specified name
     * @since 3.2
     * @deprecated MapReduce is deprecated and will be removed in 4.0.
     * For map aggregations, you can use {@link com.hazelcast.aggregation.Aggregator} on IMap.
     * For general data processing, it is superseded by <a href="http://jet.hazelcast.org">Hazelcast Jet</a>.
     */
    JobTracker getJobTracker(String name);

    /**
     * Creates or returns the distributed multimap instance with the specified name.
     *
     * @param name name of the distributed multimap
     * @return distributed multimap instance with the specified name
     */
    <K, V> MultiMap<K, V> getMultiMap(String name);

    /**
     * Creates or returns the distributed lock instance for the specified key object.
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
     * Creates or returns the distributed Ringbuffer instance with the specified name.
     *
     * @param name name of the distributed Ringbuffer
     * @return distributed RingBuffer instance with the specified name
     */
    <E> Ringbuffer<E> getRingbuffer(String name);

    /**
     * Creates or returns the reliable ReliableTopic instance with the specified name.
     *
     * @param name name of the reliable ITopic
     * @return the reliable ITopic
     */
    <E> ITopic<E> getReliableTopic(String name);

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
     * <p>
     * Returned endpoint will be a {@link Member} instance for cluster nodes
     * and a {@link Client} instance for clients.
     *
     * @return the local {@link Endpoint} which this HazelcastInstance belongs to
     * @see Member
     * @see Client
     */
    Endpoint getLocalEndpoint();

    /**
     * Creates or returns the distributed executor service for the given name.
     * Executor service enables you to run your <tt>Runnable</tt>s and <tt>Callable</tt>s
     * on the Hazelcast cluster.
     * <p>
     * <p><b>Note:</b> Note that it doesn't support {@code invokeAll/Any}
     * and doesn't have standard shutdown behavior</p>
     *
     * @param name name of the executor service
     * @return the distributed executor service for the given name
     */
    IExecutorService getExecutorService(String name);

    /**
     * Creates or returns the durable executor service for the given name.
     * DurableExecutor service enables you to run your <tt>Runnable</tt>s and <tt>Callable</tt>s
     * on the Hazelcast cluster.
     * <p>
     * <p><b>Note:</b> Note that it doesn't support {@code invokeAll/Any}
     * and doesn't have standard shutdown behavior</p>
     *
     * @param name name of the executor service
     * @return the durable executor service for the given name
     */
    DurableExecutorService getDurableExecutorService(String name);

    /**
     * Executes the given transactional task in current thread using default options
     * and returns the result of the task.
     *
     * @param task the transactional task to be executed
     * @param <T>  return type of task
     * @return result of the transactional task
     * @throws TransactionException if an error occurs during transaction.
     */
    <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException;

    /**
     * Executes the given transactional task in current thread using given options
     * and returns the result of the task.
     *
     * @param options options for this transactional task
     * @param task    task to be executed
     * @param <T>     return type of task
     * @return result of the transactional task
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
     * Creates or returns a cluster-wide unique ID generator. Generated IDs are {@code long} primitive values
     * between <tt>0</tt> and <tt>Long.MAX_VALUE</tt>. ID generation occurs almost at the speed of
     * local <tt>AtomicLong.incrementAndGet()</tt>. Generated IDs are unique during the life
     * cycle of the cluster. If the entire cluster is restarted, IDs start from <tt>0</tt> again.
     *
     * @param name name of the {@link IdGenerator}
     * @return IdGenerator for the given name
     *
     * @deprecated The implementation can produce duplicate IDs in case of network split, even
     * with split-brain protection enabled (during short window while split-brain is detected).
     * Use {@link #getFlakeIdGenerator(String)} for an alternative implementation which does not
     * suffer from this problem.
     */
    @Deprecated
    IdGenerator getIdGenerator(String name);

    /**
     * Creates or returns a cluster-wide unique ID generator. Generated IDs are {@code long} primitive values
     * and are k-ordered (roughly ordered). IDs are in the range from {@code 0} to {@code
     * Long.MAX_VALUE}.
     * <p>
     * The IDs contain timestamp component and a node ID component, which is assigned when the member
     * joins the cluster. This allows the IDs to be ordered and unique without any coordination between
     * members, which makes the generator safe even in split-brain scenario (for caveats,
     * {@link com.hazelcast.internal.cluster.ClusterService#getMemberListJoinVersion() see here}).
     * <p>
     * For more details and caveats, see class documentation for {@link FlakeIdGenerator}.
     * <p>
     * Note: this implementation doesn't share namespace with {@link #getIdGenerator(String)}.
     * That is, {@code getIdGenerator("a")} is distinct from {@code getFlakeIdGenerator("a")}.
     *
     * @param name name of the {@link FlakeIdGenerator}
     * @return FlakeIdGenerator for the given name
     */
    FlakeIdGenerator getFlakeIdGenerator(String name);

    /**
     * Creates or returns a cluster-wide atomic long. Hazelcast {@link IAtomicLong} is distributed
     * implementation of <tt>java.util.concurrent.atomic.AtomicLong</tt>.
     *
     * @param name name of the {@link IAtomicLong} proxy
     * @return IAtomicLong proxy for the given name
     */
    IAtomicLong getAtomicLong(String name);

    /**
     * Creates or returns a cluster-wide atomic reference. Hazelcast {@link IAtomicReference} is distributed
     * implementation of <tt>java.util.concurrent.atomic.AtomicReference</tt>.
     *
     * @param name name of the {@link IAtomicReference} proxy
     * @return {@link IAtomicReference} proxy for the given name
     */
    <E> IAtomicReference<E> getAtomicReference(String name);

    /**
     * Creates or returns a cluster-wide CountDownLatch. Hazelcast {@link ICountDownLatch} is distributed
     * implementation of <tt>java.util.concurrent.CountDownLatch</tt>.
     *
     * @param name name of the {@link ICountDownLatch} proxy
     * @return {@link ICountDownLatch} proxy for the given name
     */
    ICountDownLatch getCountDownLatch(String name);

    /**
     * Creates or returns a cluster-wide semaphore. Hazelcast {@link ISemaphore} is distributed
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
     * @return returns registration ID
     */
    String addDistributedObjectListener(DistributedObjectListener distributedObjectListener);

    /**
     * Removes the specified Distributed Object listener. Returns silently
     * if the specified instance listener does not exist.
     *
     * @param registrationId ID of listener registration
     * @return {@code true} if registration is removed, {@code false} otherwise
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
     * Returns the quorum service of this Hazelcast instance.
     * <p>
     * Quorum service can be used to retrieve quorum callbacks which let you to notify quorum results of your own to
     * the cluster quorum service.
     *
     * IMPORTANT: The term "quorum" simply refers to the count of members in the cluster required for an operation to succeed.
     * It does NOT refer to an implementation of Paxos or Raft protocols as used in many NoSQL and distributed systems.
     * The mechanism it provides in Hazelcast protects the user in case the number of nodes in a cluster drops below the
     * specified one.
     *
     * @return the quorum service of this Hazelcast instance
     */
    QuorumService getQuorumService();

    /**
     * Returns the client service of this Hazelcast instance.
     * Client service allows you to get information about connected clients.
     *
     * @return the {@link ClientService} of this Hazelcast instance.
     */
    ClientService getClientService();

    /**
     * Returns the logging service of this Hazelcast instance.
     * <p>
     * LoggingService allows you to listen for LogEvents generated by Hazelcast runtime.
     * You can log the events somewhere or take action based on the message.
     *
     * @return the logging service of this Hazelcast instance
     */
    LoggingService getLoggingService();

    /**
     * Returns the lifecycle service for this instance.
     * <p>
     * LifecycleService allows you to shutdown this HazelcastInstance and listen for the lifecycle events.
     *
     * @return the lifecycle service for this instance
     */
    LifecycleService getLifecycleService();

    /**
     * @param serviceName name of the service
     * @param name        name of the object
     * @param <T>         type of the DistributedObject
     * @return DistributedObject created by the service
     */
    <T extends DistributedObject> T getDistributedObject(String serviceName, String name);

    /**
     * Returns a ConcurrentMap that can be used to add user-context to the HazelcastInstance. This can be used
     * to store dependencies that otherwise are hard to obtain. HazelcastInstance can be
     * obtained by implementing a {@link HazelcastInstanceAware} interface when submitting a Runnable/Callable to
     * Hazelcast ExecutorService. By storing the dependencies in the user-context, they can be retrieved as soon
     * as you have a reference to the HazelcastInstance.
     * <p>
     * This structure is purely local and Hazelcast remains agnostic abouts its content.
     *
     * @return a ConcurrentMap that can be used to add user-context to the HazelcastInstance.
     */
    ConcurrentMap<String, Object> getUserContext();

    /**
     * Gets xaResource which will participate in XATransaction.
     *
     * @return the xaResource
     */
    HazelcastXAResource getXAResource();

    /**
     * Obtain the {@link ICacheManager} that provides access to JSR-107 (JCache) caches configured on a Hazelcast cluster.
     * <p>
     * Note that this method does not return a JCache {@code CacheManager}; to obtain a JCache
     * {@link javax.cache.CacheManager} use JCache standard API.
     *
     * @return the Hazelcast {@link ICacheManager}
     * @see ICacheManager
     */
    ICacheManager getCacheManager();

    /**
     * Obtain a {@link CardinalityEstimator} with the given name.
     * <p>
     * The estimator can be used to efficiently estimate the cardinality of <strong>unique</strong> entities
     * in big data sets, without the need of storing them.
     * <p>
     * The estimator is based on a HyperLogLog++ data-structure.
     *
     * @param name the name of the estimator
     * @return a {@link CardinalityEstimator}
     */
    CardinalityEstimator getCardinalityEstimator(String name);

    /**
     * Creates or returns a {@link com.hazelcast.crdt.pncounter.PNCounter} with the given
     * name.
     * <p>
     * The PN counter can be used as a counter with strong eventual consistency
     * guarantees - if operations to the counters stop, the counter values
     * of all replicas that can communicate with each other should eventually
     * converge to the same value.
     *
     * @param name the name of the PN counter
     * @return a {@link com.hazelcast.crdt.pncounter.PNCounter}
     */
    PNCounter getPNCounter(String name);

    /**
     * Creates or returns the {@link IScheduledExecutorService} scheduled executor service for the given name.
     * ScheduledExecutor service enables you to schedule your <tt>Runnable</tt>s and <tt>Callable</tt>s
     * on the Hazelcast cluster.
     *
     * @param name name of the executor service
     * @return the scheduled executor service for the given name
     */
    IScheduledExecutorService getScheduledExecutorService(String name);

    /**
     * Shuts down this HazelcastInstance. For more information see {@link com.hazelcast.core.LifecycleService#shutdown()}.
     */
    void shutdown();
}
