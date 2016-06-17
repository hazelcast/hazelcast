/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.ClientServiceProxy;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.Config;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.internal.diagnostics.HealthMonitor;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.ServiceNotFoundException;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.impl.xa.XAService;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;
import static com.hazelcast.util.Preconditions.checkNotNull;

@PrivateApi
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class HazelcastInstanceImpl implements HazelcastInstance {

    @SuppressWarnings("checkstyle:visibilitymodifier")
    public final Node node;

    final ILogger logger;

    final String name;

    final ManagementService managementService;

    final LifecycleServiceImpl lifecycleService;

    final ManagedContext managedContext;

    final ConcurrentMap<String, Object> userContext = new ConcurrentHashMap<String, Object>();

    final HealthMonitor healthMonitor;

    protected HazelcastInstanceImpl(String name, Config config, NodeContext nodeContext)
            throws Exception {
        this.name = name;
        this.lifecycleService = new LifecycleServiceImpl(this);

        ManagedContext configuredManagedContext = config.getManagedContext();
        managedContext = new HazelcastManagedContext(this, configuredManagedContext);

        //we are going to copy the user-context map of the Config so that each HazelcastInstance will get its own
        //user-context map instance instead of having a shared map instance. So changes made to the user-context map
        //in one HazelcastInstance will not reflect on other the user-context of other HazelcastInstances.
        userContext.putAll(config.getUserContext());
        node = createNode(config, nodeContext);

        try {
            logger = node.getLogger(getClass().getName());
            lifecycleService.fireLifecycleEvent(STARTING);

            node.start();
            if (!node.isRunning()) {
                throw new IllegalStateException("Node failed to start!");
            }

            managementService = new ManagementService(this);
            initManagedContext(configuredManagedContext);

            this.healthMonitor = new HealthMonitor(node).start();
        } catch (Throwable e) {
            try {
                // Terminate the node by terminating node engine,
                // connection manager, multicast service, operation threads, etc ... if they are exist
                node.shutdown(true);
            } catch (Throwable ignored) {
                EmptyStatement.ignore(ignored);
            }
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected Node createNode(Config config, NodeContext nodeContext) {
        return new Node(this, config, nodeContext);
    }

    private void initManagedContext(ManagedContext configuredManagedContext) {
        if (configuredManagedContext != null) {
            if (configuredManagedContext instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) configuredManagedContext).setHazelcastInstance(this);
            }
        }
    }

    public ManagementService getManagementService() {
        return managementService;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public <K, V> IMap<K, V> getMap(String name) {
        checkNotNull(name, "Retrieving a map instance with a null name is not allowed!");
        return getDistributedObject(MapService.SERVICE_NAME, name);
    }

    @Override
    public <E> IQueue<E> getQueue(String name) {
        checkNotNull(name, "Retrieving a queue instance with a null name is not allowed!");
        return getDistributedObject(QueueService.SERVICE_NAME, name);
    }

    @Override
    public <E> ITopic<E> getTopic(String name) {
        checkNotNull(name, "Retrieving a topic instance with a null name is not allowed!");
        return getDistributedObject(TopicService.SERVICE_NAME, name);
    }

    @Override
    public <E> ITopic<E> getReliableTopic(String name) {
        checkNotNull(name, "Retrieving a topic instance with a null name is not allowed!");
        return getDistributedObject(ReliableTopicService.SERVICE_NAME, name);
    }

    @Override
    public <E> ISet<E> getSet(String name) {
        checkNotNull(name, "Retrieving a set instance with a null name is not allowed!");
        return getDistributedObject(SetService.SERVICE_NAME, name);
    }

    @Override
    public <E> IList<E> getList(String name) {
        checkNotNull(name, "Retrieving a list instance with a null name is not allowed!");
        return getDistributedObject(ListService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        checkNotNull(name, "Retrieving a multi-map instance with a null name is not allowed!");
        return getDistributedObject(MultiMapService.SERVICE_NAME, name);
    }

    @Override
    public JobTracker getJobTracker(String name) {
        checkNotNull(name, "Retrieving a job tracker instance with a null name is not allowed!");
        return getDistributedObject(MapReduceService.SERVICE_NAME, name);
    }

    @Override
    public <E> Ringbuffer<E> getRingbuffer(String name) {
        checkNotNull(name, "Retrieving a ringbuffer instance with a null name is not allowed!");
        return getDistributedObject(RingbufferService.SERVICE_NAME, name);
    }

    @Override
    public ILock getLock(String key) {
        checkNotNull(key, "Retrieving a lock instance with a null key is not allowed!");
        return getDistributedObject(LockService.SERVICE_NAME, key);
    }

    @Override
    public <T> T executeTransaction(TransactionalTask<T> task)
            throws TransactionException {
        return executeTransaction(TransactionOptions.getDefault(), task);
    }

    @Override
    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task)
            throws TransactionException {
        TransactionManagerService transactionManagerService = node.nodeEngine.getTransactionManagerService();
        return transactionManagerService.executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return newTransactionContext(TransactionOptions.getDefault());
    }

    @Override
    public TransactionContext newTransactionContext(TransactionOptions options) {
        TransactionManagerService transactionManagerService = node.nodeEngine.getTransactionManagerService();
        return transactionManagerService.newTransactionContext(options);
    }

    @Override
    public IExecutorService getExecutorService(String name) {
        checkNotNull(name, "Retrieving an executor instance with a null name is not allowed!");
        return getDistributedObject(DistributedExecutorService.SERVICE_NAME, name);
    }

    @Override
    public IdGenerator getIdGenerator(String name) {
        checkNotNull(name, "Retrieving an id-generator instance with a null name is not allowed!");
        return getDistributedObject(IdGeneratorService.SERVICE_NAME, name);
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        checkNotNull(name, "Retrieving an atomic-long instance with a null name is not allowed!");
        return getDistributedObject(AtomicLongService.SERVICE_NAME, name);
    }

    @Override
    public <E> IAtomicReference<E> getAtomicReference(String name) {
        checkNotNull(name, "Retrieving an atomic-reference instance with a null name is not allowed!");
        return getDistributedObject(AtomicReferenceService.SERVICE_NAME, name);
    }

    @Override
    public ICountDownLatch getCountDownLatch(String name) {
        checkNotNull(name, "Retrieving a countdown-latch instance with a null name is not allowed!");
        return getDistributedObject(CountDownLatchService.SERVICE_NAME, name);
    }

    @Override
    public ISemaphore getSemaphore(String name) {
        checkNotNull(name, "Retrieving a semaphore instance with a null name is not allowed!");
        return getDistributedObject(SemaphoreService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(String name) {
        checkNotNull(name, "Retrieving a replicated map instance with a null name is not allowed!");
        return getDistributedObject(ReplicatedMapService.SERVICE_NAME, name);
    }

    @Override
    public <T extends Closeable & ICache> T getCache(String name) {
        return (T) getCache1(name);
    }

    public <K, V> ICache<K, V> getCache1(String name) {
        checkNotNull(name, "Retrieving a cache instance with a null name is not allowed!");
        return getCacheByFullName(HazelcastCacheManager.CACHE_MANAGER_PREFIX + name);
    }

    public <K, V> ICache<K, V> getCacheByFullName(String fullName) {
        checkNotNull(fullName, "Retrieving a cache instance with a null name is not allowed!");
        try {
            return getDistributedObject(ICacheService.SERVICE_NAME, fullName);
        } catch (HazelcastException e) {
            if (e.getCause() instanceof ServiceNotFoundException) {
                throw new IllegalStateException(ICacheService.CACHE_SUPPORT_NOT_AVAILABLE_ERROR_MESSAGE);
            } else {
                throw e;
            }
        }
    }

    @Override
    public Cluster getCluster() {
        return node.clusterService;
    }

    @Override
    public Member getLocalEndpoint() {
        return node.clusterService.getLocalMember();
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        ProxyService proxyService = node.nodeEngine.getProxyService();
        return proxyService.getAllDistributedObjects();
    }

    @Override
    public Config getConfig() {
        return node.getConfig();
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return userContext;
    }

    @Override
    public PartitionService getPartitionService() {
        return node.partitionService.getPartitionServiceProxy();
    }

    @Override
    public QuorumService getQuorumService() {
        return node.nodeEngine.getQuorumService();
    }

    @Override
    public ClientService getClientService() {
        return new ClientServiceProxy(node);
    }

    @Override
    public LoggingService getLoggingService() {
        return node.loggingService;
    }

    @Override
    public LifecycleServiceImpl getLifecycleService() {
        return lifecycleService;
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
        ProxyService proxyService = node.nodeEngine.getProxyService();
        return (T) proxyService.getDistributedObject(serviceName, name);
    }

    @Override
    public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        final ProxyService proxyService = node.nodeEngine.getProxyService();
        return proxyService.addProxyListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(String registrationId) {
        final ProxyService proxyService = node.nodeEngine.getProxyService();
        return proxyService.removeProxyListener(registrationId);
    }

    public InternalSerializationService getSerializationService() {
        return node.getSerializationService();
    }

    public MemoryStats getMemoryStats() {
        return node.getNodeExtension().getMemoryStats();
    }

    @Override
    public HazelcastXAResource getXAResource() {
        return getDistributedObject(XAService.SERVICE_NAME, XAService.SERVICE_NAME);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof HazelcastInstance)) {
            return false;
        }

        HazelcastInstance that = (HazelcastInstance) o;
        return !(name != null ? !name.equals(that.getName()) : that.getName() != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "HazelcastInstance{name='" + name + "', node=" + node.getThisAddress() + '}';
    }
}
