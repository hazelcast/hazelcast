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

package com.hazelcast.instance;

import com.hazelcast.client.ClientServiceProxy;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockProxy;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.Config;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
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
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.management.ThreadMonitoringService;
import com.hazelcast.map.MapService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.queue.QueueService;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.topic.TopicService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.util.HealthMonitor;
import com.hazelcast.util.HealthMonitorLevel;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;

@SuppressWarnings("unchecked")
@PrivateApi
public final class HazelcastInstanceImpl
        implements HazelcastInstance {

    public final Node node;

    final ILogger logger;

    final String name;

    final ManagementService managementService;

    final LifecycleServiceImpl lifecycleService;

    final ManagedContext managedContext;

    final ThreadMonitoringService threadMonitoringService;

    final ThreadGroup threadGroup;

    final ConcurrentMap<String, Object> userContext = new ConcurrentHashMap<String, Object>();

    HazelcastInstanceImpl(String name, Config config, NodeContext nodeContext)
            throws Exception {
        this.name = name;
        this.threadGroup = new ThreadGroup(name);
        threadMonitoringService = new ThreadMonitoringService(threadGroup);
        lifecycleService = new LifecycleServiceImpl(this);
        ManagedContext configuredManagedContext = config.getManagedContext();
        managedContext = new HazelcastManagedContext(this, configuredManagedContext);

        //we are going to copy the user-context map of the Config so that each HazelcastInstance will get its own
        //user-context map instance instead of having a shared map instance. So changes made to the user-context map
        //in one HazelcastInstance will not reflect on other the user-context of other HazelcastInstances.
        userContext.putAll(config.getUserContext());
        node = new Node(this, config, nodeContext);
        logger = node.getLogger(getClass().getName());
        lifecycleService.fireLifecycleEvent(STARTING);
        node.start();
        if (!node.isActive()) {
            node.connectionManager.shutdown();
            throw new IllegalStateException("Node failed to start!");
        }
        managementService = new ManagementService(this);

        if (configuredManagedContext != null) {
            if (configuredManagedContext instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) configuredManagedContext).setHazelcastInstance(this);
            }
        }

        initHealthMonitor();
    }

    private void initHealthMonitor() {
        String healthMonitorLevelString = node.getGroupProperties().HEALTH_MONITORING_LEVEL.getString();
        HealthMonitorLevel healthLevel = HealthMonitorLevel.valueOf(healthMonitorLevelString);
        if (healthLevel != HealthMonitorLevel.OFF) {
            logger.finest("Starting health monitor");
            int delaySeconds = node.getGroupProperties().HEALTH_MONITORING_DELAY_SECONDS.getInteger();
            new HealthMonitor(this, healthLevel, delaySeconds).start();
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
        if (name == null) {
            throw new NullPointerException("Retrieving a map instance with a null name is not allowed!");
        }
        return getDistributedObject(MapService.SERVICE_NAME, name);
    }

    @Override
    public <E> IQueue<E> getQueue(String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving a queue instance with a null name is not allowed!");
        }
        return getDistributedObject(QueueService.SERVICE_NAME, name);
    }

    @Override
    public <E> ITopic<E> getTopic(String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving a topic instance with a null name is not allowed!");
        }
        return getDistributedObject(TopicService.SERVICE_NAME, name);
    }

    @Override
    public <E> ISet<E> getSet(String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving a set instance with a null name is not allowed!");
        }
        return getDistributedObject(SetService.SERVICE_NAME, name);
    }

    @Override
    public <E> IList<E> getList(String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving a list instance with a null name is not allowed!");
        }
        return getDistributedObject(ListService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving a multi-map instance with a null name is not allowed!");
        }
        return getDistributedObject(MultiMapService.SERVICE_NAME, name);
    }

    @Override
    public JobTracker getJobTracker(String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving a job tracker instance with a null name is not allowed!");
        }
        return getDistributedObject(MapReduceService.SERVICE_NAME, name);
    }

    @Deprecated
    public ILock getLock(Object key) {
        //this method will be deleted in the near future.
        if (key == null) {
            throw new NullPointerException("Retrieving a lock instance with a null key is not allowed!");
        }
        String name = LockProxy.convertToStringKey(key, node.getSerializationService());
        return getLock(name);
    }

    @Override
    public ILock getLock(String key) {
        if (key == null) {
            throw new NullPointerException("Retrieving a lock instance with a null key is not allowed!");
        }
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
    public IExecutorService getExecutorService(final String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving an executor instance with a null name is not allowed!");
        }
        return getDistributedObject(DistributedExecutorService.SERVICE_NAME, name);
    }

    @Override
    public IdGenerator getIdGenerator(final String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving an id-generator instance with a null name is not allowed!");
        }
        return getDistributedObject(IdGeneratorService.SERVICE_NAME, name);
    }

    @Override
    public IAtomicLong getAtomicLong(final String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving an atomic-long instance with a null name is not allowed!");
        }
        return getDistributedObject(AtomicLongService.SERVICE_NAME, name);
    }

    @Override
    public <E> IAtomicReference<E> getAtomicReference(final String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving an atomic-reference instance with a null name is not allowed!");
        }
        return getDistributedObject(AtomicReferenceService.SERVICE_NAME, name);
    }

    @Override
    public ICountDownLatch getCountDownLatch(final String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving a countdown-latch instance with a null name is not allowed!");
        }
        return getDistributedObject(CountDownLatchService.SERVICE_NAME, name);
    }

    @Override
    public ISemaphore getSemaphore(final String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving a semaphore instance with a null name is not allowed!");
        }
        return getDistributedObject(SemaphoreService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(final String name) {
        if (name == null) {
            throw new NullPointerException("Retrieving a replicated map instance with a null name is not allowed!");
        }
        return getDistributedObject(ReplicatedMapService.SERVICE_NAME, name);
    }

    @Override
    public Cluster getCluster() {
        return node.clusterService.getClusterProxy();
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
    @Deprecated
    public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
        if (id instanceof String) {
            return (T) node.nodeEngine.getProxyService().getDistributedObject(serviceName, (String) id);
        }
        throw new IllegalArgumentException("'id' must be type of String!");
    }

    @Override
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

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    public SerializationService getSerializationService() {
        return node.getSerializationService();
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
        final StringBuilder sb = new StringBuilder();
        sb.append("HazelcastInstance");
        sb.append("{name='").append(name).append('\'');
        sb.append(", node=").append(node.getThisAddress());
        sb.append('}');
        return sb.toString();
    }
}
