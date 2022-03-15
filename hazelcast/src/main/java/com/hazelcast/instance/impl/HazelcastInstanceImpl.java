/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import com.hazelcast.cache.impl.HazelcastInstanceCacheManager;
import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.client.ClientService;
import com.hazelcast.client.impl.ClientServiceProxy;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.internal.CPSubsystemImpl;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.impl.xa.XAService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class HazelcastInstanceImpl implements HazelcastInstance, SerializationServiceSupport {

    @SuppressWarnings("checkstyle:visibilitymodifier")
    public final Node node;

    final ConcurrentMap<String, Object> userContext = new ConcurrentHashMap<>();

    final ILogger logger;
    final String name;
    final ManagementService managementService;
    final LifecycleServiceImpl lifecycleService;
    final CPSubsystemImpl cpSubsystem;
    final ManagedContext managedContext;
    final HazelcastInstanceCacheManager hazelcastCacheManager;

    @SuppressWarnings("checkstyle:executablestatementcount")
    protected HazelcastInstanceImpl(String name, Config config, NodeContext nodeContext) {
        this.name = name;
        this.lifecycleService = new LifecycleServiceImpl(this);

        ManagedContext configuredManagedContext = config.getManagedContext();
        this.managedContext = new HazelcastManagedContext(this, configuredManagedContext);

        // we are going to copy the user-context map of the Config so that each HazelcastInstance will get its own
        // user-context map instance instead of having a shared map instance. So changes made to the user-context map
        // in one HazelcastInstance will not reflect on other the user-context of other HazelcastInstances
        this.userContext.putAll(config.getUserContext());
        this.node = createNode(config, nodeContext);
        this.cpSubsystem = new CPSubsystemImpl(this);

        try {
            this.logger = node.getLogger(getClass().getName());

            node.start();
            if (!node.isRunning()) {
                throw new IllegalStateException("Node failed to start!");
            }

            this.managementService = node.getNodeExtension().createJMXManagementService(this);
            initManagedContext(configuredManagedContext);

            this.hazelcastCacheManager = new HazelcastInstanceCacheManager(this);
            ClassLoader classLoader = node.getConfigClassLoader();
            if (classLoader instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) classLoader).setHazelcastInstance(this);
            }
        } catch (Throwable e) {
            try {
                // terminate the node by terminating the NodeEngine, ConnectionManager, services, operation threads etc.
                node.shutdown(true);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
            throw rethrow(e);
        }
    }

    private Node createNode(Config config, NodeContext nodeContext) {
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

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        checkNotNull(name, "Retrieving a map instance with a null name is not allowed!");
        return getDistributedObject(MapService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        checkNotNull(name, "Retrieving a queue instance with a null name is not allowed!");
        return getDistributedObject(QueueService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        checkNotNull(name, "Retrieving a topic instance with a null name is not allowed!");
        return getDistributedObject(TopicService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        checkNotNull(name, "Retrieving a topic instance with a null name is not allowed!");
        return getDistributedObject(ReliableTopicService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> ISet<E> getSet(@Nonnull String name) {
        checkNotNull(name, "Retrieving a set instance with a null name is not allowed!");
        return getDistributedObject(SetService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        checkNotNull(name, "Retrieving a list instance with a null name is not allowed!");
        return getDistributedObject(ListService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        checkNotNull(name, "Retrieving a multi-map instance with a null name is not allowed!");
        return getDistributedObject(MultiMapService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        checkNotNull(name, "Retrieving a ringbuffer instance with a null name is not allowed!");
        return getDistributedObject(RingbufferService.SERVICE_NAME, name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task)
            throws TransactionException {
        return executeTransaction(TransactionOptions.getDefault(), task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        TransactionManagerService transactionManagerService = node.getNodeEngine().getTransactionManagerService();
        return transactionManagerService.executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return newTransactionContext(TransactionOptions.getDefault());
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        checkNotNull(options, "TransactionOptions must not be null!");
        TransactionManagerService transactionManagerService = node.getNodeEngine().getTransactionManagerService();
        return transactionManagerService.newTransactionContext(options);
    }

    @Nonnull
    @Override
    public IExecutorService getExecutorService(@Nonnull String name) {
        checkNotNull(name, "Retrieving an executor instance with a null name is not allowed!");
        return getDistributedObject(DistributedExecutorService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        checkNotNull(name, "Retrieving a durable executor instance with a null name is not allowed!");
        return getDistributedObject(DistributedDurableExecutorService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        checkNotNull(name, "Retrieving a Flake ID-generator instance with a null name is not allowed!");
        return getDistributedObject(FlakeIdGeneratorService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        checkNotNull(name, "Retrieving a replicated map instance with a null name is not allowed!");
        return getDistributedObject(ReplicatedMapService.SERVICE_NAME, name);
    }

    @Override
    public HazelcastInstanceCacheManager getCacheManager() {
        return hazelcastCacheManager;
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return node.getClusterService();
    }

    @Nonnull
    @Override
    public Member getLocalEndpoint() {
        return node.getLocalMember();
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        ProxyService proxyService = node.getNodeEngine().getProxyService();
        return proxyService.getAllDistributedObjects();
    }

    @Nonnull
    @Override
    public Config getConfig() {
        return node.getConfig();
    }

    @Nonnull
    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return userContext;
    }

    @Nonnull
    @Override
    public PartitionService getPartitionService() {
        return node.getPartitionService().getPartitionServiceProxy();
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        return node.getNodeEngine().getSplitBrainProtectionService();
    }

    @Nonnull
    @Override
    public ClientService getClientService() {
        return new ClientServiceProxy(node);
    }

    @Nonnull
    @Override
    public LoggingService getLoggingService() {
        return node.getLoggingService();
    }

    @Nonnull
    @Override
    public LifecycleServiceImpl getLifecycleService() {
        return lifecycleService;
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        ProxyService proxyService = node.getNodeEngine().getProxyService();
        UUID source = node.nodeEngine.getLocalMember().getUuid();
        return (T) proxyService.getDistributedObject(serviceName, name, source);
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        checkNotNull(distributedObjectListener, "DistributedObjectListener must not be null!");
        final ProxyService proxyService = node.getNodeEngine().getProxyService();
        return proxyService.addProxyListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "Registration ID must not be null!");
        final ProxyService proxyService = node.getNodeEngine().getProxyService();
        return proxyService.removeProxyListener(registrationId);
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return node.getSerializationService();
    }

    public MemoryStats getMemoryStats() {
        return node.getNodeExtension().getMemoryStats();
    }

    @Nonnull
    @Override
    public HazelcastXAResource getXAResource() {
        return getDistributedObject(XAService.SERVICE_NAME, XAService.SERVICE_NAME);
    }

    @Nonnull
    @Override
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        checkNotNull(name, "Retrieving a cardinality estimator instance with a null name is not allowed!");
        return getDistributedObject(CardinalityEstimatorService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public PNCounter getPNCounter(@Nonnull String name) {
        checkNotNull(name, "Retrieving a PN counter instance with a null name is not allowed!");
        return getDistributedObject(PNCounterService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        checkNotNull(name, "Retrieving a scheduled executor instance with a null name is not allowed!");
        return getDistributedObject(DistributedScheduledExecutorService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public CPSubsystem getCPSubsystem() {
        return cpSubsystem;
    }

    @Nonnull
    @Override
    public SqlService getSql() {
        return node.getNodeEngine().getSqlService();
    }

    @Nonnull
    @Override
    public JetService getJet() {
        return node.getNodeExtension().getJet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HazelcastInstance)) {
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
