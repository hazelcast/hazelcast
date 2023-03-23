/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.client.ClientService;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Endpoint;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.jet.JetService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * An abstract proxy to a target HazelcastInstance.
 * <p/>
 * The primary reason this class exists is that there are a collection of HazelcastInstance
 * implementations that are proxies which leads to a lot of code duplication and this class
 * will get rid of all the forwarding duplication.
 *
 * @param <H>
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public abstract class AbstractHazelcastInstanceProxy<H extends HazelcastInstance>
        implements HazelcastInstance {

    protected volatile H target;

    public AbstractHazelcastInstanceProxy(H target) {
        this.target = target;
    }

    /**
     * Sets the target instance which could be <code>null</code>.
     *
     * @param target the target instance.
     */
    public void target(H target) {
        this.target = target;
    }

    /**
     * Gets the target instance which could be <code>null</code>.
     *
     * @return the target instance.
     */
    public H target() {
        return target;
    }

    /**
     * Gets the target instance. The default implementation will check
     * if the target is actually set and throw an HazelcastInstanceNotActiveException
     * if that isn't the case. Override this method for custom behavior.
     *
     * @return the target instance.
     */
    public H getTarget() {
        H instance = target;
        if (instance == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return instance;
    }

    @Override
    @Nonnull
    public String getName() {
        return getTarget().getName();
    }

    @Override
    @Nonnull
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        return getTarget().getQueue(name);
    }

    @Override
    @Nonnull
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        return getTarget().getTopic(name);
    }

    @Override
    @Nonnull
    public <E> ISet<E> getSet(@Nonnull String name) {
        return getTarget().getSet(name);
    }

    @Override
    @Nonnull
    public <E> IList<E> getList(@Nonnull String name) {
        return getTarget().getList(name);
    }

    @Override
    @Nonnull
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return getTarget().getMap(name);
    }

    @Override
    @Nonnull
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return getTarget().getReplicatedMap(name);
    }

    @Override
    @Nonnull
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        return getTarget().getMultiMap(name);
    }

    @Override
    @Nonnull
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        return getTarget().getRingbuffer(name);
    }

    @Override
    @Nonnull
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        return getTarget().getReliableTopic(name);
    }

    @Override
    @Nonnull
    public Cluster getCluster() {
        return getTarget().getCluster();
    }

    @Override
    @Nonnull
    public Endpoint getLocalEndpoint() {
        return getTarget().getLocalEndpoint();
    }

    @Override
    @Nonnull
    public IExecutorService getExecutorService(@Nonnull String name) {
        return getTarget().getExecutorService(name);
    }

    @Override
    @Nonnull
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        return getTarget().getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
        return getTarget().executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        return getTarget().executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return getTarget().newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return getTarget().newTransactionContext(options);
    }

    @Override
    @Nonnull
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        return getTarget().getFlakeIdGenerator(name);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return getTarget().getDistributedObjects();
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        return getTarget().addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        return getTarget().removeDistributedObjectListener(registrationId);
    }

    @Override
    @Nonnull
    public Config getConfig() {
        return getTarget().getConfig();
    }

    @Override
    @Nonnull
    public PartitionService getPartitionService() {
        return getTarget().getPartitionService();
    }

    @Override
    @Nonnull
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        return getTarget().getSplitBrainProtectionService();
    }

    @Override
    @Nonnull
    public ClientService getClientService() {
        return getTarget().getClientService();
    }

    @Override
    @Nonnull
    public LoggingService getLoggingService() {
        return getTarget().getLoggingService();
    }

    @Override
    @Nonnull
    public LifecycleService getLifecycleService() {
        return getTarget().getLifecycleService();
    }

    @Override
    @Nonnull
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        return getTarget().getDistributedObject(serviceName, name);
    }

    @Override
    @Nonnull
    public ConcurrentMap<String, Object> getUserContext() {
        return getTarget().getUserContext();
    }

    @Override
    @Nonnull
    public HazelcastXAResource getXAResource() {
        return getTarget().getXAResource();
    }

    @Override
    public ICacheManager getCacheManager() {
        return getTarget().getCacheManager();
    }

    @Override
    @Nonnull
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        return getTarget().getCardinalityEstimator(name);
    }

    @Override
    @Nonnull
    public PNCounter getPNCounter(@Nonnull String name) {
        return getTarget().getPNCounter(name);
    }

    @Override
    @Nonnull
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        return getTarget().getScheduledExecutorService(name);
    }

    @Override
    @Nonnull
    public CPSubsystem getCPSubsystem() {
        return getTarget().getCPSubsystem();
    }

    @Override
    @Nonnull
    public SqlService getSql() {
        return getTarget().getSql();
    }

    @Override
    @Nonnull
    public JetService getJet() {
        return getTarget().getJet();
    }

    @Override
    public void shutdown() {
        getTarget().shutdown();
    }
}
