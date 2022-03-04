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

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.client.ClientService;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.spi.impl.SerializationServiceSupport;
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
 * A proxy around the actual HazelcastInstanceImpl. This class serves 2 purposes:
 * <ol>
 * <li>
 * if the HazelcastInstance is shut down, the reference to the original HazelcastInstanceImpl is nulled and
 * this HazelcastInstanceImpl and all its dependencies can be GCed. If the HazelcastInstanceImpl would
 * be exposed directly, it could still retain unusable objects due to its not-null fields.</li>
 * <li>
 * it provides a barrier for accessing the HazelcastInstanceImpl internals. Otherwise a simple cast to HazelcastInstanceImpl
 * would be sufficient but now a bit of reflection is needed to get there.
 * </li>
 * </ol>
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public final class HazelcastInstanceProxy implements HazelcastInstance, SerializationServiceSupport {

    protected volatile HazelcastInstanceImpl original;

    private final String name;

    protected HazelcastInstanceProxy(HazelcastInstanceImpl original) {
        this.original = original;
        name = original.getName();
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return getOriginal().getMap(name);
    }

    @Nonnull
    @Override
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        return getOriginal().getQueue(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        return getOriginal().getTopic(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        return getOriginal().getReliableTopic(name);
    }

    @Nonnull
    @Override
    public <E> ISet<E> getSet(@Nonnull String name) {
        return getOriginal().getSet(name);
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return getOriginal().getList(name);
    }

    @Nonnull
    @Override
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        return getOriginal().getMultiMap(name);
    }

    @Nonnull
    @Override
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        return getOriginal().getRingbuffer(name);
    }

    @Nonnull
    @Override
    public IExecutorService getExecutorService(@Nonnull String name) {
        return getOriginal().getExecutorService(name);
    }

    @Nonnull
    @Override
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        return getOriginal().getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
        return getOriginal().executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        return getOriginal().executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return getOriginal().newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return getOriginal().newTransactionContext(options);
    }

    @Nonnull
    @Override
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        return getOriginal().getFlakeIdGenerator(name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return getOriginal().getReplicatedMap(name);
    }

    @Override
    public ICacheManager getCacheManager() {
        return getOriginal().getCacheManager();
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return getOriginal().getCluster();
    }

    @Nonnull
    @Override
    public Member getLocalEndpoint() {
        return getOriginal().getLocalEndpoint();
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return getOriginal().getDistributedObjects();
    }

    @Nonnull
    @Override
    public Config getConfig() {
        return getOriginal().getConfig();
    }

    @Nonnull
    @Override
    public PartitionService getPartitionService() {
        return getOriginal().getPartitionService();
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        return getOriginal().getSplitBrainProtectionService();
    }

    @Nonnull
    @Override
    public ClientService getClientService() {
        return getOriginal().getClientService();
    }

    @Nonnull
    @Override
    public LoggingService getLoggingService() {
        return getOriginal().getLoggingService();
    }

    @Nonnull
    @Override
    public LifecycleService getLifecycleService() {
        final HazelcastInstanceImpl hz = original;
        return hz != null ? hz.getLifecycleService() : new TerminatedLifecycleService();
    }

    @Nonnull
    @Override
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        return getOriginal().getDistributedObject(serviceName, name);
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        return getOriginal().addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        return getOriginal().removeDistributedObjectListener(registrationId);
    }

    @Nonnull
    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return getOriginal().getUserContext();
    }

    @Nonnull
    @Override
    public HazelcastXAResource getXAResource() {
        return getOriginal().getXAResource();
    }

    @Nonnull
    @Override
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        return getOriginal().getCardinalityEstimator(name);
    }

    @Nonnull
    @Override
    public PNCounter getPNCounter(@Nonnull String name) {
        return getOriginal().getPNCounter(name);
    }

    @Nonnull
    @Override
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        return getOriginal().getScheduledExecutorService(name);
    }

    @Nonnull
    @Override
    public CPSubsystem getCPSubsystem() {
        return getOriginal().getCPSubsystem();
    }

    @Nonnull
    @Override
    public SqlService getSql() {
        return getOriginal().getSql();
    }

    @Nonnull
    @Override
    public JetService getJet() {
        return getOriginal().getJet();
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return getOriginal().getSerializationService();
    }

    public HazelcastInstanceImpl getOriginal() {
        final HazelcastInstanceImpl hazelcastInstance = original;
        if (hazelcastInstance == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return hazelcastInstance;
    }

    @Override
    public String toString() {
        final HazelcastInstanceImpl hazelcastInstance = original;
        if (hazelcastInstance != null) {
            return hazelcastInstance.toString();
        }
        return "HazelcastInstance {NOT ACTIVE}";
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
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
}


