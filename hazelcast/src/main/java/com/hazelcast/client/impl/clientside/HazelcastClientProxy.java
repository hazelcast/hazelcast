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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.client.Client;
import com.hazelcast.client.ClientService;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.instance.impl.TerminatedLifecycleService;
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
 * A client-side proxy {@link com.hazelcast.core.HazelcastInstance} instance.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class HazelcastClientProxy implements HazelcastInstance, SerializationServiceSupport {

    public volatile HazelcastClientInstanceImpl client;

    public HazelcastClientProxy(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Nonnull
    @Override
    public Config getConfig() {
        return getClient().getConfig();
    }

    @Nonnull
    @Override
    public String getName() {
        return getClient().getName();
    }

    @Nonnull
    @Override
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        return getClient().getRingbuffer(name);
    }

    @Nonnull
    @Override
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        return getClient().getQueue(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        return getClient().getTopic(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        return getClient().getReliableTopic(name);
    }

    @Nonnull
    @Override
    public <E> ISet<E> getSet(@Nonnull String name) {
        return getClient().getSet(name);
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return getClient().getList(name);
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return getClient().getMap(name);
    }

    @Nonnull
    @Override
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        return getClient().getMultiMap(name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return getClient().getReplicatedMap(name);
    }

    @Override
    public ICacheManager getCacheManager() {
        return getClient().getCacheManager();
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return getClient().getCluster();
    }

    @Nonnull
    @Override
    public Client getLocalEndpoint() {
        return getClient().getLocalEndpoint();
    }

    @Nonnull
    @Override
    public IExecutorService getExecutorService(@Nonnull String name) {
        return getClient().getExecutorService(name);
    }

    @Nonnull
    @Override
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        return getClient().getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task)
            throws TransactionException {
        return getClient().executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options, @Nonnull TransactionalTask<T> task)
            throws TransactionException {
        return getClient().executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return getClient().newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return getClient().newTransactionContext(options);
    }

    @Nonnull
    @Override
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        return getClient().getFlakeIdGenerator(name);
    }

    @Nonnull
    @Override
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        return getClient().getCardinalityEstimator(name);
    }

    @Nonnull
    @Override
    public PNCounter getPNCounter(@Nonnull String name) {
        return getClient().getPNCounter(name);
    }

    @Nonnull
    @Override
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        return getClient().getScheduledExecutorService(name);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return getClient().getDistributedObjects();
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        return getClient().addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        return getClient().removeDistributedObjectListener(registrationId);
    }

    @Nonnull
    @Override
    public PartitionService getPartitionService() {
        return getClient().getPartitionService();
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public ClientService getClientService() {
        return getClient().getClientService();
    }

    @Nonnull
    @Override
    public LoggingService getLoggingService() {
        return getClient().getLoggingService();
    }

    @Nonnull
    @Override
    public LifecycleService getLifecycleService() {
        final HazelcastClientInstanceImpl hz = client;
        return hz != null ? hz.getLifecycleService() : new TerminatedLifecycleService();
    }

    @Nonnull
    @Override
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        return getClient().getDistributedObject(serviceName, name);
    }

    @Nonnull
    @Override
    public CPSubsystem getCPSubsystem() {
        return getClient().getCPSubsystem();
    }

    @Nonnull
    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return getClient().getUserContext();
    }

    public ClientConfig getClientConfig() {
        return getClient().getClientConfig();
    }

    @Nonnull
    @Override
    public HazelcastXAResource getXAResource() {
        return getClient().getXAResource();
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return getClient().getSerializationService();
    }

    protected HazelcastClientInstanceImpl getClient() {
        final HazelcastClientInstanceImpl c = client;
        if (c == null || !c.getLifecycleService().isRunning()) {
            throw new HazelcastClientNotActiveException();
        }
        return c;
    }

    public String toString() {
        final HazelcastClientInstanceImpl hazelcastInstance = client;
        if (hazelcastInstance != null) {
            return hazelcastInstance.toString();
        }
        return "HazelcastClientInstance {NOT ACTIVE}";
    }

    @Nonnull
    @Override
    public SqlService getSql() {
        return getClient().getSql();
    }

    @Nonnull
    @Override
    public JetService getJet() {
        return client.getJet();
    }
}
