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
import com.hazelcast.core.ICacheManager;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.instance.impl.executejar.ExecuteJobParameters;
import com.hazelcast.jet.Job;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

// A special HazelcastInstance that has a BootstrappedJetProxy
@SuppressWarnings({"checkstyle:methodcount"})
public final class BootstrappedInstanceProxy implements HazelcastInstance {

    private static final ILogger LOGGER = Logger.getLogger(BootstrappedInstanceProxy.class);

    private final HazelcastInstance instance;

    private final BootstrappedJetProxy jetProxy;

    private boolean shutDownAllowed = true;

    BootstrappedInstanceProxy(HazelcastInstance instance, BootstrappedJetProxy jetProxy) {
        this.instance = instance;
        this.jetProxy = jetProxy;
    }

    public List<Job> getSubmittedJobs() {
        ExecuteJobParameters executeJobParameters = jetProxy.getExecuteJobParameters();
        return executeJobParameters.getSubmittedJobs();
    }

    public BootstrappedInstanceProxy setShutDownAllowed(boolean shutDownAllowed) {
        this.shutDownAllowed = shutDownAllowed;
        return this;
    }

    public void setExecuteJobParameters(ExecuteJobParameters executeJobParameters) {
        jetProxy.setExecuteJobParameters(executeJobParameters);
    }

    public void removeExecuteJobParameters() {
        jetProxy.removeExecuteJobParameters();
    }

    @Nonnull
    @Override
    public String getName() {
        return instance.getName();
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return instance.getMap(name);
    }

    @Nonnull
    @Override
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        return instance.getQueue(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        return instance.getTopic(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        return instance.getReliableTopic(name);
    }

    @Nonnull
    @Override
    public <E> ISet<E> getSet(@Nonnull String name) {
        return instance.getSet(name);
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return instance.getList(name);
    }

    @Nonnull
    @Override
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        return instance.getMultiMap(name);
    }

    @Nonnull
    @Override
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        return instance.getRingbuffer(name);
    }

    @Nonnull
    @Override
    public IExecutorService getExecutorService(@Nonnull String name) {
        return instance.getExecutorService(name);
    }

    @Nonnull
    @Override
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        return instance.getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
        return instance.executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        return instance.executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return instance.newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return instance.newTransactionContext(options);
    }

    @Nonnull
    @Override
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        return instance.getFlakeIdGenerator(name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return instance.getReplicatedMap(name);
    }

    @Override
    public ICacheManager getCacheManager() {
        return instance.getCacheManager();
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return instance.getCluster();
    }

    @Nonnull
    @Override
    public Endpoint getLocalEndpoint() {
        return instance.getLocalEndpoint();
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return instance.getDistributedObjects();
    }

    @Nonnull
    @Override
    public Config getConfig() {
        return instance.getConfig();
    }

    @Nonnull
    @Override
    public PartitionService getPartitionService() {
        return instance.getPartitionService();
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        return instance.getSplitBrainProtectionService();
    }

    @Nonnull
    @Override
    public ClientService getClientService() {
        return instance.getClientService();
    }

    @Nonnull
    @Override
    public LoggingService getLoggingService() {
        return instance.getLoggingService();
    }

    @Nonnull
    @Override
    public LifecycleService getLifecycleService() {
        return instance.getLifecycleService();
    }

    @Nonnull
    @Override
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        return instance.getDistributedObject(serviceName, name);
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        return instance.addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        return instance.removeDistributedObjectListener(registrationId);
    }

    @Nonnull
    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return instance.getUserContext();
    }

    @Nonnull
    @Override
    public HazelcastXAResource getXAResource() {
        return instance.getXAResource();
    }

    @Nonnull
    @Override
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        return instance.getCardinalityEstimator(name);
    }

    @Nonnull
    @Override
    public PNCounter getPNCounter(@Nonnull String name) {
        return instance.getPNCounter(name);
    }

    @Nonnull
    @Override
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        return instance.getScheduledExecutorService(name);
    }

    @Nonnull
    @Override
    public CPSubsystem getCPSubsystem() {
        return instance.getCPSubsystem();
    }

    @Nonnull
    @Override
    public SqlService getSql() {
        return instance.getSql();
    }

    @Nonnull
    @Override
    public BootstrappedJetProxy getJet() {
        return jetProxy;
    }

    @Override
    public void shutdown() {
        if (shutDownAllowed) {
            getLifecycleService().shutdown();
        } else {
            LOGGER.severe("Shutdown of BootstrappedInstanceProxy is not allowed");
        }
    }
}
