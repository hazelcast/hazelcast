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

package com.hazelcast.osgi.impl;

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
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.JetService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.osgi.HazelcastOSGiInstance;
import com.hazelcast.osgi.HazelcastOSGiService;
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
 * {@link com.hazelcast.osgi.HazelcastOSGiInstance} implementation
 * as proxy of delegated {@link com.hazelcast.core.HazelcastInstance} for getting from OSGi service.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity"})
class HazelcastOSGiInstanceImpl
        implements HazelcastOSGiInstance {

    private final HazelcastInstance delegatedInstance;
    private final HazelcastOSGiService ownerService;

    HazelcastOSGiInstanceImpl(HazelcastInstance delegatedInstance,
                              HazelcastOSGiService ownerService) {
        this.delegatedInstance = delegatedInstance;
        this.ownerService = ownerService;
    }

    @Nonnull
    @Override
    public String getName() {
        return delegatedInstance.getName();
    }

    @Nonnull
    @Override
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        return delegatedInstance.getQueue(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        return delegatedInstance.getTopic(name);
    }

    @Nonnull
    @Override
    public <E> ISet<E> getSet(@Nonnull String name) {
        return delegatedInstance.getSet(name);
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return delegatedInstance.getList(name);
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return delegatedInstance.getMap(name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return delegatedInstance.getReplicatedMap(name);
    }

    @Nonnull
    @Override
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        return delegatedInstance.getMultiMap(name);
    }

    @Nonnull
    @Override
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        return delegatedInstance.getRingbuffer(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        return delegatedInstance.getReliableTopic(name);
    }

    @Override
    public ICacheManager getCacheManager() {
        return delegatedInstance.getCacheManager();
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return delegatedInstance.getCluster();
    }

    @Nonnull
    @Override
    public Endpoint getLocalEndpoint() {
        return delegatedInstance.getLocalEndpoint();
    }

    @Nonnull
    @Override
    public IExecutorService getExecutorService(@Nonnull String name) {
        return delegatedInstance.getExecutorService(name);
    }

    @Nonnull
    @Override
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        return delegatedInstance.getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
        return delegatedInstance.executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        return delegatedInstance.executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return delegatedInstance.newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return delegatedInstance.newTransactionContext(options);
    }

    @Nonnull
    @Override
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        return delegatedInstance.getFlakeIdGenerator(name);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return delegatedInstance.getDistributedObjects();
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        return delegatedInstance.addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        return delegatedInstance.removeDistributedObjectListener(registrationId);
    }

    @Nonnull
    @Override
    public Config getConfig() {
        return delegatedInstance.getConfig();
    }

    @Nonnull
    @Override
    public PartitionService getPartitionService() {
        return delegatedInstance.getPartitionService();
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        return delegatedInstance.getSplitBrainProtectionService();
    }

    @Nonnull
    @Override
    public ClientService getClientService() {
        return delegatedInstance.getClientService();
    }

    @Nonnull
    @Override
    public LoggingService getLoggingService() {
        return delegatedInstance.getLoggingService();
    }

    @Nonnull
    @Override
    public LifecycleService getLifecycleService() {
        return delegatedInstance.getLifecycleService();
    }

    @Nonnull
    @Override
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        return delegatedInstance.getDistributedObject(serviceName, name);
    }

    @Nonnull
    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return delegatedInstance.getUserContext();
    }

    @Nonnull
    @Override
    public HazelcastXAResource getXAResource() {
        return delegatedInstance.getXAResource();
    }

    @Nonnull
    @Override
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        return delegatedInstance.getCardinalityEstimator(name);
    }

    @Nonnull
    @Override
    public PNCounter getPNCounter(@Nonnull String name) {
        return delegatedInstance.getPNCounter(name);
    }

    @Nonnull
    @Override
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        return delegatedInstance.getScheduledExecutorService(name);
    }

    @Nonnull
    @Override
    public CPSubsystem getCPSubsystem() {
        return delegatedInstance.getCPSubsystem();
    }

    @Nonnull
    @Override
    public SqlService getSql() {
        return delegatedInstance.getSql();
    }

    @Nonnull
    @Override
    public JetService getJet() {
        return delegatedInstance.getJet();
    }

    @Override
    public void shutdown() {
        delegatedInstance.shutdown();
    }

    @Override
    public HazelcastInstance getDelegatedInstance() {
        return delegatedInstance;
    }

    @Override
    public HazelcastOSGiService getOwnerService() {
        return ownerService;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HazelcastOSGiInstanceImpl that = (HazelcastOSGiInstanceImpl) o;

        if (!delegatedInstance.equals(that.delegatedInstance)) {
            return false;
        }
        if (!ownerService.equals(that.ownerService)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = ownerService.hashCode();
        result = 31 * result + delegatedInstance.hashCode();
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("HazelcastOSGiInstanceImpl");
        sb.append("{delegatedInstance='").append(delegatedInstance).append('\'');
        Config config = getConfig();
        if (!StringUtil.isNullOrEmpty(config.getClusterName())) {
            sb.append(", clusterName=").append(config.getClusterName());
        }
        sb.append(", ownerServiceId=").append(ownerService.getId());
        sb.append('}');
        return sb.toString();
    }

}
