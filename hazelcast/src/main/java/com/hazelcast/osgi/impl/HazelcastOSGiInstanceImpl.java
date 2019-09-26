/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.osgi.HazelcastOSGiInstance;
import com.hazelcast.osgi.HazelcastOSGiService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

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

    @Override
    public String getName() {
        return delegatedInstance.getName();
    }

    @Override
    public <E> IQueue<E> getQueue(String name) {
        return delegatedInstance.getQueue(name);
    }

    @Override
    public <E> ITopic<E> getTopic(String name) {
        return delegatedInstance.getTopic(name);
    }

    @Override
    public <E> ISet<E> getSet(String name) {
        return delegatedInstance.getSet(name);
    }

    @Override
    public <E> IList<E> getList(String name) {
        return delegatedInstance.getList(name);
    }

    @Override
    public <K, V> IMap<K, V> getMap(String name) {
        return delegatedInstance.getMap(name);
    }

    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(String name) {
        return delegatedInstance.getReplicatedMap(name);
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return delegatedInstance.getMultiMap(name);
    }

    @Override
    public ILock getLock(String key) {
        return delegatedInstance.getLock(key);
    }

    @Override
    public <E> Ringbuffer<E> getRingbuffer(String name) {
        return delegatedInstance.getRingbuffer(name);
    }

    @Override
    public <E> ITopic<E> getReliableTopic(String name) {
        return delegatedInstance.getReliableTopic(name);
    }

    @Override
    public ICacheManager getCacheManager() {
        return delegatedInstance.getCacheManager();
    }

    @Override
    public Cluster getCluster() {
        return delegatedInstance.getCluster();
    }

    @Override
    public Endpoint getLocalEndpoint() {
        return delegatedInstance.getLocalEndpoint();
    }

    @Override
    public IExecutorService getExecutorService(String name) {
        return delegatedInstance.getExecutorService(name);
    }

    @Override
    public DurableExecutorService getDurableExecutorService(String name) {
        return delegatedInstance.getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
        return delegatedInstance.executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        return delegatedInstance.executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return delegatedInstance.newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(TransactionOptions options) {
        return delegatedInstance.newTransactionContext(options);
    }

    @Override
    public IdGenerator getIdGenerator(String name) {
        return delegatedInstance.getIdGenerator(name);
    }

    @Override
    public FlakeIdGenerator getFlakeIdGenerator(String name) {
        return delegatedInstance.getFlakeIdGenerator(name);
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        return delegatedInstance.getAtomicLong(name);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return delegatedInstance.getDistributedObjects();
    }

    @Override
    public UUID addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        return delegatedInstance.addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(UUID registrationId) {
        return delegatedInstance.removeDistributedObjectListener(registrationId);
    }

    @Override
    public Config getConfig() {
        return delegatedInstance.getConfig();
    }

    @Override
    public PartitionService getPartitionService() {
        return delegatedInstance.getPartitionService();
    }

    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        return delegatedInstance.getSplitBrainProtectionService();
    }

    @Override
    public ClientService getClientService() {
        return delegatedInstance.getClientService();
    }

    @Override
    public LoggingService getLoggingService() {
        return delegatedInstance.getLoggingService();
    }

    @Override
    public LifecycleService getLifecycleService() {
        return delegatedInstance.getLifecycleService();
    }

    @Override
    public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
        return delegatedInstance.getDistributedObject(serviceName, name);
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return delegatedInstance.getUserContext();
    }

    @Override
    public HazelcastXAResource getXAResource() {
        return delegatedInstance.getXAResource();
    }

    @Override
    public CardinalityEstimator getCardinalityEstimator(String name) {
        return delegatedInstance.getCardinalityEstimator(name);
    }

    @Override
    public PNCounter getPNCounter(String name) {
        return delegatedInstance.getPNCounter(name);
    }

    @Override
    public IScheduledExecutorService getScheduledExecutorService(String name) {
        return delegatedInstance.getScheduledExecutorService(name);
    }

    @Override
    public CPSubsystem getCPSubsystem() {
        return delegatedInstance.getCPSubsystem();
    }

    @Override
    public SqlService getSqlService() {
        return delegatedInstance.getSqlService();
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
