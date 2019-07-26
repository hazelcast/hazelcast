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
import com.hazelcast.map.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.topic.ITopic;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * A proxy around the actual HazelcastInstanceImpl. This class serves 2 purposes:
 * <ol>
 * <li>
 * if the HazelcastInstance is shutdown, the reference to the original HazelcastInstanceImpl is null'ed and
 * this HazelcastInstanceImpl and all its dependencies can be gc'ed. If the HazelcastInstanceImpl would
 * be exposed directly, it could still retain unusable objects due to its not null fields.</li>
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

    @Override
    public String getName() {
        return name;
    }

    @Override
    public <K, V> IMap<K, V> getMap(String name) {
        return getOriginal().getMap(name);
    }

    @Override
    public <E> IQueue<E> getQueue(String name) {
        return getOriginal().getQueue(name);
    }

    @Override
    public <E> ITopic<E> getTopic(String name) {
        return getOriginal().getTopic(name);
    }

    @Override
    public <E> ITopic<E> getReliableTopic(String name) {
        return getOriginal().getReliableTopic(name);
    }

    @Override
    public <E> ISet<E> getSet(String name) {
        return getOriginal().getSet(name);
    }

    @Override
    public <E> IList<E> getList(String name) {
        return getOriginal().getList(name);
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getOriginal().getMultiMap(name);
    }

    @Override
    public <E> Ringbuffer<E> getRingbuffer(String name) {
        return getOriginal().getRingbuffer(name);
    }

    @Override
    public ILock getLock(String key) {
        return getOriginal().getLock(key);
    }

    @Override
    public IExecutorService getExecutorService(String name) {
        return getOriginal().getExecutorService(name);
    }

    @Override
    public DurableExecutorService getDurableExecutorService(String name) {
        return getOriginal().getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
        return getOriginal().executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        return getOriginal().executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return getOriginal().newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(TransactionOptions options) {
        return getOriginal().newTransactionContext(options);
    }

    @Override
    public IdGenerator getIdGenerator(String name) {
        return getOriginal().getIdGenerator(name);
    }

    @Override
    public FlakeIdGenerator getFlakeIdGenerator(String name) {
        return getOriginal().getFlakeIdGenerator(name);
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        return getOriginal().getAtomicLong(name);
    }

    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(String name) {
        return getOriginal().getReplicatedMap(name);
    }

    @Override
    public <E> IAtomicReference<E> getAtomicReference(String name) {
        return getOriginal().getAtomicReference(name);
    }

    @Override
    public ICountDownLatch getCountDownLatch(String name) {
        return getOriginal().getCountDownLatch(name);
    }

    @Override
    public ISemaphore getSemaphore(String name) {
        return getOriginal().getSemaphore(name);
    }

    @Override
    public ICacheManager getCacheManager() {
        return getOriginal().getCacheManager();
    }

    @Override
    public Cluster getCluster() {
        return getOriginal().getCluster();
    }

    @Override
    public Member getLocalEndpoint() {
        return getOriginal().getLocalEndpoint();
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return getOriginal().getDistributedObjects();
    }

    @Override
    public Config getConfig() {
        return getOriginal().getConfig();
    }

    @Override
    public PartitionService getPartitionService() {
        return getOriginal().getPartitionService();
    }

    @Override
    public QuorumService getQuorumService() {
        return getOriginal().getQuorumService();
    }

    @Override
    public ClientService getClientService() {
        return getOriginal().getClientService();
    }

    @Override
    public LoggingService getLoggingService() {
        return getOriginal().getLoggingService();
    }

    @Override
    public LifecycleService getLifecycleService() {
        final HazelcastInstanceImpl hz = original;
        return hz != null ? hz.getLifecycleService() : new TerminatedLifecycleService();
    }

    @Override
    public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
        return getOriginal().getDistributedObject(serviceName, name);
    }

    @Override
    public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        return getOriginal().addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(String registrationId) {
        return getOriginal().removeDistributedObjectListener(registrationId);
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return getOriginal().getUserContext();
    }

    @Override
    public HazelcastXAResource getXAResource() {
        return getOriginal().getXAResource();
    }

    @Override
    public CardinalityEstimator getCardinalityEstimator(String name) {
        return getOriginal().getCardinalityEstimator(name);
    }

    @Override
    public PNCounter getPNCounter(String name) {
        return getOriginal().getPNCounter(name);
    }

    @Override
    public IScheduledExecutorService getScheduledExecutorService(String name) {
        return getOriginal().getScheduledExecutorService(name);
    }

    @Override
    public CPSubsystem getCPSubsystem() {
        return getOriginal().getCPSubsystem();
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


