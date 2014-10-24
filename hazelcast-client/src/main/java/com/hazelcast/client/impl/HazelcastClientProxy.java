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

package com.hazelcast.client.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
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
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.instance.TerminatedLifecycleService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * A client-side proxy {@link com.hazelcast.core.HazelcastInstance} instance.
 *
 */
public final class HazelcastClientProxy implements HazelcastInstance {

    public volatile HazelcastClientInstanceImpl client;

    public HazelcastClientProxy(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public Config getConfig() {
        return getClient().getConfig();
    }

    @Override
    public String getName() {
        return getClient().getName();
    }

    @Override
    public <E> IQueue<E> getQueue(String name) {
        return getClient().getQueue(name);
    }

    @Override
    public <E> ITopic<E> getTopic(String name) {
        return getClient().getTopic(name);
    }

    @Override
    public <E> ISet<E> getSet(String name) {
        return getClient().getSet(name);
    }

    @Override
    public <E> IList<E> getList(String name) {
        return getClient().getList(name);
    }

    @Override
    public <K, V> IMap<K, V> getMap(String name) {
        return getClient().getMap(name);
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getClient().getMultiMap(name);
    }

    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(String name) {
        return getClient().getReplicatedMap(name);
    }

    @Override
    public JobTracker getJobTracker(String name) {
        return getClient().getJobTracker(name);
    }

    @Override
    public ILock getLock(Object key) {
        return getClient().getLock(key);
    }

    @Override
    public ILock getLock(String key) {
        return getClient().getLock(key);
    }

    @Override
    public Cluster getCluster() {
        return getClient().getCluster();
    }

    @Override
    public Client getLocalEndpoint() {
        return getClient().getLocalEndpoint();
    }

    @Override
    public IExecutorService getExecutorService(String name) {
        return getClient().getExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(TransactionalTask<T> task)
            throws TransactionException {
        return getClient().executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task)
            throws TransactionException {
        return getClient().executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return getClient().newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(TransactionOptions options) {
        return getClient().newTransactionContext(options);
    }

    @Override
    public IdGenerator getIdGenerator(String name) {
        return getClient().getIdGenerator(name);
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        return getClient().getAtomicLong(name);
    }

    @Override
    public <E> IAtomicReference<E> getAtomicReference(String name) {
        return getClient().getAtomicReference(name);
    }

    @Override
    public ICountDownLatch getCountDownLatch(String name) {
        return getClient().getCountDownLatch(name);
    }

    @Override
    public ISemaphore getSemaphore(String name) {
        return getClient().getSemaphore(name);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return getClient().getDistributedObjects();
    }

    @Override
    public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        return getClient().addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(String registrationId) {
        return getClient().removeDistributedObjectListener(registrationId);
    }

    @Override
    public PartitionService getPartitionService() {
        return getClient().getPartitionService();
    }

    @Override
    public ClientService getClientService() {
        return getClient().getClientService();
    }

    @Override
    public LoggingService getLoggingService() {
        return getClient().getLoggingService();
    }

    @Override
    public LifecycleService getLifecycleService() {
        final HazelcastClientInstanceImpl hz = client;
        return hz != null ? hz.getLifecycleService() : new TerminatedLifecycleService();
    }

    @Deprecated
    public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
        return getClient().getDistributedObject(serviceName, id);
    }

    @Override
    public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
        return getClient().getDistributedObject(serviceName, name);
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return getClient().getUserContext();
    }

    public ClientConfig getClientConfig() {
        return getClient().getClientConfig();
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    public SerializationService getSerializationService() {
        return getClient().getSerializationService();
    }

    private HazelcastClientInstanceImpl getClient() {
        final HazelcastClientInstanceImpl c = client;
        if (c == null || !c.getLifecycleService().isRunning()) {
            throw new HazelcastInstanceNotActiveException();
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
}
