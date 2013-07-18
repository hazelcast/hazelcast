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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.instance.TerminatedLifecycleService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mdogan 5/16/13
 */
public final class HazelcastClientProxy implements HazelcastInstance {

    volatile HazelcastClient client;

    HazelcastClientProxy(HazelcastClient client) {
        this.client = client;
    }

    public Config getConfig() {
        return getClient().getConfig();
    }

    public String getName() {
        return getClient().getName();
    }

    public <E> IQueue<E> getQueue(String name) {
        return getClient().getQueue(name);
    }

    public <E> ITopic<E> getTopic(String name) {
        return getClient().getTopic(name);
    }

    public <E> ISet<E> getSet(String name) {
        return getClient().getSet(name);
    }

    public <E> IList<E> getList(String name) {
        return getClient().getList(name);
    }

    public <K, V> IMap<K, V> getMap(String name) {
        return getClient().getMap(name);
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getClient().getMultiMap(name);
    }

    public ILock getLock(Object key) {
        return getClient().getLock(key);
    }

    public Cluster getCluster() {
        return getClient().getCluster();
    }

    public IExecutorService getExecutorService(String name) {
        return getClient().getExecutorService(name);
    }

    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
        return getClient().executeTransaction(task);
    }

    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        return getClient().executeTransaction(options, task);
    }

    public TransactionContext newTransactionContext() {
        return getClient().newTransactionContext();
    }

    public TransactionContext newTransactionContext(TransactionOptions options) {
        return getClient().newTransactionContext(options);
    }

    public IdGenerator getIdGenerator(String name) {
        return getClient().getIdGenerator(name);
    }

    public IAtomicLong getAtomicLong(String name) {
        return getClient().getAtomicLong(name);
    }

    public ICountDownLatch getCountDownLatch(String name) {
        return getClient().getCountDownLatch(name);
    }

    public ISemaphore getSemaphore(String name) {
        return getClient().getSemaphore(name);
    }

    public Collection<DistributedObject> getDistributedObjects() {
        return getClient().getDistributedObjects();
    }

    public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        return getClient().addDistributedObjectListener(distributedObjectListener);
    }

    public boolean removeDistributedObjectListener(String registrationId) {
        return getClient().removeDistributedObjectListener(registrationId);
    }

    public PartitionService getPartitionService() {
        return getClient().getPartitionService();
    }

    public ClientService getClientService() {
        return getClient().getClientService();
    }

    public LoggingService getLoggingService() {
        return getClient().getLoggingService();
    }

    public LifecycleService getLifecycleService() {
        final HazelcastClient hz = client;
        return hz != null ? hz.getLifecycleService() : new TerminatedLifecycleService();
    }

    public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
        return getClient().getDistributedObject(serviceName, id);
    }

    public ConcurrentMap<String, Object> getUserContext() {
        return getClient().getUserContext();
    }

    public ClientConfig getClientConfig() {
        return getClient().getClientConfig();
    }

    // to be able destroy instance bean from Spring
    public final void shutdown() {
        getLifecycleService().shutdown();
    }

    public final SerializationService getSerializationService() {
        return getClient().getSerializationService();
    }

    private HazelcastClient getClient() {
        final HazelcastClient c = client;
        if (c == null || !c.getLifecycleService().isRunning()) {
            throw new HazelcastInstanceNotActiveException();
        }
        return c;
    }
}
