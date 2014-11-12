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

package com.hazelcast.jca;

import com.hazelcast.config.Config;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.HazelcastInstance;
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
import com.hazelcast.core.TransactionalList;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.cci.ConnectionMetaData;
import javax.resource.cci.Interaction;
import javax.resource.cci.ResultSetInfo;
import javax.resource.spi.ConnectionEvent;
import javax.security.auth.Subject;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Implementation class of {@link com.hazelcast.jca.HazelcastConnectionImpl}
 */
public class HazelcastConnectionImpl implements HazelcastConnection {
    /**
     * Identity generator
     */
    private static AtomicInteger idGen = new AtomicInteger();
    /**
     * Reference to this creator and access to container infrastructure
     */
    final ManagedConnectionImpl managedConnection;
    /**
     * this identity
     */
    private final int id;

    public HazelcastConnectionImpl(ManagedConnectionImpl managedConnectionImpl, Subject subject) {
        super();
        this.managedConnection = managedConnectionImpl;
        id = idGen.incrementAndGet();
    }

    @Override
    public void close() throws ResourceException {
        managedConnection.log(Level.FINEST, "close");
        //important: inform the container!
        managedConnection.fireConnectionEvent(ConnectionEvent.CONNECTION_CLOSED, this);
    }

    @Override
    public Interaction createInteraction() throws ResourceException {
        //TODO
        return null;
    }

    /**
     * @throws NotSupportedException as this is not supported by this resource adapter
     */
    @Override
    public ResultSetInfo getResultSetInfo() throws NotSupportedException {
        //as per spec 15.11.3
        throw new NotSupportedException();
    }

    @Override
    public HazelcastTransaction getLocalTransaction() throws ResourceException {
        managedConnection.log(Level.FINEST, "getLocalTransaction");
        return managedConnection.getLocalTransaction();
    }

    @Override
    public ConnectionMetaData getMetaData() throws ResourceException {
        return managedConnection.getMetaData();
    }

    @Override
    public String toString() {
        return "hazelcast.ConnectionImpl [" + id + "]";
    }

    /**
     * Method is not exposed to force all clients to go through this connection object and its
     * methods from {@link HazelcastConnection}
     *
     * @return the local hazelcast instance
     */
    private HazelcastInstance getHazelcastInstance() {
        return managedConnection.getHazelcastInstance();
    }

    @Override
    public <K, V> IMap<K, V> getMap(String name) {
        return getHazelcastInstance().getMap(name);
    }

    @Override
    public <E> IQueue<E> getQueue(String name) {
        return getHazelcastInstance().getQueue(name);
    }

    @Override
    public <E> ITopic<E> getTopic(String name) {
        return getHazelcastInstance().getTopic(name);
    }

    @Override
    public <E> ISet<E> getSet(String name) {
        return getHazelcastInstance().getSet(name);
    }

    @Override
    public <E> IList<E> getList(String name) {
        return getHazelcastInstance().getList(name);
    }


    @Override
    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getHazelcastInstance().getMultiMap(name);
    }

    @Override
    public IExecutorService getExecutorService(String name) {
        return getHazelcastInstance().getExecutorService(name);
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        return getHazelcastInstance().getAtomicLong(name);
    }

    @Override
    public ICountDownLatch getCountDownLatch(String name) {
        return getHazelcastInstance().getCountDownLatch(name);
    }

    @Override
    public ISemaphore getSemaphore(String name) {
        return getHazelcastInstance().getSemaphore(name);
    }

    @Override
    public <K, V> TransactionalMap<K, V> getTransactionalMap(String name) {
        final TransactionContext txContext = this.managedConnection.getTx().getTxContext();
        if (txContext == null) {
            throw new IllegalStateException("Transaction is not active");
        }
        return txContext.getMap(name);
    }

    @Override
    public <E> TransactionalQueue<E> getTransactionalQueue(String name) {
        final TransactionContext txContext = this.managedConnection.getTx().getTxContext();
        if (txContext == null) {
            throw new IllegalStateException("Transaction is not active");
        }
        return txContext.getQueue(name);
    }

    @Override
    public <K, V> TransactionalMultiMap<K, V> getTransactionalMultiMap(String name) {
        final TransactionContext txContext = this.managedConnection.getTx().getTxContext();
        if (txContext == null) {
            throw new IllegalStateException("Transaction is not active");
        }
        return txContext.getMultiMap(name);
    }

    @Override
    public <E> TransactionalList<E> getTransactionalList(String name) {
        final TransactionContext txContext = this.managedConnection.getTx().getTxContext();
        if (txContext == null) {
            throw new IllegalStateException("Transaction is not active");
        }
        return txContext.getList(name);
    }

    @Override
    public <E> TransactionalSet<E> getTransactionalSet(String name) {
        final TransactionContext txContext = this.managedConnection.getTx().getTxContext();
        if (txContext == null) {
            throw new IllegalStateException("Transaction is not active");
        }
        return txContext.getSet(name);
    }

    @Override
    public IdGenerator getIdGenerator(String name) {
        return getHazelcastInstance().getIdGenerator(name);
    }

    @Override
    public <E> IAtomicReference<E> getAtomicReference(String name) {
        return getHazelcastInstance().getAtomicReference(name);
    }

    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(String name) {
        return getHazelcastInstance().getReplicatedMap(name);
    }

    @Override
    public JobTracker getJobTracker(String name) {
        return getHazelcastInstance().getJobTracker(name);
    }

    @Override
    public String getName() {
        return getHazelcastInstance().getName();
    }


    @Override
    public ILock getLock(String key) {
        return getHazelcastInstance().getLock(key);
    }

    @Override
    public ILock getLock(Object key) {
        return getHazelcastInstance().getLock(key);
    }


    // unsupported operations
    @Override
    public LoggingService getLoggingService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LifecycleService getLifecycleService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeDistributedObjectListener(String registrationId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Config getConfig() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionService getPartitionService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClientService getClientService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cluster getCluster() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Endpoint getLocalEndpoint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionContext newTransactionContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionContext newTransactionContext(TransactionOptions options) {
        throw new UnsupportedOperationException();
    }

}
