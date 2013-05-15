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
import com.hazelcast.client.connection.ConnectionManager;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hazelcast Client enables you to do all Hazelcast operations without
 * being a member of the cluster. It connects to one of the
 * cluster members and delegates all cluster wide operations to it.
 * When the connected cluster member dies, client will
 * automatically switch to another live member.
 */
public class HazelcastClient implements HazelcastInstance {

    private final static ILogger logger = Logger.getLogger(HazelcastClient.class.getName());
    private final static AtomicInteger CLIENT_ID = new AtomicInteger();
    private final static List<HazelcastClient> CLIENTS = new CopyOnWriteArrayList<HazelcastClient>();

    private final int id = CLIENT_ID.getAndIncrement();
    private final ClientConfig config;
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final Map<String, Map<Object, DistributedObject>> mapProxies = new ConcurrentHashMap<String, Map<Object, DistributedObject>>(10);
    private final SerializationServiceImpl serializationService = new SerializationServiceImpl(1, null);
    private final ConnectionManager connectionPool;

    private HazelcastClient(ClientConfig config) {
        this.config = config;
        connectionPool = new ConnectionManager(config, serializationService);

        //

        connectionPool.init(this);
        CLIENTS.add(this);
    }

    public static HazelcastClient newHazelcastClient(ClientConfig config) {
        if (config == null) {
            config = new ClientConfig();
        }
        return new HazelcastClient(config);
    }

    public Config getConfig() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public <E> IQueue<E> getQueue(String name) {
        return null;
    }

    @Override
    public <E> ITopic<E> getTopic(String name) {
        return null;
    }

    @Override
    public <E> ISet<E> getSet(String name) {
        return null;
    }

    @Override
    public <E> IList<E> getList(String name) {
        return null;
    }

    @Override
    public <K, V> IMap<K, V> getMap(String name) {
        return null;
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return null;
    }

    @Override
    public ILock getLock(Object key) {
        return null;
    }

    @Override
    public Cluster getCluster() {
        return null;
    }

    @Override
    public IExecutorService getExecutorService(String name) {
        return null;
    }

    @Override
    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
        return null;
    }

    @Override
    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        return null;
    }

    @Override
    public TransactionContext newTransactionContext() {
        return null;
    }

    @Override
    public TransactionContext newTransactionContext(TransactionOptions options) {
        return null;
    }

    @Override
    public IdGenerator getIdGenerator(String name) {
        return null;
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        return null;
    }

    @Override
    public ICountDownLatch getCountDownLatch(String name) {
        return null;
    }

    @Override
    public ISemaphore getSemaphore(String name) {
        return null;
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return null;
    }

    @Override
    public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        return null;
    }

    @Override
    public boolean removeDistributedObjectListener(String registrationId) {
        return false;
    }

    @Override
    public PartitionService getPartitionService() {
        return null;
    }

    @Override
    public ClientService getClientService() {
        return null;
    }

    @Override
    public LoggingService getLoggingService() {
        return null;
    }

    @Override
    public LifecycleService getLifecycleService() {
        return null;
    }

    @Override
    public <T extends DistributedObject> T getDistributedObject(Class<? extends RemoteService> serviceClass, Object id) {
        return null;
    }

    @Override
    public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
        return null;
    }

    @Override
    public void registerSerializer(TypeSerializer serializer, Class type) {

    }

    @Override
    public void registerGlobalSerializer(TypeSerializer serializer) {

    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return null;
    }

    public static void shutdownAll() {
        for (HazelcastClient hazelcastClient : CLIENTS) {
            try {
                hazelcastClient.getLifecycleService().shutdown();
            } catch (Exception ignored) {
            }
        }
        CLIENTS.clear();
    }

    public ClientConfig getClientConfig() {
        return config;
    }
}
