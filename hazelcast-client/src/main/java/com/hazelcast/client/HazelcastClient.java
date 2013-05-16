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
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.proxy.ClusterProxy;
import com.hazelcast.client.proxy.PartitionServiceProxy;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientPartitionServiceImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    private final static ConcurrentMap<Integer, HazelcastClientProxy> CLIENTS = new ConcurrentHashMap<Integer, HazelcastClientProxy>(5);

    private final int id = CLIENT_ID.getAndIncrement();
    private final ClientConfig config;
    private final LifecycleServiceImpl lifecycleService;
    private final SerializationServiceImpl serializationService = new SerializationServiceImpl(0, null);
    private final ClientClusterServiceImpl clusterService;
    private final ClientPartitionServiceImpl partitionService;
    private final ClientConnectionManager connectionManager;
    private final ClientExecutionServiceImpl executionService = new ClientExecutionServiceImpl();
    private final ConcurrentMap<String, Object> userContext;
    private final Map<String, Map<Object, DistributedObject>> mapProxies = new ConcurrentHashMap<String, Map<Object, DistributedObject>>(10);

    private HazelcastClient(ClientConfig config) {
        this.config = config;
        lifecycleService = new LifecycleServiceImpl(this);

        clusterService = new ClientClusterServiceImpl(this);
        partitionService = new ClientPartitionServiceImpl(this);
        connectionManager = new ClientConnectionManager(config, clusterService.getAuthenticator(), serializationService);
        userContext = new ConcurrentHashMap<String, Object>();
        clusterService.start();
        partitionService.start();
        lifecycleService.started();
    }

    public static HazelcastInstance newHazelcastClient(ClientConfig config) {
        if (config == null) {
            config = new ClientConfig();
        }
        final HazelcastClient client = new HazelcastClient(config);
        final HazelcastClientProxy proxy = new HazelcastClientProxy(client);
        CLIENTS.put(client.id, proxy);
        return proxy;
    }

    public Config getConfig() {
        throw new UnsupportedOperationException("Client cannot access cluster config!");
    }

    @Override
    public String getName() {
        return "HazelcastClient[" + id + "]";
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
        return new ClusterProxy(clusterService);
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
        return new PartitionServiceProxy(partitionService);
    }

    @Override
    public ClientService getClientService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LoggingService getLoggingService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LifecycleService getLifecycleService() {
        return lifecycleService;
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
        return userContext;
    }

    public ClientConfig getClientConfig() {
        return config;
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    public ClientConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public ClientClusterService getClientClusterService() {
        return clusterService;
    }

    public ClientExecutionService getClientExecutionService() {
        return executionService;
    }

    public ClientPartitionService getClientPartitionService() {
        return partitionService;
    }

    void shutdown() {
        CLIENTS.remove(id);
        executionService.shutdown();
        partitionService.stop();
        clusterService.stop();
        connectionManager.shutdown();
    }

    public static void shutdownAll() {
        for (HazelcastClientProxy proxy : CLIENTS.values()) {
            try {
                proxy.client.getLifecycleService().shutdown();
            } catch (Exception ignored) {
            }
            proxy.client = null;
        }
        CLIENTS.clear();
    }
}
