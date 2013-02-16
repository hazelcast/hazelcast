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

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.spi.RemoteService;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * @mdogan 1/31/13
 */
public final class HazelcastInstanceProxy implements HazelcastInstance {

    volatile HazelcastInstanceImpl original;

    HazelcastInstanceProxy(HazelcastInstanceImpl original) {
        this.original = original;
    }

    public String getName() {
        return getOriginal().getName();
    }

    public <K, V> IMap<K, V> getMap(String name) {
        return getOriginal().getMap(name);
    }

    public <E> IQueue<E> getQueue(String name) {
        return getOriginal().getQueue(name);
    }

    public <E> ITopic<E> getTopic(String name) {
        return getOriginal().getTopic(name);
    }

    public <E> ISet<E> getSet(String name) {
        return getOriginal().getSet(name);
    }

    public <E> IList<E> getList(String name) {
        return getOriginal().getList(name);
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getOriginal().getMultiMap(name);
    }

    public ILock getLock(Object key) {
        return getOriginal().getLock(key);
    }

    public IExecutorService getExecutorService(String name) {
        return getOriginal().getExecutorService(name);
    }

    public Transaction getTransaction() {
        return getOriginal().getTransaction();
    }

    public IdGenerator getIdGenerator(String name) {
        return getOriginal().getIdGenerator(name);
    }

    public IAtomicLong getAtomicLong(String name) {
        return getOriginal().getAtomicLong(name);
    }

    public ICountDownLatch getCountDownLatch(String name) {
        return getOriginal().getCountDownLatch(name);
    }

    public ISemaphore getSemaphore(String name) {
        return getOriginal().getSemaphore(name);
    }

    public Cluster getCluster() {
        return getOriginal().getCluster();
    }

    public Collection<DistributedObject> getDistributedObjects() {
        return getOriginal().getDistributedObjects();
    }

    public Config getConfig() {
        return getOriginal().getConfig();
    }

    public PartitionService getPartitionService() {
        return getOriginal().getPartitionService();
    }

    public ClientService getClientService() {
        return getOriginal().getClientService();
    }

    public LoggingService getLoggingService() {
        return getOriginal().getLoggingService();
    }

    public LifecycleService getLifecycleService() {
        return getOriginal().getLifecycleService();
    }

    public <S extends DistributedObject> S getDistributedObject(Class<? extends RemoteService> serviceClass, Object id) {
        return getOriginal().getDistributedObject(serviceClass, id);
    }

    public <S extends DistributedObject> S getDistributedObject(String serviceName, Object id) {
        return getOriginal().getDistributedObject(serviceName, id);
    }

    public void registerSerializer(TypeSerializer serializer, Class type) {
        getOriginal().registerSerializer(serializer, type);
    }

    public void registerFallbackSerializer(TypeSerializer serializer) {
        getOriginal().registerFallbackSerializer(serializer);
    }

    public void addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        getOriginal().addDistributedObjectListener(distributedObjectListener);
    }

    public void removeDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        getOriginal().removeDistributedObjectListener(distributedObjectListener);
    }

    public ConcurrentMap<String, Object> getUserContext() {
        return getOriginal().getUserContext();
    }

    private HazelcastInstanceImpl getOriginal() {
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
}


