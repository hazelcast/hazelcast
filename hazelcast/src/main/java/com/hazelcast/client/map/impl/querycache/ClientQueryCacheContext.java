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

package com.hazelcast.client.map.impl.querycache;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.map.impl.querycache.subscriber.ClientInvokerWrapper;
import com.hazelcast.client.map.impl.querycache.subscriber.ClientQueryCacheConfigurator;
import com.hazelcast.client.map.impl.querycache.subscriber.ClientQueryCacheEventService;
import com.hazelcast.client.map.impl.querycache.subscriber.ClientQueryCacheScheduler;
import com.hazelcast.client.map.impl.querycache.subscriber.ClientSubscriberContext;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.QueryCacheScheduler;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheFactory;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberRegistry;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ContextMutexFactory;

import java.util.Collection;
import java.util.Map;

/**
 * Client side implementation of {@link QueryCacheContext}.
 *
 * @see QueryCacheContext
 */
public class ClientQueryCacheContext implements QueryCacheContext {

    private final HazelcastClientInstanceImpl client;
    private final InvokerWrapper invokerWrapper;
    private final QueryCacheScheduler queryCacheScheduler;
    private final QueryCacheEventService queryCacheEventService;
    private final QueryCacheConfigurator queryCacheConfigurator;
    private final ContextMutexFactory mutexFactory = new ContextMutexFactory();

    // not final for testing purposes
    private SubscriberContext subscriberContext;

    public ClientQueryCacheContext(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.queryCacheEventService = new ClientQueryCacheEventService(client);
        this.queryCacheConfigurator = new ClientQueryCacheConfigurator(client.getClientConfig(), queryCacheEventService);
        this.queryCacheScheduler = new ClientQueryCacheScheduler(client.getTaskScheduler());
        this.invokerWrapper = new ClientInvokerWrapper(this, client);
        this.subscriberContext = new ClientSubscriberContext(this);
    }

    @Override
    public SubscriberContext getSubscriberContext() {
        return subscriberContext;
    }

    @Override
    public Object toObject(Object obj) {
        SerializationService serializationService = client.getSerializationService();
        return serializationService.toObject(obj);
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return client.getSerializationService();
    }

    @Override
    public Collection<Member> getMemberList() {
        return client.getClientClusterService().getMemberList();
    }

    @Override
    public int getPartitionId(Object object) {
        return client.getClientPartitionService().getPartitionId(object);
    }

    @Override
    public int getPartitionCount() {
        return client.getClientPartitionService().getPartitionCount();
    }

    @Override
    public ContextMutexFactory getLifecycleMutexFactory() {
        return mutexFactory;
    }

    @Override
    public InvokerWrapper getInvokerWrapper() {
        return invokerWrapper;
    }

    @Override
    public QueryCacheEventService getQueryCacheEventService() {
        return queryCacheEventService;
    }

    @Override
    public QueryCacheConfigurator getQueryCacheConfigurator() {
        return queryCacheConfigurator;
    }

    @Override
    public QueryCacheScheduler getQueryCacheScheduler() {
        return queryCacheScheduler;
    }

    @Override
    public void setSubscriberContext(SubscriberContext subscriberContext) {
        this.subscriberContext = subscriberContext;
    }

    @Override
    public PublisherContext getPublisherContext() {
        // no need to implement this for client part
        throw new UnsupportedOperationException();
    }

    @Override
    public Address getThisNodesAddress() {
        // no need to implement this for client part
        throw new UnsupportedOperationException();
    }

    public void recreateAllCaches() {
        //Since query cache is lost we are firing event lost event for each cache and for each partition
        QueryCacheFactory queryCacheFactory = subscriberContext.getQueryCacheFactory();
        Map<String, SubscriberRegistry> registryMap = subscriberContext.getMapSubscriberRegistry().getAll();
        for (SubscriberRegistry subscriberRegistry : registryMap.values()) {
            Map<String, Accumulator> accumulatorMap = subscriberRegistry.getAll();
            for (Accumulator accumulator : accumulatorMap.values()) {
                AccumulatorInfo info = accumulator.getInfo();
                String cacheId = info.getCacheId();
                InternalQueryCache queryCache = queryCacheFactory.getOrNull(cacheId);
                if (queryCache != null) {
                    queryCache.recreate();
                }
            }
        }
    }
}
