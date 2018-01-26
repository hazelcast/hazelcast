/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.querycache;

import com.hazelcast.client.impl.querycache.subscriber.ClientInvokerWrapper;
import com.hazelcast.client.impl.querycache.subscriber.ClientQueryCacheConfigurator;
import com.hazelcast.client.impl.querycache.subscriber.ClientQueryCacheEventService;
import com.hazelcast.client.impl.querycache.subscriber.ClientQueryCacheScheduler;
import com.hazelcast.client.impl.querycache.subscriber.ClientSubscriberContext;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.QueryCacheScheduler;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ContextMutexFactory;

import java.util.Collection;

/**
 * Client side implementation of {@link QueryCacheContext}.
 *
 * @see QueryCacheContext
 */
public class ClientQueryCacheContext implements QueryCacheContext {

    private final ClientContext clientContext;
    private final InvokerWrapper invokerWrapper;
    private final QueryCacheScheduler queryCacheScheduler;
    private final QueryCacheEventService queryCacheEventService;
    private final QueryCacheConfigurator queryCacheConfigurator;
    private final ContextMutexFactory mutexFactory = new ContextMutexFactory();

    // not final for testing purposes
    private SubscriberContext subscriberContext;

    public ClientQueryCacheContext(ClientContext clientContext) {
        this.clientContext = clientContext;
        this.queryCacheEventService = new ClientQueryCacheEventService(clientContext);
        this.queryCacheConfigurator = new ClientQueryCacheConfigurator(clientContext.getClientConfig(), queryCacheEventService);
        this.queryCacheScheduler = new ClientQueryCacheScheduler(clientContext.getExecutionService());
        this.invokerWrapper = new ClientInvokerWrapper(this, clientContext);
        this.subscriberContext = new ClientSubscriberContext(this);
    }

    @Override
    public SubscriberContext getSubscriberContext() {
        return subscriberContext;
    }

    @Override
    public Object toObject(Object obj) {
        SerializationService serializationService = clientContext.getSerializationService();
        return serializationService.toObject(obj);
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return (InternalSerializationService) clientContext.getSerializationService();
    }

    @Override
    public Collection<Member> getMemberList() {
        return clientContext.getClusterService().getMemberList();
    }

    @Override
    public int getPartitionId(Object object) {
        return clientContext.getPartitionService().getPartitionId(object);
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
}
