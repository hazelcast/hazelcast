package com.hazelcast.client.impl.querycache;

import com.hazelcast.client.impl.querycache.subscriber.ClientInvokerWrapper;
import com.hazelcast.client.impl.querycache.subscriber.ClientQueryCacheConfigurator;
import com.hazelcast.client.impl.querycache.subscriber.ClientQueryCacheEventService;
import com.hazelcast.client.impl.querycache.subscriber.ClientQueryCacheScheduler;
import com.hazelcast.client.impl.querycache.subscriber.ClientSubscriberContext;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.QueryCacheScheduler;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.nio.Address;

import java.util.Collection;

/**
 * Client side implementation of {@link QueryCacheContext}.
 *
 * @see QueryCacheContext
 */
public class ClientQueryCacheContext implements QueryCacheContext {

    private final ClientContext clientContext;
    private final QueryCacheEventService queryCacheEventService;
    private final QueryCacheConfigurator queryCacheConfigurator;
    private final QueryCacheScheduler queryCacheScheduler;
    private final InvokerWrapper invokerWrapper;
    // not final for testing purposes.
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
    public SerializationService getSerializationService() {
        return clientContext.getSerializationService();
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
        // no need to implement this for client part.
        throw new UnsupportedOperationException();
    }

    @Override
    public Address getThisNodesAddress() {
        // no need to implement this for client part.
        throw new UnsupportedOperationException();
    }
}

