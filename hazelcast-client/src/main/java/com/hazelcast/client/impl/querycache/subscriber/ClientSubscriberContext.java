package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.subscriber.AbstractSubscriberContext;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContextSupport;

/**
 * {@code SubscriberContext} implementation for client side.
 *
 * @see com.hazelcast.map.impl.querycache.subscriber.SubscriberContext
 */
public class ClientSubscriberContext extends AbstractSubscriberContext {

    private final ClientSubscriberContextSupport clientSubscriberContextSupport;

    public ClientSubscriberContext(QueryCacheContext context) {
        super(context);
        clientSubscriberContextSupport = new ClientSubscriberContextSupport();
    }

    @Override
    public SubscriberContextSupport getSubscriberContextSupport() {
        return clientSubscriberContextSupport;
    }
}
