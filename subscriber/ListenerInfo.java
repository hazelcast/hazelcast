package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.spi.EventFilter;

/**
 * Contains all listener specific information for {@link ClientQueryCacheEventService}
 *
 * @see ClientQueryCacheEventService
 */
public class ListenerInfo {

    private final String id;
    private final EventFilter filter;
    private final ListenerAdapter listenerAdapter;

    public ListenerInfo(EventFilter filter, ListenerAdapter listenerAdapter, String id) {
        this.id = id;
        this.filter = filter;
        this.listenerAdapter = listenerAdapter;
    }

    public EventFilter getFilter() {
        return filter;
    }

    public ListenerAdapter getListenerAdapter() {
        return listenerAdapter;
    }

    public String getId() {
        return id;
    }
}
