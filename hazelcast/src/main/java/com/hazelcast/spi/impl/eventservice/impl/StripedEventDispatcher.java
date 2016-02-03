package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.util.executor.StripedRunnable;

public class StripedEventDispatcher extends LocalEventDispatcher implements StripedRunnable {

    private final int orderKey;

    StripedEventDispatcher(EventServiceImpl eventService, String serviceName, Object event, Object listener,
                         long timeoutMs, int orderKey) {
    	super(eventService, serviceName, event, listener, timeoutMs);
        this.orderKey = orderKey;
    }
	
    @Override
    public int getKey() {
        return orderKey;
    }
	
}
