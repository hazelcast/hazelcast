package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.util.executor.StripedRunnable;

public class StripedEventProcessor extends EventProcessor implements StripedRunnable {

    private final int orderKey;
	
	public StripedEventProcessor(EventServiceImpl eventService, EventEnvelope envelope, int orderKey) {
		super(eventService, envelope);
		this.orderKey = orderKey;
	}

    @Override
    public int getKey() {
        return orderKey;
    }

}
