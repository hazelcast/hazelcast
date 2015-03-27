package com.hazelcast.map.impl;

import com.hazelcast.core.IMapEvent;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * Responsible for dispatching the event to the public api
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 */
@PrivateApi
class InternalMapPartitionLostListenerAdapter
        implements ListenerAdapter {

    private final MapPartitionLostListener partitionLostListener;

    public InternalMapPartitionLostListenerAdapter(MapPartitionLostListener partitionLostListener) {
        this.partitionLostListener = partitionLostListener;
    }

    @Override
    public void onEvent(IMapEvent event) {
        partitionLostListener.partitionLost((MapPartitionLostEvent) event);
    }

}
