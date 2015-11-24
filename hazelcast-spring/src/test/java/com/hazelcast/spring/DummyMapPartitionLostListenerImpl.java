package com.hazelcast.spring;

import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.listener.MapPartitionLostListener;

public class DummyMapPartitionLostListenerImpl implements MapPartitionLostListener {
    @Override
    public void partitionLost(MapPartitionLostEvent event) {

    }
}
