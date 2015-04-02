package com.hazelcast.config;

import com.hazelcast.map.listener.MapPartitionLostListener;

import java.util.EventListener;

/**
 * Read-Only Configuration for MapPartitionLostListener
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 */
public class MapPartitionLostListenerConfigReadOnly
        extends MapPartitionLostListenerConfig {

    public MapPartitionLostListenerConfigReadOnly(MapPartitionLostListenerConfig config) {
        super(config);
    }

    public MapPartitionLostListener getImplementation() {
        return (MapPartitionLostListener) implementation;
    }

    public ListenerConfig setClassName(String className) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    public ListenerConfig setImplementation(EventListener implementation) {
        throw new UnsupportedOperationException("this config is read-only");
    }
}
