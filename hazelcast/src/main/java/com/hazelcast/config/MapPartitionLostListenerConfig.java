package com.hazelcast.config;

import com.hazelcast.map.listener.MapPartitionLostListener;

/**
 * Configuration for MapPartitionLostListener
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 */
public class MapPartitionLostListenerConfig
        extends ListenerConfig {

    private MapPartitionLostListenerConfigReadOnly readOnly;

    public MapPartitionLostListenerConfig() {
        super();
    }

    public MapPartitionLostListenerConfig(String className) {
        super(className);
    }

    public MapPartitionLostListenerConfig(MapPartitionLostListener implementation) {
        super(implementation);
    }

    public MapPartitionLostListenerConfig(MapPartitionLostListenerConfig config) {
        implementation = config.getImplementation();
        className = config.getClassName();
    }

    public MapPartitionLostListenerConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapPartitionLostListenerConfigReadOnly(this);
        }
        return readOnly;
    }

    public MapPartitionLostListener getImplementation() {
        return (MapPartitionLostListener) implementation;
    }

    public MapPartitionLostListenerConfig setImplementation(final MapPartitionLostListener implementation) {
        super.setImplementation(implementation);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MapPartitionLostListenerConfig that = (MapPartitionLostListenerConfig) o;

        return !(readOnly != null ? !readOnly.equals(that.readOnly) : that.readOnly != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (readOnly != null ? readOnly.hashCode() : 0);
        return result;
    }
}
