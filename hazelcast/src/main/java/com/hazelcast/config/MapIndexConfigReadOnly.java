package com.hazelcast.config;

/**
 * @ali 10/11/13
 */
public class MapIndexConfigReadOnly extends MapIndexConfig {

    public MapIndexConfigReadOnly(MapIndexConfig config) {
        super.setAttribute(config.getAttribute());
        super.setOrdered(config.isOrdered());
    }

    public MapIndexConfig setAttribute(String attribute) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapIndexConfig setOrdered(boolean ordered) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
