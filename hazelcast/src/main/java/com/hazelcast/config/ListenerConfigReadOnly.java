package com.hazelcast.config;

import java.util.EventListener;

/**
 * @ali 10/11/13
 */
public class ListenerConfigReadOnly extends ListenerConfig {

    public ListenerConfigReadOnly(ListenerConfig config) {
        this.setClassName(config.getClassName());
        this.setImplementation(config.getImplementation());
    }

    public ListenerConfig setClassName(String className) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public ListenerConfig setImplementation(EventListener implementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
