package com.hazelcast.config;

import com.hazelcast.core.EntryListener;

import java.util.EventListener;

/**
 * @ali 10/11/13
 */
public class EntryListenerConfigReadOnly extends EntryListenerConfig {

    public EntryListenerConfigReadOnly(EntryListenerConfig config) {
        super.setImplementation(config.getImplementation());
        super.setIncludeValue(config.isIncludeValue());
        super.setLocal(config.isLocal());
        if (config.getClassName() != null ) {
            super.setClassName(config.getClassName());
        }
    }

    public EntryListenerConfig setImplementation(EntryListener implementation) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    public EntryListenerConfig setLocal(boolean local) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    public EntryListenerConfig setIncludeValue(boolean includeValue) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    public ListenerConfig setClassName(String className) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    public ListenerConfig setImplementation(EventListener implementation) {
        throw new UnsupportedOperationException("this config is read-only");
    }
}
