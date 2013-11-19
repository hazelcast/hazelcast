package com.hazelcast.config;

import com.hazelcast.core.ItemListener;

import java.util.EventListener;

/**
 * @ali 10/11/13
 */
public class ItemListenerConfigReadOnly extends ItemListenerConfig {

    public ItemListenerConfigReadOnly(ItemListenerConfig config) {
        super.setImplementation(config.getImplementation());
        super.setIncludeValue(config.isIncludeValue());
        if (config.getClassName() != null) {
            super.setClassName(config.getClassName());
        }
    }

    public ItemListenerConfig setImplementation(ItemListener implementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public ItemListenerConfig setIncludeValue(boolean includeValue) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public ListenerConfig setClassName(String className) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public ListenerConfig setImplementation(EventListener implementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
