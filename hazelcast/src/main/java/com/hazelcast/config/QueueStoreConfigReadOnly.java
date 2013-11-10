package com.hazelcast.config;

import com.hazelcast.core.QueueStore;
import com.hazelcast.core.QueueStoreFactory;

import java.util.Properties;

/**
 * @ali 10/11/13
 */
public class QueueStoreConfigReadOnly extends QueueStoreConfig {

    public QueueStoreConfigReadOnly(QueueStoreConfig config) {
        this.setClassName(config.getClassName());
        this.setStoreImplementation(config.getStoreImplementation());
        this.setFactoryClassName(config.getFactoryClassName());
        this.setFactoryImplementation(config.getFactoryImplementation());
        this.setProperties(config.getProperties());
        this.setEnabled(config.isEnabled());
    }

    public QueueStoreConfig setStoreImplementation(QueueStore storeImplementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public QueueStoreConfig setEnabled(boolean enabled) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public QueueStoreConfig setClassName(String className) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public QueueStoreConfig setProperties(Properties properties) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public QueueStoreConfig setProperty(String name, String value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public QueueStoreConfig setFactoryClassName(String factoryClassName) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public QueueStoreConfig setFactoryImplementation(QueueStoreFactory factoryImplementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
