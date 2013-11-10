package com.hazelcast.config;

import java.util.Properties;

/**
 * @ali 10/11/13
 */
public class MapStoreConfigReadOnly extends MapStoreConfig {

    public MapStoreConfigReadOnly(MapStoreConfig config) {
        this.setClassName(config.getClassName());
        this.setImplementation(config.getImplementation());
        this.setProperties(config.getProperties());
        this.setEnabled(config.isEnabled());
        this.setFactoryClassName(config.getFactoryClassName());
        this.setFactoryImplementation(config.getFactoryImplementation());
        this.setWriteDelaySeconds(config.getWriteDelaySeconds());
    }

    public MapStoreConfig setClassName(String className) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setFactoryClassName(String factoryClassName) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setWriteDelaySeconds(int writeDelaySeconds) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setEnabled(boolean enabled) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setImplementation(Object implementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setFactoryImplementation(Object factoryImplementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setProperty(String name, String value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public String getProperty(String name) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setProperties(Properties properties) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}