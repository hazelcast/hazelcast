package com.hazelcast.config;

/**
 * @ali 10/11/13
 */
public class NearCacheConfigReadOnly extends NearCacheConfig {

    public NearCacheConfigReadOnly(NearCacheConfig config) {
        this.setName(config.getName());
        this.setEvictionPolicy(config.getEvictionPolicy());
        this.setInMemoryFormat(config.getInMemoryFormat());
        this.setInvalidateOnChange(config.isInvalidateOnChange());
        this.setMaxIdleSeconds(config.getMaxIdleSeconds());
        this.setMaxSize(config.getMaxSize());
        this.setTimeToLiveSeconds(config.getTimeToLiveSeconds());
    }

    public void setName(String name) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public NearCacheConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public NearCacheConfig setMaxSize(int maxSize) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public NearCacheConfig setEvictionPolicy(String evictionPolicy) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public NearCacheConfig setMaxIdleSeconds(int maxIdleSeconds) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public NearCacheConfig setInvalidateOnChange(boolean invalidateOnChange) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public NearCacheConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public NearCacheConfig setInMemoryFormat(String inMemoryFormat) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}