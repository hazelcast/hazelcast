package com.hazelcast.hibernate.region;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.GeneralDataRegion;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public abstract class AbstractGeneralRegion extends AbstractHazelcastRegion implements GeneralDataRegion {

    protected AbstractGeneralRegion(final String name) {
        super(name);
    }

    public void evict(final Object key) throws CacheException {
        getCache().remove(key);
    }

    public void evictAll() throws CacheException {
        getCache().clear();
    }

    public Object get(final Object key) throws CacheException {
        return getCache().get(key);
    }

    public void put(final Object key, final Object value) throws CacheException {
        getCache().put(key, value);
    }
}
