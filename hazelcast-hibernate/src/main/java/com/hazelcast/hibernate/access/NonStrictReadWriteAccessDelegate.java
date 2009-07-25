package com.hazelcast.hibernate.access;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;

import com.hazelcast.hibernate.region.HazelcastRegion;

/**
 * Makes no guarantee of consistency between the cache and the database. Stale data from the cache is possible if expiry
 * is not configured appropriately.
 * 
 * @author Leo Kim (lkim@limewire.com)
 */
public class NonStrictReadWriteAccessDelegate<T extends HazelcastRegion> extends AbstractAccessDelegate<T> {

    public NonStrictReadWriteAccessDelegate(final T hazelcastRegion) {
        super(hazelcastRegion);
    }

    public boolean afterInsert(final Object key, final Object value, final Object version) throws CacheException {
        return false;
    }

    public boolean afterUpdate(final Object key, final Object value, final Object currentVersion,
            final Object previousVersion, final SoftLock lock) throws CacheException {
        return false;
    }

    public void evict(final Object key) throws CacheException {
        getCache().remove(key);
    }

    public void evictAll() throws CacheException {
        getCache().clear();
    }

    public Object get(final Object key, final long txTimestamp) throws CacheException {
        return getCache().get(key);
    }

    public boolean insert(final Object key, final Object value, final Object version) throws CacheException {
        getCache().put(key, value);
        return true;
    }

    public SoftLock lockItem(final Object key, final Object version) throws CacheException {
        return null;
    }

    public SoftLock lockRegion() throws CacheException {
        return null;
    }

    public boolean putFromLoad(final Object key, final Object value, final long txTimestamp, final Object version)
            throws CacheException {
        getCache().put(key, value);
        return true;
    }

    public boolean putFromLoad(final Object key, final Object value, final long txTimestamp, final Object version,
            final boolean minimalPutOverride) throws CacheException {
        getCache().put(key, value);
        return true;
    }

    public void remove(final Object key) throws CacheException {
        getCache().remove(key);
    }

    public void removeAll() throws CacheException {
        getCache().clear();
    }

    public void unlockItem(final Object key, final SoftLock lock) throws CacheException {}

    public void unlockRegion(final SoftLock lock) throws CacheException {}

    public boolean update(final Object key, final Object value, final Object currentVersion,
            final Object previousVersion) throws CacheException {
        getCache().put(key, value);
        return true;
    }

}
