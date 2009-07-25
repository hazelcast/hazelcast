package com.hazelcast.hibernate.access;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;

import com.hazelcast.core.IMap;
import com.hazelcast.hibernate.region.HazelcastRegion;

/**
 * Makes <b>READ COMMITTED</b> consistency guarantees even in a clustered environment.
 * 
 * @author Leo Kim (lkim@limewire.com)
 */
public class ReadWriteAccessDelegate<T extends HazelcastRegion> extends AbstractAccessDelegate<T> {

    public ReadWriteAccessDelegate(final T hazelcastRegion) {
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
        final IMap cache = getCache();
        cache.lock(key);
        try {
            cache.remove(key);
        } finally {
            cache.unlock(key);
        }
    }

    public void evictAll() throws CacheException {
        getCache().clear();
    }

    public Object get(final Object key, final long txTimestamp) throws CacheException {
        return getCache().get(key);
    }

    public boolean insert(final Object key, final Object value, final Object version) throws CacheException {
        final IMap cache = getCache();
        cache.lock(key);
        try {
            cache.put(key, value);
        } finally {
            cache.unlock(key);
        }
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
        final IMap cache = getCache();
        cache.lock(key);
        try {
            cache.put(key, value);
        } finally {
            cache.unlock(key);
        }
        return true;
    }

    public boolean putFromLoad(final Object key, final Object value, final long txTimestamp, final Object version,
            final boolean minimalPutOverride) throws CacheException {
        final IMap cache = getCache();
        cache.lock(key);
        try {
            cache.put(key, value);
        } finally {
            cache.unlock(key);
        }
        return true;
    }

    public void remove(final Object key) throws CacheException {
        final IMap cache = getCache();
        cache.lock(key);
        try {
            cache.remove(key);
        } finally {
            cache.unlock(key);
        }
    }

    public void removeAll() throws CacheException {
        getCache().clear();
    }

    public void unlockItem(final Object key, final SoftLock lock) throws CacheException {}

    public void unlockRegion(final SoftLock lock) throws CacheException {}

    public boolean update(final Object key, final Object value, final Object currentVersion,
            final Object previousVersion) throws CacheException {
        final IMap cache = getCache();
        cache.lock(key);
        try {
            cache.put(key, value);
        } finally {
            cache.unlock(key);
        }
        return true;
    }

}
