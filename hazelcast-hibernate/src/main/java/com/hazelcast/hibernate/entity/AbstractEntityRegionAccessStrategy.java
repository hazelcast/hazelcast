package com.hazelcast.hibernate.entity;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.EntityRegion;
import org.hibernate.cache.access.EntityRegionAccessStrategy;
import org.hibernate.cache.access.SoftLock;

import com.hazelcast.hibernate.access.AccessDelegate;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
abstract class AbstractEntityRegionAccessStrategy implements EntityRegionAccessStrategy {

    private final AccessDelegate<HazelcastEntityRegion> delegate;

    protected AbstractEntityRegionAccessStrategy(final AccessDelegate<HazelcastEntityRegion> delegate) {
        this.delegate = delegate;
    }

    public boolean afterInsert(final Object key, final Object value, final Object version) throws CacheException {
        return delegate.afterInsert(key, value, version);
    }

    public boolean afterUpdate(final Object key, final Object value, final Object currentVersion,
            final Object previousVersion, final SoftLock lock) throws CacheException {
        return delegate.afterUpdate(key, value, currentVersion, previousVersion, lock);
    }

    public void evict(final Object key) throws CacheException {
        delegate.evict(key);
    }

    public void evictAll() throws CacheException {
        delegate.evictAll();
    }

    public Object get(final Object key, final long txTimestamp) throws CacheException {
        return delegate.get(key, txTimestamp);
    }

    public EntityRegion getRegion() {
        return delegate.getHazelcastRegion();
    }

    public boolean insert(final Object key, final Object value, final Object version) throws CacheException {
        return delegate.insert(key, value, version);
    }

    public SoftLock lockItem(final Object key, final Object version) throws CacheException {
        return delegate.lockItem(key, version);
    }

    public SoftLock lockRegion() throws CacheException {
        return delegate.lockRegion();
    }

    public boolean putFromLoad(final Object key, final Object value, final long txTimestamp, final Object version)
            throws CacheException {
        return delegate.putFromLoad(key, value, txTimestamp, version);
    }

    public boolean putFromLoad(final Object key, final Object value, final long txTimestamp, final Object version,
            final boolean minimalPutOverride) throws CacheException {
        return delegate.putFromLoad(key, value, txTimestamp, version, minimalPutOverride);
    }

    public void remove(final Object key) throws CacheException {
        delegate.remove(key);
    }

    public void removeAll() throws CacheException {
        delegate.removeAll();
    }

    public void unlockItem(final Object key, final SoftLock lock) throws CacheException {
        delegate.unlockItem(key, lock);
    }

    public void unlockRegion(final SoftLock lock) throws CacheException {
        delegate.unlockRegion(lock);
    }

    public boolean update(final Object key, final Object value, final Object currentVersion,
            final Object previousVersion) throws CacheException {
        return delegate.update(key, value, currentVersion, previousVersion);
    }

}
