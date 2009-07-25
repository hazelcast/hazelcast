package com.hazelcast.hibernate.collection;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.CollectionRegion;
import org.hibernate.cache.access.CollectionRegionAccessStrategy;
import org.hibernate.cache.access.SoftLock;

import com.hazelcast.hibernate.access.AccessDelegate;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
abstract class AbstractCollectionRegionAccessStrategy implements CollectionRegionAccessStrategy {

    private final AccessDelegate<HazelcastCollectionRegion> delegate;

    protected AbstractCollectionRegionAccessStrategy(final AccessDelegate<HazelcastCollectionRegion> delegate) {
        this.delegate = delegate;
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

    public CollectionRegion getRegion() {
        return delegate.getHazelcastRegion();
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
}
