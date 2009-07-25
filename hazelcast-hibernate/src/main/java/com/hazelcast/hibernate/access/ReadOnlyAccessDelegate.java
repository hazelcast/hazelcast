package com.hazelcast.hibernate.access;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.hibernate.region.HazelcastRegion;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public class ReadOnlyAccessDelegate<T extends HazelcastRegion> extends NonStrictReadWriteAccessDelegate<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyAccessDelegate.class);

    public ReadOnlyAccessDelegate(final T hazelcastRegion) {
        super(hazelcastRegion);
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean afterUpdate(final Object key, final Object value, final Object currentVersion,
            final Object previousVersion, final SoftLock lock) throws CacheException {
        throw new UnsupportedOperationException("Cannot update an item in a read-only cache: "
                + getHazelcastRegion().getName());
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public SoftLock lockItem(final Object key, final Object version) throws CacheException {
        throw new UnsupportedOperationException("Attempting to lock an item in a read-only cache region: "
                + getHazelcastRegion().getName());
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public SoftLock lockRegion() throws CacheException {
        throw new UnsupportedOperationException("Attempting to lock a read-only cache region: "
                + getHazelcastRegion().getName());
    }

    /**
     * This will issue a log warning stating that an attempt was made to unlock an item from a read-only cache region.
     */
    @Override
    public void unlockItem(final Object key, final SoftLock lock) throws CacheException {
        LOG.warn("Attempting to unlock an item from a read-only cache region: {}", getHazelcastRegion().getName());
    }

    /**
     * This will issue a log warning stating that an attempt was made to unlock a read-only cache region.
     */
    @Override
    public void unlockRegion(final SoftLock lock) throws CacheException {
        LOG.warn("Attempting to unlock a read-only cache region: {}", getHazelcastRegion().getName());
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean update(final Object key, final Object value, final Object currentVersion,
            final Object previousVersion) throws CacheException {
        throw new UnsupportedOperationException("Attempting to update an item in a read-only cache: "
                + getHazelcastRegion().getName());
    }

}
