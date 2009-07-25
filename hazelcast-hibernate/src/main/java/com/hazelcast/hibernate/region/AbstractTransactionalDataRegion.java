package com.hazelcast.hibernate.region;

import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.TransactionalDataRegion;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public abstract class AbstractTransactionalDataRegion extends AbstractHazelcastRegion implements
        TransactionalDataRegion {

    private final CacheDataDescription metadata;

    protected AbstractTransactionalDataRegion(final String regionName, final CacheDataDescription metadata) {
        super(regionName);
        this.metadata = metadata;
    }

    /**
     * @see org.hibernate.cache.TransactionalDataRegion#getCacheDataDescription()
     */
    public CacheDataDescription getCacheDataDescription() {
        return metadata;
    }

    /**
     * @see org.hibernate.cache.TransactionalDataRegion#isTransactionAware()
     */
    public boolean isTransactionAware() {
        return false;
    }
}
