package com.hazelcast.hibernate.collection;

import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.CollectionRegion;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cache.access.CollectionRegionAccessStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.hibernate.region.AbstractTransactionalDataRegion;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public class HazelcastCollectionRegion extends AbstractTransactionalDataRegion implements CollectionRegion {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastCollectionRegion.class.getName());

    public HazelcastCollectionRegion(final String regionName, final CacheDataDescription metadata) {
        super(regionName, metadata);
    }

    public CollectionRegionAccessStrategy buildAccessStrategy(final AccessType accessType) throws CacheException {
        if (null == accessType) {
            throw new CacheException(
                    "Got null AccessType while attempting to build CollectionRegionAccessStrategy. This can't happen!");
        }
        LOG.info("Attempting to add {} CollectionRegion: {}", accessType, getName());
        if (AccessType.READ_ONLY.equals(accessType)) {
            return new ReadOnlyAccessStrategy(this);
        }
        if (AccessType.NONSTRICT_READ_WRITE.equals(accessType)) {
            return new NonStrictReadWriteAccessStrategy(this);
        }
        if (AccessType.READ_WRITE.equals(accessType)) {
            return new ReadWriteAccessStrategy(this);
        }
        if (AccessType.TRANSACTIONAL.equals(accessType)) {
            throw new CacheException("Transactional access is not currently supported by Hazelcast.");
        }
        throw new CacheException("Got unknown AccessType " + accessType
                + " while attempting to build CollectionRegionAccessStrategy.");
    }

}
