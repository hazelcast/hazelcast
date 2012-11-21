package com.hazelcast.hibernate.region;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.RegionCache;
import com.hazelcast.hibernate.access.NonStrictReadWriteAccessDelegate;
import com.hazelcast.hibernate.access.ReadOnlyAccessDelegate;
import com.hazelcast.hibernate.access.ReadWriteAccessDelegate;
import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.EntityRegion;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cache.access.EntityRegionAccessStrategy;

import java.util.Properties;

/**
 * @mdogan 11/9/12
 */
public final class HazelcastEntityRegion<Cache extends RegionCache>
        extends AbstractTransactionalDataRegion<Cache> implements EntityRegion {

    public HazelcastEntityRegion(final HazelcastInstance instance,
                                 final String regionName, final Properties props,
                                 final CacheDataDescription metadata, final Cache cache) {
        super(instance, regionName, props, metadata, cache);
    }

    public EntityRegionAccessStrategy buildAccessStrategy(final AccessType accessType) throws CacheException {
        if (null == accessType) {
            throw new CacheException(
                    "Got null AccessType while attempting to determine a proper EntityRegionAccessStrategy. This can't happen!");
        }
        if (AccessType.READ_ONLY.equals(accessType)) {
            return new EntityRegionAccessStrategyAdapter(
                    new ReadOnlyAccessDelegate<HazelcastEntityRegion>(this, props));
        }
        if (AccessType.NONSTRICT_READ_WRITE.equals(accessType)) {
            return new EntityRegionAccessStrategyAdapter(
                    new NonStrictReadWriteAccessDelegate<HazelcastEntityRegion>(this, props));
        }
        if (AccessType.READ_WRITE.equals(accessType)) {
            return new EntityRegionAccessStrategyAdapter(
                    new ReadWriteAccessDelegate<HazelcastEntityRegion>(this, props));
        }
        if (AccessType.TRANSACTIONAL.equals(accessType)) {
            throw new CacheException("Transactional access is not currently supported by Hazelcast.");
        }
        throw new CacheException("Got unknown AccessType \"" + accessType
                                 + "\" while attempting to build EntityRegionAccessStrategy.");
    }

}
