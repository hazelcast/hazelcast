package com.hazelcast.hibernate.entity;

import com.hazelcast.hibernate.access.ReadWriteAccessDelegate;

/**
 * Makes <b>READ COMMITTED</b> consistency guarantees even in a clustered environment.
 * 
 * @author Leo Kim (lkim@limewire.com)
 */
final class ReadWriteAccessStrategy extends AbstractEntityRegionAccessStrategy {
    ReadWriteAccessStrategy(final HazelcastEntityRegion entityRegion) {
        super(new ReadWriteAccessDelegate<HazelcastEntityRegion>(entityRegion));
    }
}
