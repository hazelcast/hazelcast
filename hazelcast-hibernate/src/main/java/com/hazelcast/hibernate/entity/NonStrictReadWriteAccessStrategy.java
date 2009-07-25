package com.hazelcast.hibernate.entity;

import com.hazelcast.hibernate.access.NonStrictReadWriteAccessDelegate;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
final class NonStrictReadWriteAccessStrategy extends AbstractEntityRegionAccessStrategy {
    NonStrictReadWriteAccessStrategy(final HazelcastEntityRegion entityRegion) {
        super(new NonStrictReadWriteAccessDelegate<HazelcastEntityRegion>(entityRegion));
    }
}
