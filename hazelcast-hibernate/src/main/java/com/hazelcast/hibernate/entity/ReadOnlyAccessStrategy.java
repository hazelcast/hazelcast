package com.hazelcast.hibernate.entity;

import com.hazelcast.hibernate.access.ReadOnlyAccessDelegate;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
final class ReadOnlyAccessStrategy extends AbstractEntityRegionAccessStrategy {
    ReadOnlyAccessStrategy(final HazelcastEntityRegion entityRegion) {
        super(new ReadOnlyAccessDelegate<HazelcastEntityRegion>(entityRegion));
    }
}
