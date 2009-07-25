package com.hazelcast.hibernate.collection;

import com.hazelcast.hibernate.access.ReadWriteAccessDelegate;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
final class ReadWriteAccessStrategy extends AbstractCollectionRegionAccessStrategy {
    ReadWriteAccessStrategy(final HazelcastCollectionRegion collectionRegion) {
        super(new ReadWriteAccessDelegate<HazelcastCollectionRegion>(collectionRegion));
    }
}
