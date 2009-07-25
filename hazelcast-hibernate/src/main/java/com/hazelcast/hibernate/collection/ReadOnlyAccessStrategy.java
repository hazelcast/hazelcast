package com.hazelcast.hibernate.collection;

import com.hazelcast.hibernate.access.ReadOnlyAccessDelegate;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
final class ReadOnlyAccessStrategy extends AbstractCollectionRegionAccessStrategy {
    ReadOnlyAccessStrategy(final HazelcastCollectionRegion collectionRegion) {
        super(new ReadOnlyAccessDelegate<HazelcastCollectionRegion>(collectionRegion));
    }
}
