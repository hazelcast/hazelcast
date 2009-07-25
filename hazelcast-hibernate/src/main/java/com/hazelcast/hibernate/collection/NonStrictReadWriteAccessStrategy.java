package com.hazelcast.hibernate.collection;

import com.hazelcast.hibernate.access.NonStrictReadWriteAccessDelegate;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
final class NonStrictReadWriteAccessStrategy extends AbstractCollectionRegionAccessStrategy {

    NonStrictReadWriteAccessStrategy(final HazelcastCollectionRegion collectionRegion) {
        super(new NonStrictReadWriteAccessDelegate<HazelcastCollectionRegion>(collectionRegion));
    }
}
