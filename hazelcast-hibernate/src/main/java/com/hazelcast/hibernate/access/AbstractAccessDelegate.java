package com.hazelcast.hibernate.access;

import com.hazelcast.core.IMap;
import com.hazelcast.hibernate.region.HazelcastRegion;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public abstract class AbstractAccessDelegate<T extends HazelcastRegion> implements AccessDelegate<T> {
    private final T hazelcastRegion;

    protected AbstractAccessDelegate(final T hazelcastRegion) {
        this.hazelcastRegion = hazelcastRegion;
    }

    public final T getHazelcastRegion() {
        return hazelcastRegion;
    }

    public final IMap getCache() {
        return getHazelcastRegion().getCache();
    }

}
