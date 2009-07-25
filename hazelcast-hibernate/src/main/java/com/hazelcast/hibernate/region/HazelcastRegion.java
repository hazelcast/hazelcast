package com.hazelcast.hibernate.region;

import org.hibernate.cache.Region;

import com.hazelcast.core.IMap;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public interface HazelcastRegion extends Region {
    IMap getCache();
}
