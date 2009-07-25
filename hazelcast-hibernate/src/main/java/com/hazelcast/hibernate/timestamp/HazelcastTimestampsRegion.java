package com.hazelcast.hibernate.timestamp;

import org.hibernate.cache.TimestampsRegion;

import com.hazelcast.hibernate.region.AbstractGeneralRegion;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public class HazelcastTimestampsRegion extends AbstractGeneralRegion implements TimestampsRegion {

    public HazelcastTimestampsRegion(final String name) {
        super(name);
    }

}
