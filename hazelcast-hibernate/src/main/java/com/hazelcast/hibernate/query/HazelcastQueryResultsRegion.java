package com.hazelcast.hibernate.query;

import org.hibernate.cache.QueryResultsRegion;

import com.hazelcast.hibernate.region.AbstractGeneralRegion;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public class HazelcastQueryResultsRegion extends AbstractGeneralRegion implements QueryResultsRegion {

    public HazelcastQueryResultsRegion(final String name) {
        super(name);
    }

}
