/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.hibernate;

import java.util.Properties;
import java.util.logging.Level;

import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.CollectionRegion;
import org.hibernate.cache.EntityRegion;
import org.hibernate.cache.QueryResultsRegion;
import org.hibernate.cache.RegionFactory;
import org.hibernate.cache.TimestampsRegion;
import org.hibernate.cfg.Settings;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.hibernate.collection.HazelcastCollectionRegion;
import com.hazelcast.hibernate.entity.HazelcastEntityRegion;
import com.hazelcast.hibernate.query.HazelcastQueryResultsRegion;
import com.hazelcast.hibernate.timestamp.HazelcastTimestampsRegion;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public class HazelcastCacheRegionFactory implements RegionFactory {

    private static final ILogger LOG = Logger.getLogger(HazelcastCacheRegionFactory.class.getName());

    private final IdGenerator idGenerator;

    public HazelcastCacheRegionFactory() {
        LOG.log(Level.INFO, "Initializing HazelcastCacheRegionFactory...");
        idGenerator = Hazelcast.getIdGenerator("HazelcastCacheRegionFactoryTimestampIdGenerator");
    }

    public HazelcastCacheRegionFactory(final Properties properties) {
        this();
    }

    public CollectionRegion buildCollectionRegion(final String regionName, final Properties properties,
                                                  final CacheDataDescription metadata) throws CacheException {
        return new HazelcastCollectionRegion(regionName, metadata);
    }

    public EntityRegion buildEntityRegion(final String regionName, final Properties properties,
                                          final CacheDataDescription metadata) throws CacheException {
        return new HazelcastEntityRegion(regionName, metadata);
    }

    public QueryResultsRegion buildQueryResultsRegion(final String regionName, final Properties properties)
            throws CacheException {
        return new HazelcastQueryResultsRegion(regionName);
    }

    public TimestampsRegion buildTimestampsRegion(final String regionName, final Properties properties)
            throws CacheException {
        return new HazelcastTimestampsRegion(regionName);
    }

    /**
     * From what I can tell from the <code>{@link org.hibernate.cache.CacheCurrencyStrategy}</code>s implemented in
     * Hibernate, the return value "false" will mean an object will be replaced in a cache if it already exists there,
     * and "true" will not replace it.
     *
     * @return true - for a large cluster, unnecessary puts will most likely slow things down.
     */
    public boolean isMinimalPutsEnabledByDefault() {
        return true;
    }

    /**
     * @return Output of <code>{@link Hazelcast#getIdGenerator}</code> and <code>{@link IdGenerator#newId()}</code>
     */
    public long nextTimestamp() {
        final long id = idGenerator.newId();
        return id;
    }

    public void start(final Settings settings, final Properties properties) throws CacheException {
        LOG.log(Level.INFO, "Starting up HazelcastCacheRegionFactory...");
    }

    /**
     * Calls <code>{@link Hazelcast#shutdown()}</code>.
     */
    public void stop() {
        LOG.log(Level.INFO, "Shutting down HazelcastCacheRegionFactory...");
        Hazelcast.shutdown();
    }
}
