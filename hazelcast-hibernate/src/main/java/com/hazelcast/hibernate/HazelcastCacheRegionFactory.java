/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
 */

package com.hazelcast.hibernate;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.collection.HazelcastCollectionRegion;
import com.hazelcast.hibernate.entity.HazelcastEntityRegion;
import com.hazelcast.hibernate.instance.HazelcastInstanceFactory;
import com.hazelcast.hibernate.instance.IHazelcastInstanceLoader;
import com.hazelcast.hibernate.query.HazelcastQueryResultsRegion;
import com.hazelcast.hibernate.timestamp.HazelcastTimestampsRegion;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.cache.*;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cfg.Settings;

import java.util.Properties;
import java.util.logging.Level;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public class HazelcastCacheRegionFactory implements RegionFactory {

    private static final ILogger LOG = Logger.getLogger(HazelcastCacheRegionFactory.class.getName());

    private IHazelcastInstanceLoader instanceLoader = null;
    private HazelcastInstance instance;

    public HazelcastCacheRegionFactory() {
    }

    public HazelcastCacheRegionFactory(final Properties properties) {
        this();
    }

    public HazelcastCacheRegionFactory(final HazelcastInstance instance) {
        this.instance = instance;
    }

    public CollectionRegion buildCollectionRegion(final String regionName, final Properties properties,
                                                  final CacheDataDescription metadata) throws CacheException {
        return new HazelcastCollectionRegion(instance, regionName, properties, metadata);
    }

    public EntityRegion buildEntityRegion(final String regionName, final Properties properties,
                                          final CacheDataDescription metadata) throws CacheException {
        return new HazelcastEntityRegion(instance, regionName, properties, metadata);
    }

    public QueryResultsRegion buildQueryResultsRegion(final String regionName, final Properties properties)
            throws CacheException {
        return new HazelcastQueryResultsRegion(instance, regionName, properties);
    }

    public TimestampsRegion buildTimestampsRegion(final String regionName, final Properties properties)
            throws CacheException {
        return new HazelcastTimestampsRegion(instance, regionName, properties);
    }

    /**
     * @return true - for a large cluster, unnecessary puts will most likely slow things down.
     */
    public boolean isMinimalPutsEnabledByDefault() {
        return true;
    }

    public long nextTimestamp() {
        return HazelcastTimestamper.nextTimestamp(instance);
    }

    public void start(final Settings settings, final Properties properties) throws CacheException {
        LOG.log(Level.INFO, "Starting up HazelcastCacheRegionFactory...");
        if (instance == null || !instance.getLifecycleService().isRunning()) {
            instanceLoader = HazelcastInstanceFactory.createInstanceLoader(properties);
            instance = instanceLoader.loadInstance();
        }
    }

    public void stop() {
        if (instanceLoader != null) {
            LOG.log(Level.INFO, "Shutting down HazelcastCacheRegionFactory...");
            instanceLoader.unloadInstance();
            instance = null;
            instanceLoader = null;
        }
    }

    public HazelcastInstance getHazelcastInstance() {
        return instance;
    }

    public AccessType getDefaultAccessType() {
        return AccessType.READ_WRITE;
    }
}
