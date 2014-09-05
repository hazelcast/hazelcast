/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.hibernate.distributed.IMapRegionCache;
import com.hazelcast.hibernate.region.HazelcastCollectionRegion;
import com.hazelcast.hibernate.region.HazelcastEntityRegion;
import com.hazelcast.hibernate.region.HazelcastTimestampsRegion;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.TimestampsRegion;

import java.util.Properties;

/**
 * Simple RegionFactory implementation to return Hazelcast based Region implementations
 */
public class HazelcastCacheRegionFactory extends AbstractHazelcastCacheRegionFactory implements RegionFactory {

    public HazelcastCacheRegionFactory() {
    }

    public HazelcastCacheRegionFactory(final HazelcastInstance instance) {
        super(instance);
    }

    public HazelcastCacheRegionFactory(final Properties properties) {
        super(properties);
    }

    public CollectionRegion buildCollectionRegion(final String regionName, final Properties properties,
                                                  final CacheDataDescription metadata) throws CacheException {
        return new HazelcastCollectionRegion<IMapRegionCache>(instance, regionName, properties, metadata,
                new IMapRegionCache(regionName, instance, properties, metadata));
    }

    public EntityRegion buildEntityRegion(final String regionName, final Properties properties,
                                          final CacheDataDescription metadata) throws CacheException {
        return new HazelcastEntityRegion<IMapRegionCache>(instance, regionName, properties, metadata,
                new IMapRegionCache(regionName, instance, properties, metadata));
    }

    public TimestampsRegion buildTimestampsRegion(final String regionName, final Properties properties)
            throws CacheException {
        return new HazelcastTimestampsRegion<IMapRegionCache>(instance, regionName, properties,
                new IMapRegionCache(regionName, instance, properties, null));
    }
}
