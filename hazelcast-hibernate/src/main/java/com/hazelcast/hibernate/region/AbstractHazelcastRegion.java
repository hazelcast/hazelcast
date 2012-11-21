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

package com.hazelcast.hibernate.region;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.hibernate.HazelcastTimestamper;
import com.hazelcast.hibernate.RegionCache;
import com.hazelcast.logging.ILogger;
import org.hibernate.cache.CacheException;

import java.util.Map;
import java.util.Properties;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
abstract class AbstractHazelcastRegion<Cache extends RegionCache> implements HazelcastRegion<Cache> {

    private final HazelcastInstance instance;
//    private final IMap cache;
    private final String regionName;
    private final int timeout;
    protected final Properties props;

    protected AbstractHazelcastRegion(final HazelcastInstance instance, final String regionName, final Properties props) {
        super();
        this.instance = instance;
//        this.cache = instance.getMap(regionName);
        this.regionName = regionName;
        this.timeout = HazelcastTimestamper.getTimeout(instance, regionName);
        this.props = props;
    }

//    public final RegionCache getCache() {
//        return cache;
//    }

//    public final void clearCache() {
        // clear all cache and destroy proxies
        // when a new operation done over this proxy
        // Hazelcast will initialize and create map again.
//        cache.destroy();
        // create Hazelcast internal proxies, has no effect on map operations
//        instance.getMap(regionName);
//    }

    public void destroy() throws CacheException {
//    	Destroy of the region should not propagate 
//    	to other nodes of cluster.
//    	Do nothing on destroy.
    }

    /**
     * @return The size of the internal <code>{@link IMap}</code>.
     */
    public long getElementCountInMemory() {
        return getCache().size();
    }

    /**
     * Hazelcast does not support pushing elements to disk.
     *
     * @return -1 this value means "unsupported"
     */
    public long getElementCountOnDisk() {
        return -1;
    }

    /**
     * @return The name of the region.
     */
    public String getName() {
        return regionName;
    }

    /**
     * @return a rough estimate of number of bytes used by this region.
     */
    public long getSizeInMemory() {
//        long size = 0;
//        for (final Object key : getCache().keySet()) {
//            final MapEntry entry = getCache().getMapEntry(key);
//            if (entry != null) {
//                size += entry.getCost();
//            }
//        }
//        return size;
        return getCache().getSizeInMemory();
    }

    public final int getTimeout() {
        return timeout;
    }

    public final long nextTimestamp() {
        return HazelcastTimestamper.nextTimestamp(instance);
    }

    /**
     * Appears to be used only by <code>org.hibernate.stat.SecondLevelCacheStatistics</code>.
     *
     * @return the internal <code>IMap</code> used for this region.
     */
    public Map toMap() {
        return getCache().asMap();
    }

    public boolean contains(Object key) {
        return getCache().contains(key);
    }

    public final HazelcastInstance getInstance() {
        return instance;
    }

    public final ILogger getLogger() {
        return instance.getLoggingService().getLogger(getClass().getName());
    }
}
