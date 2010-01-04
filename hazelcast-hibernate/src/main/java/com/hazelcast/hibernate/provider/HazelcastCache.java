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

package com.hazelcast.hibernate.provider;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.hibernate.HazelcastCacheRegionFactory;
import org.hibernate.cache.Cache;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.Timestamper;

import java.util.Map;
import java.util.logging.Logger;

/**
 * Implementation of (deprecated) Hibernate <code>Cache</code> interface for compatibility with pre-Hibernate 3.3.x
 * code.
 *
 * @author Leo Kim (lkim@limewire.com)
 * @see HazelcastCacheProvider
 * @see HazelcastCacheRegionFactory
 */
public final class HazelcastCache implements Cache {

    private static final Logger LOG = Logger.getLogger(HazelcastCache.class.getName());
    private final IMap cache;
    private final String regionName;

    public HazelcastCache(final String regionName) {
        LOG.info("Creating new HazelcastCache with region name: " + regionName);
        cache = Hazelcast.getMap(regionName);
        this.regionName = regionName;
    }

    public void clear() throws CacheException {
        cache.clear();
    }

    public void destroy() throws CacheException {
        cache.destroy();
    }

    public long getElementCountInMemory() {
        return cache.size();
    }

    public long getElementCountOnDisk() {
        return -1L;
    }

    public String getRegionName() {
        return regionName;
    }

    /**
     * @return a rough estimate of number of bytes used by this region.
     */
    public long getSizeInMemory() {
        long size = 0;
        for (final Object key : cache.keySet()) {
            final MapEntry entry = cache.getMapEntry(key);
            if (entry != null) {
                size += entry.getCost();
            }
        }
        return size;
    }

    public int getTimeout() {
        return Timestamper.ONE_MS * 60000;
    }

    public long nextTimestamp() {
        return System.currentTimeMillis() / 100;
    }

    public void lock(final Object key) throws CacheException {
        cache.lock(key);
    }

    public void unlock(final Object key) throws CacheException {
        cache.unlock(key);
    }

    public Object get(final Object key) throws CacheException {
        return cache.get(key);
    }

    public Object read(final Object key) throws CacheException {
        return get(key);
    }

    public void put(final Object key, final Object value) throws CacheException {
        cache.put(key, value);
    }

    public void remove(final Object key) throws CacheException {
        cache.remove(key);
    }

    public void update(final Object key, final Object value) throws CacheException {
        put(key, value);
    }

    /**
     * @return the internal <code>IMap</code> used for this cache.
     */
    public Map toMap() {
        return cache;
    }
}
