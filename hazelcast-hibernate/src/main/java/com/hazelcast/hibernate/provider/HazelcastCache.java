/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hibernate.provider;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.hibernate.CacheEnvironment;
import com.hazelcast.hibernate.HazelcastCacheRegionFactory;
import com.hazelcast.hibernate.HazelcastTimestamper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.cache.Cache;
import org.hibernate.cache.CacheException;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Implementation of (deprecated) Hibernate <code>Cache</code> interface for compatibility with pre-Hibernate 3.3.x
 * code.
 *
 * @author Leo Kim (lkim@limewire.com)
 * @see HazelcastCacheProvider
 * @see HazelcastCacheRegionFactory
 */
public final class HazelcastCache implements Cache {

    private static final ILogger LOG = Logger.getLogger(HazelcastCache.class.getName());

    private final HazelcastInstance instance;
    private final IMap cache;
    private final String regionName;
    private final int timeout;
    private final int lockTimeout;

    public HazelcastCache(final HazelcastInstance instance, final String regionName, final Properties props) {
        LOG.log(Level.INFO, "Creating new HazelcastCache with region name: " + regionName);
        this.instance = instance;
        this.cache = instance.getMap(regionName);
        this.regionName = regionName;
        this.timeout = HazelcastTimestamper.getTimeout(instance, regionName);
        this.lockTimeout = CacheEnvironment.getLockTimeoutInMillis(props);
    }

    public void clear() throws CacheException {
        cache.clear();
    }

    public void destroy() throws CacheException {
//    	Destroy of the cache should not propagate 
//    	to other nodes of cluster.
//    	Do nothing on destroy.
    }

    public long getElementCountInMemory() {
        return cache.size();
    }

    /**
     * Hazelcast does not support pushing elements to disk.
     *
     * @return -1 this value means "unsupported"
     */
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
        return timeout;
    }

    public long nextTimestamp() {
        return HazelcastTimestamper.nextTimestamp(instance);
    }

    public void lock(final Object key) throws CacheException {
        if (lockTimeout > 0) {
            if (!cache.tryLock(key, lockTimeout, TimeUnit.MILLISECONDS)) {
                throw new CacheException("Cache lock could not be acquired! Wait-time: " + lockTimeout + " milliseconds");
            }
        } else {
            cache.lock(key);
        }
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
