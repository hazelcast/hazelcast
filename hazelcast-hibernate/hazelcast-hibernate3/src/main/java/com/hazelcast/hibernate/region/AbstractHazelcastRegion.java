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

package com.hazelcast.hibernate.region;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.HazelcastTimestamper;
import com.hazelcast.hibernate.RegionCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.cache.CacheException;

import java.util.Map;
import java.util.Properties;

/**
 * Abstract superclass of Hazelcast region of Hibernate caches
 *
 * @param <Cache> implementation type of RegionCache
 */
abstract class AbstractHazelcastRegion<Cache extends RegionCache> implements HazelcastRegion<Cache> {

    protected final Properties props;
    private final HazelcastInstance instance;
    private final String regionName;
    private final int timeout;

    protected AbstractHazelcastRegion(final HazelcastInstance instance, final String regionName, final Properties props) {
        super();
        this.instance = instance;
        this.regionName = regionName;
        this.timeout = HazelcastTimestamper.getTimeout(instance, regionName);
        this.props = props;
    }

    public void destroy() throws CacheException {
//      Destroy of the region should not propagate
//      to other nodes of cluster.
//      Do nothing on destroy.
    }

    /**
     * @return The size of the internal <code>{@link com.hazelcast.core.IMap}</code>.
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
        final String name = getClass().getName();
        try {
            return instance.getLoggingService().getLogger(name);
        } catch (UnsupportedOperationException e) {
            // HazelcastInstance is instance of HazelcastClient.
            return Logger.getLogger(name);
        }
    }
}
