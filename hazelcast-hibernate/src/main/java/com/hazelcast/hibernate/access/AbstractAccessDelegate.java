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

package com.hazelcast.hibernate.access;

import com.hazelcast.core.IMap;
import com.hazelcast.hibernate.region.AbstractTransactionalDataRegion;
import com.hazelcast.hibernate.region.HazelcastRegion;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;

import java.util.Comparator;
import java.util.Properties;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public abstract class AbstractAccessDelegate<T extends HazelcastRegion> implements AccessDelegate<T> {

    protected final ILogger LOG = Logger.getLogger(getClass().getName());
    private final T hazelcastRegion;
    protected final Comparator<Object> versionComparator;

    protected AbstractAccessDelegate(final T hazelcastRegion, final Properties props) {
        super();
        this.hazelcastRegion = hazelcastRegion;
        if (hazelcastRegion instanceof AbstractTransactionalDataRegion) {
            this.versionComparator = ((AbstractTransactionalDataRegion) hazelcastRegion).getCacheDataDescription().getVersionComparator();
        } else {
            this.versionComparator = null;
        }
    }

    public final T getHazelcastRegion() {
        return hazelcastRegion;
    }

    public final IMap getCache() {
        return hazelcastRegion.getCache();
    }

    protected boolean putTransient(final Object key, final Object value) {
        getCache().putTransient(key, value, 0, null);
        return true;
    }

    public Object get(final Object key, final long txTimestamp) throws CacheException {
        return getCache().get(key);
    }

    public boolean putFromLoad(final Object key, final Object value, final long txTimestamp, final Object version) throws CacheException {
        return putFromLoad(key, value, txTimestamp, version, true);
    }

    public void remove(final Object key) throws CacheException {
        getCache().remove(key);
    }

    public void removeAll() throws CacheException {
        hazelcastRegion.clearCache();
    }

    public void evict(final Object key) throws CacheException {
        remove(key);
    }

    public void evictAll() throws CacheException {
        hazelcastRegion.clearCache();
    }

    /**
     * NO-OP
     */
    public SoftLock lockRegion() throws CacheException {
        return null;
    }

    /**
     * NO-OP
     */
    public void unlockRegion(final SoftLock lock) throws CacheException {
    }

    /**
     * This is an asynchronous cache access strategy.
     * NO-OP
     */
    public boolean insert(final Object key, final Object value, final Object version) throws CacheException {
        return false;
    }

    /**
     * This is an asynchronous cache access strategy.
     * NO-OP
     */
    public boolean update(final Object key, final Object value, final Object currentVersion, final Object previousVersion)
            throws CacheException {
        return false;
    }
}
