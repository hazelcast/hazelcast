/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.hibernate.region.HazelcastRegion;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;

import java.util.Properties;

/**
 * Makes no guarantee of consistency between the cache and the database. Stale data from the cache is possible if expiry
 * is not configured appropriately.
 *
 * @author Leo Kim (lkim@limewire.com)
 * @param <T> implementation type of HazelcastRegion
 */
public class NonStrictReadWriteAccessDelegate<T extends HazelcastRegion> extends AbstractAccessDelegate<T> {

    public NonStrictReadWriteAccessDelegate(T hazelcastRegion, final Properties props) {
        super(hazelcastRegion, props);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Returns <code>false</code> since this is a non-strict read/write cache access strategy
     */
    public boolean afterInsert(final Object key, final Object value, final Object version) throws CacheException {
        return false;
    }

    public boolean afterUpdate(final Object key, final Object value, final Object currentVersion, final Object previousVersion,
                               final SoftLock lock) throws CacheException {
        unlockItem(key, lock);
        return false;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Returns <code>false</code> since this is an asynchronous cache access strategy.
     */
    public boolean insert(final Object key, final Object value, final Object version) throws CacheException {
        return false;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Removes the entry since this is a non-strict read/write cache strategy.
     */
    public boolean update(final Object key, final Object value, final Object currentVersion, final Object previousVersion) {
        remove(key);
        return false;
    }

    @Override
    public void remove(final Object key) throws CacheException {
        try {
            cache.remove(key);
        } catch (HazelcastException e) {
            throw new CacheException("Operation timeout during remove operation from cache!", e);
        }
    }

    public SoftLock lockItem(Object key, Object version) throws CacheException {
        return null;
    }

    public void removeAll() throws CacheException {
        cache.clear();
    }

    public void unlockItem(final Object key, final SoftLock lock) throws CacheException {
        remove(key);
    }
}
