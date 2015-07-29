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
import org.hibernate.cache.spi.access.SoftLock;

import java.util.Properties;

/**
 * Makes <b>READ COMMITTED</b> consistency guarantees even in a clustered environment.
 *
 * @author Leo Kim (lkim@limewire.com)
 * @param <T> implementation type of HazelcastRegion
 */
public class ReadWriteAccessDelegate<T extends HazelcastRegion> extends AbstractAccessDelegate<T> {


    public ReadWriteAccessDelegate(T hazelcastRegion, final Properties props) {
        super(hazelcastRegion, props);
    }

    @Override
    public boolean afterInsert(final Object key, final Object value, final Object version) throws CacheException {
        try {
            return cache.insert(key, value, version);
        } catch (HazelcastException e) {
            if (log.isFinestEnabled()) {
                log.finest("Could not insert into Cache[" + hazelcastRegion.getName() + "]: " + e.getMessage());
            }
            return false;
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Called after <code>com.hazelcast.ReadWriteAccessDelegate.lockItem()</code>
     */
    @Override
    public boolean afterUpdate(final Object key, final Object value, final Object currentVersion, final Object previousVersion,
                               final SoftLock lock) throws CacheException {
        try {
            return cache.update(key, value, currentVersion, lock);
        } catch (HazelcastException e) {
            if (log.isFinestEnabled()) {
                log.finest("Could not update Cache[" + hazelcastRegion.getName() + "]: " + e.getMessage());
            }
            return false;
        }
    }

    /**
     * This is an asynchronous cache access strategy.
     * NO-OP
     */
    @Override
    public boolean insert(final Object key, final Object value, final Object version) throws CacheException {
        return false;
    }

    @Override
    public SoftLock lockItem(final Object key, final Object version) throws CacheException {
        return cache.tryLock(key, version);
    }

    @Override
    public void unlockItem(final Object key, final SoftLock lock) throws CacheException {
        cache.unlock(key, lock);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * A no-op since this is an asynchronous cache access strategy.
     */
    @Override
    public boolean update(Object key, Object value, Object currentVersion, Object previousVersion)
            throws CacheException {
        return false;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * A no-op since this is an asynchronous cache access strategy.
     */
    @Override
    public void remove(final Object key) throws CacheException {
    }

    /**
     * This is an asynchronous cache access strategy.
     * NO-OP
     */
    @Override
    public void removeAll() throws CacheException {
    }
}
