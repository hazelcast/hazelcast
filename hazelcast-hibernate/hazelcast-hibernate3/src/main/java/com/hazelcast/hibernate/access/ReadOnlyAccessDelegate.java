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

package com.hazelcast.hibernate.access;

import com.hazelcast.hibernate.region.HazelcastRegion;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;

import java.util.Properties;

/**
 * Guarantees that view is read-only and no updates can be made
 *
 * @author Leo Kim (lkim@limewire.com)
 * @param <T> implementation type of HazelcastRegion
 */
public class ReadOnlyAccessDelegate<T extends HazelcastRegion> extends NonStrictReadWriteAccessDelegate<T> {

    public ReadOnlyAccessDelegate(T hazelcastRegion, final Properties props) {
        super(hazelcastRegion, props);
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean afterUpdate(final Object key, final Object value, final Object currentVersion,
                               final Object previousVersion, final SoftLock lock) throws CacheException {
        throw new UnsupportedOperationException("Cannot update an item in a read-only cache: "
                + getHazelcastRegion().getName());
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public SoftLock lockItem(final Object key, final Object version) throws CacheException {
        throw new UnsupportedOperationException("Attempting to lock an item in a read-only cache region: "
                + getHazelcastRegion().getName());
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public SoftLock lockRegion() throws CacheException {
        throw new UnsupportedOperationException("Attempting to lock a read-only cache region: "
                + getHazelcastRegion().getName());
    }

    /**
     * This will issue a log warning stating that an attempt was made to unlock an item from a read-only cache region.
     */
    @Override
    public void unlockItem(final Object key, final SoftLock lock) throws CacheException {
        log.warning("Attempting to unlock an item from a read-only cache region");
    }

    /**
     * This will issue a log warning stating that an attempt was made to unlock a read-only cache region.
     */
    @Override
    public void unlockRegion(final SoftLock lock) throws CacheException {
        log.warning("Attempting to unlock a read-only cache region");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean update(final Object key, final Object value, final Object currentVersion,
                          final Object previousVersion) throws CacheException {
        throw new UnsupportedOperationException("Attempting to update an item in a read-only cache: "
                + getHazelcastRegion().getName());
    }
}
