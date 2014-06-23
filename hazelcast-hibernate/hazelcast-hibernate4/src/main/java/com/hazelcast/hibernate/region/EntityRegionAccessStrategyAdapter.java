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

import com.hazelcast.hibernate.access.AccessDelegate;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;

/**
 * Simple adapter implementation for transactional / concurrent access control on entities
 *
 * @author Leo Kim (lkim@limewire.com)
 */
public final class EntityRegionAccessStrategyAdapter implements EntityRegionAccessStrategy {

    private final AccessDelegate<? extends HazelcastEntityRegion> delegate;

    public EntityRegionAccessStrategyAdapter(final AccessDelegate<? extends HazelcastEntityRegion> delegate) {
        this.delegate = delegate;
    }

    public boolean afterInsert(final Object key, final Object value, final Object version) throws CacheException {
        return delegate.afterInsert(key, value, version);
    }

    public boolean afterUpdate(final Object key, final Object value, final Object currentVersion,
                               final Object previousVersion, final SoftLock lock) throws CacheException {
        return delegate.afterUpdate(key, value, currentVersion, previousVersion, lock);
    }

    public void evict(final Object key) throws CacheException {
        delegate.evict(key);
    }

    public void evictAll() throws CacheException {
        delegate.evictAll();
    }

    public Object get(final Object key, final long txTimestamp) throws CacheException {
        return delegate.get(key, txTimestamp);
    }

    public EntityRegion getRegion() {
        return delegate.getHazelcastRegion();
    }

    public boolean insert(final Object key, final Object value, final Object version) throws CacheException {
        return delegate.insert(key, value, version);
    }

    public SoftLock lockItem(final Object key, final Object version) throws CacheException {
        return delegate.lockItem(key, version);
    }

    public SoftLock lockRegion() throws CacheException {
        return delegate.lockRegion();
    }

    public boolean putFromLoad(final Object key, final Object value, final long txTimestamp, final Object version)
            throws CacheException {
        return delegate.putFromLoad(key, value, txTimestamp, version);
    }

    public boolean putFromLoad(final Object key, final Object value, final long txTimestamp, final Object version,
                               final boolean minimalPutOverride) throws CacheException {
        return delegate.putFromLoad(key, value, txTimestamp, version, minimalPutOverride);
    }

    public void remove(final Object key) throws CacheException {
        delegate.remove(key);
    }

    public void removeAll() throws CacheException {
        delegate.removeAll();
    }

    public void unlockItem(final Object key, final SoftLock lock) throws CacheException {
        delegate.unlockItem(key, lock);
    }

    public void unlockRegion(final SoftLock lock) throws CacheException {
        delegate.unlockRegion(lock);
    }

    public boolean update(final Object key, final Object value, final Object currentVersion,
                          final Object previousVersion) throws CacheException {
        return delegate.update(key, value, currentVersion, previousVersion);
    }
}
