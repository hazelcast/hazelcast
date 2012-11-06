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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.hibernate.CacheEnvironment;
import com.hazelcast.hibernate.HazelcastTimestamper;
import com.hazelcast.hibernate.region.HazelcastRegion;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;
import org.hibernate.cache.entry.CacheEntry;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

/**
 * Makes <b>READ COMMITTED</b> consistency guarantees even in a clustered environment.
 *
 * @author Leo Kim (lkim@limewire.com)
 */
public class ReadWriteAccessDelegate<T extends HazelcastRegion> extends AbstractAccessDelegate<T> {

    private static final int TRY_LOCK_AND_GET_TIMEOUT = 500;

    private final int lockTimeout;
    private final boolean explicitVersionCheckEnabled;
    private final long tryLockAndGetTimeout;

    public ReadWriteAccessDelegate(T hazelcastRegion, final Properties props) {
        super(hazelcastRegion, props);
        lockTimeout = CacheEnvironment.getLockTimeoutInSeconds(props);
        explicitVersionCheckEnabled = CacheEnvironment.isExplicitVersionCheckEnabled(props);
        final long maxOperationTimeout = HazelcastTimestamper.getMaxOperationTimeout(hazelcastRegion.getInstance());
        tryLockAndGetTimeout = Math.min(maxOperationTimeout, TRY_LOCK_AND_GET_TIMEOUT);
    }

    public boolean afterInsert(final Object key, final Object value, final Object version) throws CacheException {
        try {
            return put(key, value, version, null);
        } catch (TimeoutException e) {
            LOG.log(Level.FINEST, e.getMessage());
        }
        return false;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Called after <code>com.hazelcast.hibernate.access.ReadWriteAccessDelegate.lockItem()</code>
     */
    public boolean afterUpdate(final Object key, final Object value, final Object currentVersion, final Object previousVersion,
                               final SoftLock lock) throws CacheException {
        try {
            return put(key, value, currentVersion, previousVersion);
        } catch (TimeoutException e) {
            LOG.log(Level.FINEST, e.getMessage());
        } finally {
            unlockItem(key, lock);
        }
        return false;
    }

    public boolean putFromLoad(final Object key, final Object value, final long txTimestamp, final Object version,
                               final boolean minimalPutOverride) throws CacheException {
        try {
            return put(key, value, version, null);
        } catch (TimeoutException e) {
            LOG.log(Level.FINEST, e.getMessage());
        }
        return false;
    }

    private boolean put(final Object key, final Object value, final Object currentVersion, final Object previousVersion)
            throws TimeoutException {
        if (versionComparator != null) {
            if (explicitVersionCheckEnabled && value instanceof CacheEntry) {
                try {
                    final CacheEntry currentEntry = (CacheEntry) value;
                    if (getCache().tryLock(key, tryLockAndGetTimeout, TimeUnit.MILLISECONDS)) {
                        try {
                            final CacheEntry previousEntry = (CacheEntry) getCache().get(key);
                            if (previousEntry == null ||
                                    versionComparator.compare(currentEntry.getVersion(), previousEntry.getVersion()) > 0) {
                                getCache().put(key, value);
                                return true;
                            } else {
                                return false;
                            }
                        } finally {
                            getCache().unlock(key);
                        }
                    } else {
                        return false;
                    }
                } catch (HazelcastException e) {
                    LOG.log(Level.FINEST, "Skipping version check and put: " + e.getMessage());
                    return false;
                }
            } else if (previousVersion == null || versionComparator.compare(currentVersion, previousVersion) > 0) {
                return putInToCache(key, value);
            }
            return false;
        } else {
            return putInToCache(key, value);
        }
    }

    public SoftLock lockItem(final Object key, final Object version) throws CacheException {
        if (lockTimeout > 0) {
            if (!getCache().tryLock(key, lockTimeout, TimeUnit.SECONDS)) {
                throw new CacheException("Cache lock could not be acquired! Wait-time: " + lockTimeout + " seconds");
            }
        } else {
            getCache().lock(key);
        }
        return new SoftLock() {
        }; // dummy lock
    }

    public void unlockItem(final Object key, final SoftLock lock) throws CacheException {
        getCache().unlock(key);
    }

    public void unlockRegion(SoftLock lock) throws CacheException {
    }
}
