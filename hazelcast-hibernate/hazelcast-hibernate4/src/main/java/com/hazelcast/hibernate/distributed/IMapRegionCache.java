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

package com.hazelcast.hibernate.distributed;

import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.hibernate.CacheEnvironment;
import com.hazelcast.hibernate.HazelcastTimestamper;
import com.hazelcast.hibernate.RegionCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.cache.spi.entry.CacheEntry;

import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A {@link RegionCache} implementation based on the underlying IMap
 */
public class IMapRegionCache implements RegionCache {

    private static final long COMPARISON_VALUE = 500;

    private static final SoftLock LOCK_SUCCESS = new SoftLock() {
    };

    private static final SoftLock LOCK_FAILURE = new SoftLock() {
    };

    private final String name;
    private final HazelcastInstance hazelcastInstance;
    private final IMap<Object, Object> map;
    private final Comparator versionComparator;
    private final int lockTimeout;
    private final long tryLockAndGetTimeout;
    private final boolean explicitVersionCheckEnabled;
    private final ILogger logger;

    public IMapRegionCache(final String name, final HazelcastInstance hazelcastInstance,
                           final Properties props, final CacheDataDescription metadata) {
        this.name = name;
        this.hazelcastInstance = hazelcastInstance;
        this.versionComparator = metadata != null && metadata.isVersioned() ? metadata.getVersionComparator() : null;
        this.map = hazelcastInstance.getMap(this.name);
        lockTimeout = CacheEnvironment.getLockTimeoutInMillis(props);
        final long maxOperationTimeout = HazelcastTimestamper.getMaxOperationTimeout(hazelcastInstance);
        tryLockAndGetTimeout = Math.min(maxOperationTimeout, COMPARISON_VALUE);
        explicitVersionCheckEnabled = CacheEnvironment.isExplicitVersionCheckEnabled(props);
        logger = createLogger(name, hazelcastInstance);
    }

    public Object get(final Object key) {
        return map.get(key);
    }

    public boolean put(final Object key, final Object value, final Object currentVersion) {
        return update(key, value, currentVersion, null, null);
    }

    public boolean update(final Object key, final Object value, final Object currentVersion,
                          final Object previousVersion, final SoftLock lock) {
        if (lock == LOCK_FAILURE) {
            logger.warning("Cache lock could not be acquired!");
            return false;
        }
        if (versionComparator != null && currentVersion != null) {
            if (explicitVersionCheckEnabled && value instanceof CacheEntry) {
                return compareVersion(key, value);
            } else if (previousVersion == null || versionComparator.compare(currentVersion, previousVersion) > 0) {
                map.set(key, value);
                return true;
            }
            return false;
        } else {
            map.set(key, value);
            return true;
        }
    }

    public boolean remove(final Object key) {
        return map.remove(key) != null;
    }

    public SoftLock tryLock(final Object key, final Object version) {
        try {
            return map.tryLock(key, lockTimeout, TimeUnit.MILLISECONDS) ? LOCK_SUCCESS : LOCK_FAILURE;
        } catch (InterruptedException e) {
            return LOCK_FAILURE;
        }
    }

    public void unlock(final Object key, SoftLock lock) {
        if (lock == LOCK_SUCCESS) {
            map.unlock(key);
        }
    }

    public boolean contains(final Object key) {
        return map.containsKey(key);
    }

    public void clear() {
        map.evictAll();
    }

    public long size() {
        return map.size();
    }

    public long getSizeInMemory() {
        long size = 0;
        for (final Object key : map.keySet()) {
            final EntryView entry = map.getEntryView(key);
            if (entry != null) {
                size += entry.getCost();
            }
        }
        return size;
    }

    public Map asMap() {
        return map;
    }

    private ILogger createLogger(final String name, final HazelcastInstance hazelcastInstance) {
        try {
            return hazelcastInstance.getLoggingService().getLogger(name);
        } catch (UnsupportedOperationException e) {
            return Logger.getLogger(name);
        }
    }

    private boolean compareVersion(Object key, Object value) {
        final CacheEntry currentEntry = (CacheEntry) value;

        // Ideally this would be an entry processor. Unfortunately there is no guarantee that
        // the versionComparator is Serializable.
        //
        // Previously, this was implemented using a `map.get` followed by `map.set` wrapped inside a `map.tryLock`
        // block.  Unfortunately this implementation was prone to `IllegalMonitorStateException` when the lock was
        // released under heavy load or after network partitions.  Hence this implementation now uses a spin loop
        // around atomic operations.
        long timeout = System.currentTimeMillis() + tryLockAndGetTimeout;
        do {
            final CacheEntry previousEntry = (CacheEntry) map.get(key);
            if (previousEntry == null) {
                if (map.putIfAbsent(key, value) == null) {
                    return true;
                }
            } else if (versionComparator.compare(currentEntry.getVersion(), previousEntry.getVersion()) > 0) {
                if (map.replace(key, previousEntry, value)) {
                    return true;
                }
            } else {
                return false;
            }
        } while (System.currentTimeMillis() < timeout);

        return false;
    }
}
