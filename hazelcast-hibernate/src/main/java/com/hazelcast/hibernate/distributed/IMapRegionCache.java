package com.hazelcast.hibernate.distributed;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.hibernate.CacheEnvironment;
import com.hazelcast.hibernate.HazelcastTimestamper;
import com.hazelcast.hibernate.RegionCache;
import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.access.SoftLock;
import org.hibernate.cache.entry.CacheEntry;

import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

/**
 * @mdogan 11/9/12
 */
public class IMapRegionCache implements RegionCache {

    private final String name;
    private final HazelcastInstance hazelcastInstance;
    private final IMap<Object, Object> map;
    private final Comparator versionComparator;
    private final int lockTimeout;
    private final long tryLockAndGetTimeout;
    private final boolean explicitVersionCheckEnabled;

    public IMapRegionCache(final String name, final HazelcastInstance hazelcastInstance,
                           final Properties props, final CacheDataDescription metadata) {
        this.name = name;
        this.hazelcastInstance = hazelcastInstance;
        this.versionComparator = metadata != null && metadata.isVersioned() ? metadata.getVersionComparator() : null;
        this.map = hazelcastInstance.getMap(this.name);
        lockTimeout = CacheEnvironment.getLockTimeoutInMillis(props);
        final long maxOperationTimeout = HazelcastTimestamper.getMaxOperationTimeout(hazelcastInstance);
        tryLockAndGetTimeout = Math.min(maxOperationTimeout, 500);
        explicitVersionCheckEnabled = CacheEnvironment.isExplicitVersionCheckEnabled(props);
    }

    public Object get(final Object key) {
        return map.get(key);
    }

    public boolean put(final Object key, final Object value, final Object currentVersion,
                       final Object previousVersion, final SoftLock lock) {
        if (lock == LOCK_FAILURE) {
            hazelcastInstance.getLoggingService().getLogger(name).log(Level.WARNING, "Cache lock could not be acquired!");
            return false;
        }
        if (versionComparator != null && currentVersion != null) {
            if (explicitVersionCheckEnabled && value instanceof CacheEntry) {
                try {
                    final CacheEntry currentEntry = (CacheEntry) value;
                    final CacheEntry previousEntry = (CacheEntry) map.tryLockAndGet(key,
                            tryLockAndGetTimeout, TimeUnit.MILLISECONDS);
                    if (previousEntry == null ||
                        versionComparator.compare(currentEntry.getVersion(), previousEntry.getVersion()) > 0) {
                        map.putAndUnlock(key, value);
                        return true;
                    } else {
                        map.unlock(key);
                        return false;
                    }
                } catch (TimeoutException e) {
                    return false;
                }
            } else if (previousVersion == null || versionComparator.compare(currentVersion, previousVersion) > 0) {
                map.set(key, value, 0, TimeUnit.MILLISECONDS);
            }
            return false;
        } else {
            map.set(key, value, 0, TimeUnit.MILLISECONDS);
            return true;
        }
    }

    public boolean remove(final Object key) {
        return map.remove(key) != null;
    }

    public SoftLock tryLock(final Object key, final Object version) {
        return map.tryLock(key, lockTimeout, TimeUnit.MILLISECONDS) ? LOCK_SUCCESS : LOCK_FAILURE;
    }

    public void unlock(final Object key, SoftLock lock) {
        map.unlock(key);
    }

    public boolean contains(final Object key) {
        return map.containsKey(key);
    }

    public void clear() {
        // clear all cache and destroy proxies
        // when a new operation done over this proxy
        // Hazelcast will initialize and create map again.
        map.destroy();
        // create Hazelcast internal proxies, has no effect on map operations
        hazelcastInstance.getMap(name);
    }

    public long size() {
        return map.size();
    }

    public long getSizeInMemory() {
        long size = 0;
        for (final Object key : map.keySet()) {
            final MapEntry entry = map.getMapEntry(key);
            if (entry != null) {
                size += entry.getCost();
            }
        }
        return size;
    }

    public Map asMap() {
        return map;
    }

    private static final SoftLock LOCK_SUCCESS = new SoftLock() {};

    private static final SoftLock LOCK_FAILURE = new SoftLock() {};
}
