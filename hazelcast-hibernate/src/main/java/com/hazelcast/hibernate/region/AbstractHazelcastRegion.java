package com.hazelcast.hibernate.region;

import java.util.Map;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.Region;
import org.hibernate.cache.Timestamper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
abstract class AbstractHazelcastRegion implements HazelcastRegion {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastRegion.class);
    private final IMap cache;
    private final String regionName;

    protected AbstractHazelcastRegion(final String regionName) {
        cache = Hazelcast.getMap(regionName);
        this.regionName = regionName;
    }

    public final IMap getCache() {
        return cache;
    }

    /**
     * Calls <code>{@link IMap#destroy()}</code> on the given <code>{@link Region}</code>.
     */
    public void destroy() throws CacheException {
        LOG.info("Calling destroy on {}", regionName);
        getCache().destroy();
    }

    /**
     * @return The size of the internal <code>{@link IMap}</code>.
     */
    public long getElementCountInMemory() {
        return getCache().size();
    }

    /**
     * Hazelcast does not support pushing elements to disk.
     * 
     * @return -1 (according to <code>{@link Region}</code>, this value means "unsupported"
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
        long size = 0;
        for (final Object key : getCache().keySet()) {
            final MapEntry entry = getCache().getMapEntry(key);
            if (entry != null) {
                size += entry.getCost();
            }
        }
        return size;
    }

    /**
     * TODO: I am not clear as to what this is a timeout for.
     * 
     * @return 60000 (milliseconds)
     */
    public int getTimeout() {
        return Timestamper.ONE_MS * 60000;
    }

    /**
     * @return <code>{@link System#currentTimeMillis}</code>/100.
     */
    public long nextTimestamp() {
        return System.currentTimeMillis() / 100;
    }

    /**
     * Appears to be used only by <code>org.hibernate.stat.SecondLevelCacheStatistics</code>.
     * 
     * @return the internal <code>IMap</code> used for this region.
     */
    public Map toMap() {
        return getCache();
    }
}
