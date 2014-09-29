package com.hazelcast.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.map.eviction.EvictionHelper;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.eviction.EvictionHelper.checkEvictable;
import static com.hazelcast.map.eviction.EvictionHelper.evictableSize;
import static com.hazelcast.map.eviction.EvictionHelper.fireEvent;

/**
 * Contains eviction specific functionality.
 */
abstract class AbstractEvictableRecordStore extends AbstractRecordStore {

    /**
     * Number of reads before clean up.
     * A nice number such as 2^n - 1.
     */
    private static final int POST_READ_CHECK_POINT = 63;

    /**
     * Flag for checking if this record store has at least one candidate entry
     * for expiration (idle or tll) or not.
     */
    private volatile boolean expirable;

    /**
     * Iterates over a pre-set entry count/percentage in one round.
     * Used in expiration logic for traversing entries. Initializes lazily.
     */
    private Iterator<Record> expirationIterator;

    /**
     * If there is no clean-up caused by puts after some time,
     * count a number of gets and start eviction.
     */
    private int readCountBeforeCleanUp;

    /**
     * used in LRU eviction logic.
     */
    private long lruAccessSequenceNumber;

    /**
     * Last run time of cleanup operation.
     */
    private long lastEvictionTime;

    private final boolean evictionEnabled;

    private final long minEvictionCheckMillis;

    private final long maxIdleTime;

    private final MapConfig.EvictionPolicy evictionPolicy;

    protected AbstractEvictableRecordStore(MapContainer mapContainer, int partitionId) {
        super(mapContainer, partitionId);
        final MapConfig mapConfig = mapContainer.getMapConfig();
        this.minEvictionCheckMillis = mapConfig.getMinEvictionCheckMillis();
        this.evictionPolicy = mapContainer.getMapConfig().getEvictionPolicy();
        this.evictionEnabled
                = !MapConfig.EvictionPolicy.NONE.equals(this.evictionPolicy);
        this.expirable = isRecordStoreExpirable();
        this.maxIdleTime = calculateMaxIdleTime();
    }

    private boolean isRecordStoreExpirable() {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        return mapConfig.getMaxIdleSeconds() > 0
                || mapConfig.getTimeToLiveSeconds() > 0;
    }

    @Override
    public void evictExpiredEntries(int percentage, boolean backup) {
        final long now = getNow();
        final int size = size();
        final int maxIterationCount = getMaxIterationCount(size, percentage);
        final int maxRetry = 3;
        int loop = 0;
        int evictedEntryCount = 0;
        while (true) {
            evictedEntryCount += evictExpiredEntriesInternal(maxIterationCount, now, backup);
            if (evictedEntryCount >= maxIterationCount) {
                break;
            }
            loop++;
            if (loop > maxRetry) {
                break;
            }
        }
    }

    @Override
    public boolean isExpirable() {
        return expirable;
    }

    /**
     * Intended to put an upper bound to iterations. Used in evictions.
     *
     * @param size       of iterate-able.
     * @param percentage percentage of size.
     * @return 100 If calculated iteration count is less than 100, otherwise returns calculated iteration count.
     */
    private int getMaxIterationCount(int size, int percentage) {
        final int defaultMaxIterationCount = 100;
        final float oneHundred = 100F;
        float maxIterationCount = size * (percentage / oneHundred);
        if (maxIterationCount <= defaultMaxIterationCount) {
            return defaultMaxIterationCount;
        }
        return Math.round(maxIterationCount);
    }

    private int evictExpiredEntriesInternal(int maxIterationCount, long now, boolean backup) {
        int evictedCount = 0;
        int checkedEntryCount = 0;
        initExpirationIterator();
        while (expirationIterator.hasNext()) {
            if (checkedEntryCount >= maxIterationCount) {
                break;
            }
            checkedEntryCount++;
            Record record = expirationIterator.next();
            record = getOrNullIfExpired(record, now, backup);
            if (record == null) {
                evictedCount++;
            }
        }
        return evictedCount;
    }

    private void initExpirationIterator() {
        if (expirationIterator == null || !expirationIterator.hasNext()) {
            expirationIterator = records.values().iterator();
        }
    }

    protected void resetAccessSequenceNumber() {
        lruAccessSequenceNumber = 0L;
    }

    /**
     * TODO make checkEvictable fast by carrying threshold logic to partition.
     * This cleanup adds some latency to write operations.
     * But it sweeps records much better under high write loads.
     * <p/>
     *
     * @param now now in time.
     */
    protected void evictEntries(long now, boolean backup) {
        if (evictionEnabled) {
            cleanUp(now, backup);
        }
    }

    /**
     * If there is no clean-up caused by puts after some time,
     * try to clean-up from gets.
     *
     * @param now now.
     */
    protected void postReadCleanUp(long now, boolean backup) {
        if (evictionEnabled) {
            readCountBeforeCleanUp++;
            if ((readCountBeforeCleanUp & POST_READ_CHECK_POINT) == 0) {
                cleanUp(now, backup);
            }
        }

    }

    /**
     * Makes eviction clean-up logic.
     *
     * @param now    now in millis.
     * @param backup <code>true</code> if running on a backup partition, otherwise <code>false</code>
     */
    private void cleanUp(long now, boolean backup) {
        if (size() == 0) {
            return;
        }
        if (shouldEvict(now)) {
            removeEvictableRecords(backup);
            lastEvictionTime = now;
            readCountBeforeCleanUp = 0;
        }
    }

    protected boolean shouldEvict(long now) {
        return evictionEnabled && inEvictableTimeWindow(now) && isEvictable();
    }

    private void removeEvictableRecords(boolean backup) {
        final int evictableSize = getEvictableSize();
        if (evictableSize < 1) {
            return;
        }
        final MapConfig mapConfig = mapContainer.getMapConfig();
        EvictionHelper.removeEvictableRecords(this, evictableSize, mapConfig, mapServiceContext, backup);
    }

    private int getEvictableSize() {
        final int size = size();
        if (size < 1) {
            return 0;
        }
        final int evictableSize = evictableSize(size, mapContainer.getMapConfig(), mapServiceContext);
        if (evictableSize < 1) {
            return 0;
        }
        return evictableSize;
    }


    /**
     * Eviction waits at least {@link #minEvictionCheckMillis} milliseconds to run.
     *
     * @return <code>true</code> if in that time window,
     * otherwise <code>false</code>
     */
    private boolean inEvictableTimeWindow(long now) {
        return minEvictionCheckMillis == 0L
                || (now - lastEvictionTime) > minEvictionCheckMillis;
    }

    private boolean isEvictable() {
        return checkEvictable(mapContainer, partitionId);
    }

    protected void markRecordStoreExpirable(long ttl) {
        if (ttl > 0L) {
            expirable = true;
        }
    }

    abstract Object evictInternal(Data key, boolean backup);

    /**
     * Check if record is reachable according to ttl or idle times.
     * If not reachable return null.
     *
     * @param record {@link com.hazelcast.map.record.Record}
     * @return null if evictable.
     */
    protected Record getOrNullIfExpired(Record record, long now, boolean backup) {
        if (!expirable) {
            return record;
        }
        if (record == null) {
            return null;
        }
        final Data key = record.getKey();
        if (isLocked(key)) {
            return record;
        }
        if (!isExpired(record, now)) {
            return record;
        }
        final Object value = record.getValue();
        evictInternal(key, backup);
        if (!backup) {
            doPostExpirationOperations(key, value);
        }
        return null;
    }

    private boolean isExpired(Record record, long time) {
        return record == null
                || isIdleExpired(record, time) == null
                || isTTLExpired(record, time) == null;
    }

    private Record isIdleExpired(Record record, long time) {
        if (record == null) {
            return null;
        }
        boolean result;
        // lastAccessTime : updates on every touch (put/get).
        final long lastAccessTime = record.getLastAccessTime();

        assert lastAccessTime > 0L;
        assert time > 0L;
        assert time >= lastAccessTime;

        final long maxIdleTime = getMaxIdleTime();
        result = time - lastAccessTime >= maxIdleTime;

        return result ? null : record;
    }

    private long calculateMaxIdleTime() {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final int maxIdleSeconds = mapConfig.getMaxIdleSeconds();
        if (maxIdleSeconds == 0) {
            return Long.MAX_VALUE;
        }
        return mapServiceContext.convertTime(maxIdleSeconds, TimeUnit.SECONDS);
    }

    private long getMaxIdleTime() {
        return maxIdleTime;
    }

    private Record isTTLExpired(Record record, long time) {
        if (record == null) {
            return null;
        }
        boolean result;
        final long ttl = record.getTtl();
        // when ttl is zero or negative, it should remain eternally.
        if (ttl < 1L) {
            return record;
        }
        final long creationTime = record.getCreationTime();

        assert ttl > 0L : String.format("wrong ttl %d", ttl);
        assert creationTime > 0L : String.format("wrong creationTime %d", creationTime);
        assert time > 0L : String.format("wrong time %d", time);
        assert time >= creationTime : String.format("time >= lastUpdateTime (%d >= %d)",
                time, creationTime);

        result = time - creationTime >= ttl;
        return result ? null : record;
    }

    /**
     * - Sends eviction event.
     * - Invalidates near cache.
     *
     * @param key   the key to be processed.
     * @param value the value to be processed.
     */
    private void doPostExpirationOperations(Data key, Object value) {
        final String mapName = this.name;
        final MapServiceContext mapServiceContext = this.mapServiceContext;
        final NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        if (nearCacheProvider.isNearCacheAndInvalidationEnabled(mapName)) {
            nearCacheProvider.invalidateAllNearCaches(mapName, key);
        }
        fireEvent(key, value, mapName, mapServiceContext);
    }

    void increaseRecordEvictionCriteriaNumber(Record record, MapConfig.EvictionPolicy evictionPolicy) {
        switch (evictionPolicy) {
            case LRU:
                ++lruAccessSequenceNumber;
                record.setEvictionCriteriaNumber(lruAccessSequenceNumber);
                break;
            case LFU:
                record.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber() + 1L);
                break;
            case NONE:
                break;
            default:
                throw new IllegalArgumentException("Not an appropriate eviction policy [" + evictionPolicy + ']');
        }
    }

    @Override
    protected void accessRecord(Record record, long now) {
        super.accessRecord(record, now);
        increaseRecordEvictionCriteriaNumber(record, evictionPolicy);
    }


    /**
     * Read only iterator. Iterates by checking whether a record expired or not.
     */
    protected final class ReadOnlyRecordIterator implements Iterator<Record> {

        private final long now;
        private final boolean checkExpiration;
        private final Iterator<Record> iterator;
        private Record nextRecord;
        private Record lastReturned;

        protected ReadOnlyRecordIterator(Collection<Record> values, long now) {
            this.iterator = values.iterator();
            this.now = now;
            this.checkExpiration = true;
            advance();
        }

        protected ReadOnlyRecordIterator(Collection<Record> values) {
            this.iterator = values.iterator();
            this.now = -1L;
            this.checkExpiration = false;
            advance();
        }

        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        @Override
        public Record next() {
            if (nextRecord == null) {
                throw new NoSuchElementException();
            }
            lastReturned = nextRecord;
            advance();
            return lastReturned;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() is not supported by this iterator");
        }

        private void advance() {
            final long now = this.now;
            final boolean checkExpiration = this.checkExpiration;
            final Iterator<Record> iterator = this.iterator;

            while (iterator.hasNext()) {
                nextRecord = iterator.next();
                if (nextRecord != null) {
                    if (!checkExpiration) {
                        return;
                    }

                    if (!isExpired(nextRecord, now)) {
                        return;
                    }
                }
            }
            nextRecord = null;
        }
    }
}
