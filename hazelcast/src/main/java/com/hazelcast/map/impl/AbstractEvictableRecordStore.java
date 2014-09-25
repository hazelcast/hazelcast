package com.hazelcast.map.impl;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.eviction.EvictionHelper.checkEvictable;
import static com.hazelcast.map.impl.eviction.EvictionHelper.evictableSize;
import static com.hazelcast.map.impl.eviction.EvictionHelper.fireEvent;
import static com.hazelcast.map.impl.eviction.EvictionHelper.removeEvictableRecords;

/**
 * Contains eviction specific functionality.
 */
abstract class AbstractEvictableRecordStore extends AbstractRecordStore {

    /**
     * Number of reads before clean up.
     * A nice number such as 2^n - 1.
     */
    protected static final int POST_READ_CHECK_POINT = 63;

    /**
     * Flag for checking if this record store has at least one candidate entry
     * for expiration (idle or tll) or not.
     */
    protected volatile boolean expirable;

    /**
     * Iterates over a pre-set entry count/percentage in one round.
     * Used in expiration logic for traversing entries. Initializes lazily.
     */
    protected Iterator<Record> expirationIterator;

    /**
     * If there is no clean-up caused by puts after some time,
     * count a number of gets and start eviction.
     */
    protected int readCountBeforeCleanUp;

    /**
     * used in LRU eviction logic.
     */
    protected long lruAccessSequenceNumber;

    /**
     * Last run time of cleanup operation.
     */
    protected long lastEvictionTime;

    private final boolean evictionEnabled;

    private final long minEvictionCheckMillis;

    protected AbstractEvictableRecordStore(MapContainer mapContainer, int partitionId) {
        super(mapContainer, partitionId);
        final MapConfig mapConfig = mapContainer.getMapConfig();
        this.minEvictionCheckMillis = mapConfig.getMinEvictionCheckMillis();
        this.evictionEnabled
                = !EvictionPolicy.NONE.equals(mapConfig.getEvictionPolicy());
        this.expirable = isRecordStoreExpirable();
    }

    private boolean isRecordStoreExpirable() {
        return mapContainer.getMapConfig().getMaxIdleSeconds() > 0
                || mapContainer.getMapConfig().getTimeToLiveSeconds() > 0;
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
            final Record record = expirationIterator.next();
            final Data key = record.getKey();
            if (isLocked(key)) {
                continue;
            }
            if (isReachable(record, now)) {
                continue;
            }
            //!!! get entry value here because evictInternal(key) nulls the record value.
            final Object value = record.getValue();
            evictInternal(key, backup);
            evictedCount++;
            // do post eviction operations if this partition is an owner partition.
            if (!backup) {
                doPostEvictionOperations(key, value, backup);
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
            removeEvictables(backup);
            lastEvictionTime = now;
            readCountBeforeCleanUp = 0;
        }
    }

    protected boolean shouldEvict(long now) {
        return evictionEnabled && inEvictableTimeWindow(now) && isEvictable();
    }

    private void removeEvictables(boolean backup) {
        final int evictableSize = getEvictableSize();
        if (evictableSize < 1) {
            return;
        }
        final MapConfig mapConfig = mapContainer.getMapConfig();
        removeEvictableRecords(this, evictableSize, mapConfig, mapServiceContext, backup);
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
     * @param record {@link com.hazelcast.map.impl.record.Record}
     * @return null if evictable.
     */
    protected Record getOrNullIfExpired(Record record, boolean backup) {
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
        if (isReachable(record)) {
            return record;
        }
        final Object value = record.getValue();
        evict(key, backup);
        if (!backup) {
            doPostEvictionOperations(key, value, backup);
        }
        return null;
    }

    private boolean isReachable(Record record) {
        final long now = getNow();
        return isReachable(record, now);
    }

    private boolean isReachable(Record record, long time) {
        if (record == null) {
            return false;
        }
        final Record idleExpired = isIdleExpired(record, time);
        if (idleExpired == null) {
            return false;
        }
        final Record ttlExpired = isTTLExpired(record, time);

        return ttlExpired != null;
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

        final long idleTime = getIdleTime();
        result = time - lastAccessTime >= idleTime;

        return result ? null : record;
    }

    private long getIdleTime() {
        final int maxIdleSeconds = mapContainer.getMapConfig().getMaxIdleSeconds();
        return maxIdleSeconds == 0 ? Long.MAX_VALUE : mapServiceContext.convertTime(maxIdleSeconds, TimeUnit.SECONDS);
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
     * @param key    the key to be processed.
     * @param value  the value to be processed.
     * @param backup <code>true</code> if running on a backup partition, otherwise <code>false</code>
     */
    private void doPostEvictionOperations(Data key, Object value, boolean backup) {
        if (backup) {
            return;
        }
        final NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        if (nearCacheProvider.isNearCacheAndInvalidationEnabled(name)) {
            nearCacheProvider.invalidateAllNearCaches(name, key);
        }
        fireEvent(key, value, name, mapServiceContext);
    }

    protected void increaseRecordEvictionCriteriaNumber(Record record, EvictionPolicy evictionPolicy) {
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
        increaseRecordEvictionCriteriaNumber(record, mapContainer.getMapConfig().getEvictionPolicy());
    }


    /**
     * Read only iterator. Iterates by checking whether a record expired or not.
     */
    protected final class ReadOnlyRecordIterator implements Iterator<Record> {

        private final Iterator<Record> iterator;
        private Record nextRecord;
        private Record lastReturned;

        protected ReadOnlyRecordIterator(Collection<Record> values) {
            this.iterator = values.iterator();

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
            throw new UnsupportedOperationException("remove not supported by this iterator");
        }

        private void advance() {
            while (iterator.hasNext()) {
                nextRecord = iterator.next();
                boolean reachable = isReachable(nextRecord);
                if (reachable && nextRecord != null) {
                    return;
                }
            }
            nextRecord = null;
        }
    }
}
