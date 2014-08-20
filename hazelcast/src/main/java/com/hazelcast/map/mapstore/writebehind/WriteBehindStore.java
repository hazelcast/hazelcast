package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.map.MapStoreWrapper;
import com.hazelcast.map.mapstore.AbstractMapDataStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO Holds current write behind state and should be included in migrations.
 * Write behind map data store implementation.
 * Created per every record-store.
 */
public class WriteBehindStore extends AbstractMapDataStore<Data, Object> {

    private final long writeDelayTime;

    private final int partitionId;

    private WriteBehindQueue<DelayedEntry> writeBehindQueue;

    private WriteBehindProcessor writeBehindProcessor;

    /**
     * A temporary living space for evicted data if we are using a write-behind map store.
     * Because every eviction triggers a map store flush and in write-behind mode this flush operation
     * should not cause any inconsistencies like reading a stale value from map store.
     * To prevent this kind of inconsistencies, first we are searching an evicted entry in this space and if it is not there,
     * we are asking map store to load it. All read operations will use this staging area
     * to return last set value on a specific key, since there is a possibility that WBQ
     * {@link com.hazelcast.map.mapstore.writebehind.WriteBehindQueue} may contain more than one waiting operations
     * on a specific key.
     * <p/>
     * NOTE: In case of eviction we do not want to make a huge database load by flushing entries uncontrollably.
     * This is why we do not use flushing instead of this staging area. Flushing upon eviction is used
     * when {@link #writeCoalescing} mode is on.
     *
     * @see #cleanupStagingArea for how staging area is going to be evicted.
     */
    private final Map<Data, DelayedEntry> stagingArea;

    /**
     * To check if a key has a delayed delete operation or not.
     */
    private final Set<Data> writeBehindWaitingDeletions = new HashSet<Data>();

    /**
     * Iterates over a pre-set entry count/percentage in one round.
     * Used in expiration logic for traversing entries. Initializes lazily.
     */
    private Iterator<DelayedEntry> stagingAreaIterator;

    private long lastCleanupTime;

    private final boolean writeCoalescing;

    public WriteBehindStore(MapStoreWrapper store, SerializationService serializationService,
                            long writeDelayTime, int partitionId, boolean writeCoalescing) {
        super(store, serializationService);
        this.writeDelayTime = writeDelayTime;
        this.partitionId = partitionId;
        this.stagingArea = createStagingArea();
        this.writeCoalescing = writeCoalescing;
    }

    public void setWriteBehindQueue(WriteBehindQueue<DelayedEntry> writeBehindQueue) {
        this.writeBehindQueue = writeBehindQueue;
    }

    // TODO when mode is not write-coalescing, clone value objects. this is for EntryProcessors in object memory format.
    @Override
    public Object add(Data key, Object value, long now) {
        cleanupStagingArea(now);
        final long writeDelay = this.writeDelayTime;
        final long storeTime = now + writeDelay;
        final DelayedEntry<Data, Object> delayedEntry =
                DelayedEntry.create(key, value, storeTime, partitionId);

        writeBehindQueue.offer(delayedEntry);
        removeFromWaitingDeletions(key);

        return value;
    }

    @Override
    public void addTransient(Data key, long now) {
        cleanupStagingArea(now);
        removeFromWaitingDeletions(key);
    }

    @Override
    public Object addBackup(Data key, Object value, long time) {
        return add(key, value, time);
    }

    @Override
    public void remove(Data key, long now) {
        cleanupStagingArea(now);
        final long writeDelay = this.writeDelayTime;
        final long storeTime = now + writeDelay;
        final DelayedEntry<Data, Object> delayedEntry =
                DelayedEntry.createWithNullValue(key, storeTime, partitionId);
        addToWaitingDeletions(key);
        removeFromStagingArea(key);

        writeBehindQueue.offer(delayedEntry);
    }

    @Override
    public void removeBackup(Data key, long time) {
        remove(key, time);
    }

    @Override
    public void reset() {
        writeBehindQueue.clear();
        writeBehindWaitingDeletions.clear();
        stagingArea.clear();
    }

    @Override
    public Object load(Data key) {
        if (hasWaitingWriteBehindDeleteOperation(key)) {
            return null;
        }
        final Object valueFromStagingArea = getFromStagingArea(key);
        return valueFromStagingArea == null ? getStore().load(toObject(key))
                : valueFromStagingArea;
    }

    @Override
    public Map loadAll(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<Object, Object> map = new HashMap<Object, Object>();
        final Iterator iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Object key = iterator.next();
            final Data dataKey = toData(key);
            if (hasWaitingWriteBehindDeleteOperation(dataKey)) {
                iterator.remove();
                continue;
            }
            final Object valueFromStagingArea = getFromStagingArea(dataKey);
            if (valueFromStagingArea != null) {
                map.put(dataKey, valueFromStagingArea);
                iterator.remove();
            }
        }
        map.putAll(super.loadAll(keys));
        return map;
    }

    /**
     * * Used in {@link com.hazelcast.core.IMap#loadAll} calls.
     * If write-behind map-store feature enabled, some things may lead possible data inconsistencies.
     * These are:
     * - calling evict/evictAll.
     * - calling remove.
     * - not yet stored write behind queue operation.
     * <p/>
     * With this method we can be sure that a key can be loadable from map-store or not.
     *
     * @param key            to query whether loadable or not.
     * @param lastUpdateTime last update time.
     * @param now            in mills
     * @return <code>true</code> if loadable, otherwise false.
     */
    @Override
    public boolean loadable(Data key, long lastUpdateTime, long now) {
        if (hasWaitingWriteBehindDeleteOperation(key)
                || isInStagingArea(key, now)
                || hasAnyWaitingOperationInWriteBehindQueue(lastUpdateTime, now)) {
            return false;
        }
        return true;
    }

    @Override
    public int notFinishedOperationsCount() {
        return writeBehindQueue.size();
    }

    /**
     * GOTCHA: When {@link #writeCoalescing} is true, you may see more than one write operations
     * to map store, e.g. if a flush during eviction and {@link StoreWorker} fetch the operation on same
     * key, order of operations may be reflected differently to map store.
     */
    @Override
    public Object flush(Data key, Object value, long now, boolean backup) {
        if (writeCoalescing) {
            return defaultFlush(key, backup);
        } else {
            return flushWhenNotWriteCoalescing(key, value, now);
        }
    }

    @Override
    public Collection flush() {
        return writeBehindProcessor.flush(writeBehindQueue);
    }

    private void addToWaitingDeletions(Data key) {
        writeBehindWaitingDeletions.add(key);
    }

    private void removeFromWaitingDeletions(Data key) {
        writeBehindWaitingDeletions.remove(key);
    }

    private void removeFromStagingArea(Data key) {
        stagingArea.remove(key);
    }

    private boolean hasWaitingWriteBehindDeleteOperation(Data key) {
        return writeBehindWaitingDeletions.contains(key);
    }

    private boolean hasAnyWaitingOperationInWriteBehindQueue(long lastUpdateTime, long now) {
        final long scheduledStoreTime = lastUpdateTime + writeDelayTime;
        return now < scheduledStoreTime;
    }

    private Map<Data, DelayedEntry> createStagingArea() {
        return new ConcurrentHashMap<Data, DelayedEntry>();
    }

    private void initStagingAreaIterator() {
        if (stagingAreaIterator == null || !stagingAreaIterator.hasNext()) {
            stagingAreaIterator = stagingArea.values().iterator();
        }
    }

    public void cleanupStagingArea(long now) {
        if (stagingArea.isEmpty() || !inEvictableTimeWindow(now)) {
            return;
        }
        final int size = stagingArea.size();
        if (size == 0) {
            return;
        }
        final long nextItemsStoreTimeInWriteBehindQueue = getNextItemsStoreTimeInWriteBehindQueue();
        final int evictionPercentage = 20;
        int maxAllowedIterationCount = getMaxIterationCount(size, evictionPercentage);
        initStagingAreaIterator();
        while (stagingAreaIterator.hasNext()) {
            if (maxAllowedIterationCount <= 0) {
                break;
            }
            --maxAllowedIterationCount;
            final DelayedEntry entry = stagingAreaIterator.next();
            if (entry.getStoreTime() < nextItemsStoreTimeInWriteBehindQueue) {
                stagingAreaIterator.remove();
            }
            initStagingAreaIterator();
            if (!stagingAreaIterator.hasNext()) {
                break;
            }
            lastCleanupTime = now;
        }

    }

    private long getNextItemsStoreTimeInWriteBehindQueue() {
        final DelayedEntry firstEntryInQueue = writeBehindQueue.getFirst();
        if (firstEntryInQueue == null) {
            return 0L;
        }
        return firstEntryInQueue.getStoreTime();
    }

    /**
     * TODO dublicate code.
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

    /**
     * Eviction waits at least 1000 milliseconds to run.
     *
     * @param now now in millis.
     * @return <code>true</code> if in that time window,
     * otherwise <code>false</code>
     */
    private boolean inEvictableTimeWindow(long now) {
        final int evictAfterMs = 1000;
        return (now - lastCleanupTime) > evictAfterMs;
    }

    private boolean isInStagingArea(Data key, long now) {
        final DelayedEntry entry = stagingArea.get(key);
        if (entry == null) {
            return false;
        }
        final long storeTime = entry.getStoreTime();
        return now < storeTime;
    }

    private Object getFromStagingArea(Data key) {
        final DelayedEntry entry = stagingArea.get(key);
        if (entry == null) {
            return null;
        }
        final long storeTime = entry.getStoreTime();
        final long now = Clock.currentTimeMillis();
        // entry can not be reached from staging area.
        if (now >= storeTime) {
            return null;
        }
        return toObject(entry.getValue());
    }

    private Object defaultFlush(Data key, boolean backup) {
        DelayedEntry entry = DelayedEntry.createWithNullValue(key, -1, -1);
        entry = writeBehindQueue.get(entry);
        if (entry == null) {
            return null;
        }
        if (!backup) {
            writeBehindProcessor.flush(entry);
        }
        final List<DelayedEntry> entries = Collections.singletonList(entry);
        writeBehindQueue.removeAll(entries);
        return entry.getValue();
    }

    private Object flushWhenNotWriteCoalescing(Data key, Object value, long now) {
        assert value != null : String.format("value is null");
        assert now > 0 : String.format("time should be greater than 0, but found %d", now);

        cleanupStagingArea(now);
        final long storeTime = now + writeDelayTime;
        final DelayedEntry<Void, Object> delayedEntry = DelayedEntry.createWithNullKey(value, storeTime);
        stagingArea.put(key, delayedEntry);
        removeFromWaitingDeletions(key);
        return value;
    }


    public WriteBehindQueue<DelayedEntry> getWriteBehindQueue() {
        return writeBehindQueue;
    }

    public void setWriteBehindProcessor(WriteBehindProcessor writeBehindProcessor) {
        this.writeBehindProcessor = writeBehindProcessor;
    }
}
