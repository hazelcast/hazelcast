/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.EntryLoader.MetadataAwareValue;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.AbstractMapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.operation.NotifyMapFlushOperation;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.mapstore.writebehind.TxnReservedCapacityCounter.EMPTY_COUNTER;
import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createBoundedWriteBehindQueue;
import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createDefaultWriteBehindQueue;
import static com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries.newAddedDelayedEntry;
import static com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries.newDeletedEntry;
import static com.hazelcast.spi.impl.operationservice.OperationResponseHandlerFactory.createEmptyResponseHandler;

/**
 * Write behind map data store implementation.
 * Created per every record-store. Only called from one thread.
 */
@SuppressWarnings("checkstyle:methodcount")
public class WriteBehindStore extends AbstractMapDataStore<Data, Object> {

    /**
     * Represents a transient {@link DelayedEntry}.
     * A transient entry can be added via {@link IMap#putTransient}.
     */
    private static final DelayedEntry TRANSIENT = DelayedEntries.emptyDelayedEntry();

    /**
     * Sequence number of store operations.
     */
    private final AtomicLong sequence = new AtomicLong(0);

    /**
     * @see {@link com.hazelcast.config.MapStoreConfig#setWriteCoalescing(boolean)}
     */
    private final boolean coalesce;
    private final int partitionId;
    private final String mapName;
    private final InMemoryFormat inMemoryFormat;
    private final OperationService operationService;
    /**
     * Holds the sequences according to insertion
     * order at which {@link IMap#flush()} was called.
     * <p>
     * Upon end of a store operation, these sequences are used
     * to find whether there is any flush request waiting
     * for the stored sequence, and if there is any, {@link
     * NotifyMapFlushOperation} is send to notify {@link
     * com.hazelcast.map.impl.operation.AwaitMapFlushOperation
     * AwaitMapFlushOperation}
     *
     * @see WriteBehindStore#notifyFlush
     */
    private final Queue<Sequence> flushSequences = new ConcurrentLinkedQueue<>();
    /**
     * {@code stagingArea} is a temporary living space for evicted
     * data if we are using a write-behind map store. Every eviction
     * triggers a map store flush, and in write-behind mode this flush
     * operation should not cause any inconsistencies, such as reading
     * a stale value from map store. To prevent reading stale values
     * when the time of a non-existent key is requested, before loading
     * it from map-store we search for an evicted entry in this space.
     * If the entry is not there, we ask map store to load it. All read
     * operations use this staging area to return the last set value
     * on a specific key, since there is a possibility that {@link
     * com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue}
     * may contain more than one waiting operations on a specific key.
     * <p>
     * This space is also used to control any waiting delete
     * operations on a key or any transiently put entries to {@code
     * IMap}. Values of any transiently put entries should not
     * be added to this area upon eviction, otherwise subsequent
     * {@code IMap#get} operations may return stale values.
     * <p>
     * NOTE: In case of eviction we do not want to make a huge database
     * load by flushing entries uncontrollably. We also do not want to
     * make duplicate map-store calls for a key. This is why we use the
     * staging area instead of the direct flushing option to map-store.
     */
    private final ConcurrentMap<Data, DelayedEntry> stagingArea = new ConcurrentHashMap<>();
    private final TxnReservedCapacityCounter txnReservedCapacityCounter;

    private WriteBehindProcessor writeBehindProcessor;
    private WriteBehindQueue<DelayedEntry> writeBehindQueue;

    public WriteBehindStore(MapStoreContext mapStoreContext, int partitionId,
                            WriteBehindProcessor writeBehindProcessor) {
        super(mapStoreContext);
        this.partitionId = partitionId;
        this.inMemoryFormat = getInMemoryFormat(mapStoreContext);
        this.coalesce = mapStoreContext.getMapStoreConfig().isWriteCoalescing();
        this.mapName = mapStoreContext.getMapName();
        this.operationService = nodeEngine.getOperationService();
        this.writeBehindQueue = coalesce ? createDefaultWriteBehindQueue()
                : createBoundedWriteBehindQueue(mapStoreContext);
        this.writeBehindProcessor = writeBehindProcessor;
        this.txnReservedCapacityCounter = initTxnReservedCapacityCounter();
    }

    private TxnReservedCapacityCounter initTxnReservedCapacityCounter() {
        BoundedWriteBehindQueue<DelayedEntry> unwrapped
                = writeBehindQueue.unwrap(BoundedWriteBehindQueue.class);
        return unwrapped != null ? unwrapped.getTxnReservedCapacityCounter() : EMPTY_COUNTER;
    }

    @Override
    public TxnReservedCapacityCounter getTxnReservedCapacityCounter() {
        return txnReservedCapacityCounter;
    }

    @Override
    public Object add(Data key, Object value,
                      long expirationTime, long now, UUID transactionId) {
        // When using format InMemoryFormat.NATIVE,
        // just copy key & value to heap.
        if (NATIVE == inMemoryFormat) {
            value = toHeapData(value);
            key = toHeapData(key);
        }

        // This note describes the problem when we want to persist
        // all states of an entry (means write-coalescing is off)
        // by using both EntryProcessor + OBJECT in-memory-format:
        //
        // If in-memory-format is OBJECT, there is a possibility that a previous
        // state of an entry can be overwritten by a subsequent write operation
        // while both are waiting in the write-behind-queue, this is because
        // they are referencing to the same entry-value. To prevent such a
        // problem, we are taking snapshot of the value by serializing it, this
        // means an extra serialization and additional latency for operations
        // like map#put but it is needed, otherwise we can lost a state.
        if (!coalesce && OBJECT == inMemoryFormat) {
            value = toHeapData(value);
        }
        expirationTime = getUserExpirationTime(expirationTime);
        add(DelayedEntries.newAddedDelayedEntry(key, value, expirationTime, now, partitionId, transactionId));
        return value;
    }

    @Override
    public void addForcibly(DelayedEntry<Data, Object> delayedEntry) {
        delayedEntry.setSequence(sequence.incrementAndGet());
        writeBehindQueue.addLast(delayedEntry, true);
        stagingArea.put(delayedEntry.getKey(), delayedEntry);

    }

    public void add(DelayedEntry<Data, Object> delayedEntry) {
        delayedEntry.setSequence(sequence.incrementAndGet());

        writeBehindQueue.addLast(delayedEntry, false);

        stagingArea.put(delayedEntry.getKey(), delayedEntry);

    }

    @Override
    public void addTransient(Data key, long now) {
        if (NATIVE == inMemoryFormat) {
            key = toHeapData(key);
        }

        stagingArea.put(key, TRANSIENT);
    }

    /**
     * Throw no exception while putting backup entry.
     */
    @Override
    public Object addBackup(Data key, Object value,
                            long expirationTime, long time, UUID transactionId) {
        return add(key, value, expirationTime, time, transactionId);
    }

    @Override
    public void remove(Data key, long now, UUID transactionId) {
        if (NATIVE == inMemoryFormat) {
            key = toHeapData(key);
        }
        add(newDeletedEntry(key, now, partitionId, transactionId));
    }

    @Override
    public void removeBackup(Data key, long time, UUID transactionId) {
        remove(key, time, transactionId);
    }

    @Override
    public void reset() {
        writeBehindQueue.clear();
        stagingArea.clear();
        sequence.set(0);
        flushSequences.clear();
    }

    @Override
    public Object load(Data key) {
        DelayedEntry delayedEntry = getFromStagingArea(key);
        if (delayedEntry == null) {
            return getStore().load(toObject(key));
        }
        // At this point, the value comes from staging area.
        // This may be a value with expirationTime. So we need
        // to return an ExtendedValue
        if (isWithExpirationTime() && delayedEntry.getValue() != null) {
            return new MetadataAwareValue(toObject(delayedEntry.getValue()), delayedEntry.getExpirationTime());
        }
        return toObject(delayedEntry.getValue());
    }

    /**
     * {@inheritDoc}
     * The method first checks if some of the keys to be loaded
     * have entries that are staged to be persisted to the
     * underlying store and returns those values instead of loading
     * the values from the store.
     * The keys which don't have staged entries to be persisted will
     * be loaded from the underlying store.
     *
     * @see MapLoader#loadAll(Collection)
     */
    @Override
    public Map loadAll(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Object, Object> map = createHashMap(keys.size());
        Iterator iterator = keys.iterator();
        while (iterator.hasNext()) {
            Object key = iterator.next();
            Data dataKey = toHeapData(key);

            DelayedEntry delayedEntry = getFromStagingArea(dataKey);
            if (delayedEntry != null) {
                Object value = delayedEntry.getValue();
                if (value != null) {
                    if (isWithExpirationTime()) {
                        map.put(dataKey, new MetadataAwareValue(toObject(value), delayedEntry.getExpirationTime()));
                    } else {
                        map.put(dataKey, toObject(value));
                    }
                }
                iterator.remove();
            }
        }
        map.putAll(super.loadAll(keys));
        return map;
    }

    @Override
    public boolean loadable(Data key) {
        if (NATIVE == inMemoryFormat) {
            key = toHeapData(key);
        }

        return !writeBehindQueue.contains(newAddedDelayedEntry(key));
    }

    @Override
    public int notFinishedOperationsCount() {
        return writeBehindQueue.size();
    }

    @Override
    public Object flush(Data key, Object value, boolean backup) {
        if (NATIVE == inMemoryFormat) {
            key = toHeapData(key);
            value = toHeapData(value);
        }

        DelayedEntry delayedEntry = stagingArea.get(key);
        if (delayedEntry == TRANSIENT) {
            stagingArea.remove(key);
            return null;
        }

        if (writeBehindQueue.size() == 0
                || !writeBehindQueue.contains(DelayedEntries.newNullEntry(key))) {
            return null;
        }

        addAndGetSequence(false);
        return value;
    }

    @Override
    public long softFlush() {
        int size = writeBehindQueue.size();
        if (size == 0) {
            return 0;
        }
        return addAndGetSequence(true);
    }

    /**
     * @param fullFlush {@code true} if flush cause is {@link IMap#flush()},
     *                  {@code false} for flushes caused by eviction.
     * @return last store operations sequence number
     */
    private long addAndGetSequence(boolean fullFlush) {
        Sequence sequence = new Sequence(this.sequence.get(), fullFlush);
        flushSequences.add(sequence);
        return sequence.getSequence();
    }

    @Override
    public void hardFlush() {
        if (writeBehindQueue.size() == 0) {
            return;
        }

        writeBehindProcessor.flush(writeBehindQueue);
    }

    public WriteBehindQueue<DelayedEntry> getWriteBehindQueue() {
        return writeBehindQueue;
    }

    public void setSequence(long newSequence) {
        this.sequence.set(newSequence);
    }

    public void notifyFlush() {
        long nextSequenceNumber = sequence.get() + 1;
        DelayedEntry firstEntry = writeBehindQueue.peek();
        if (firstEntry == null) {
            if (!flushSequences.isEmpty()) {
                findAwaitingFlushesAndSendNotification(nextSequenceNumber);
            }
        } else {
            findAwaitingFlushesAndSendNotification(firstEntry.getSequence());
        }

    }

    private void findAwaitingFlushesAndSendNotification(
            long lastSequenceInQueue) {
        final int maxIterationCount = 100;

        Iterator<Sequence> iterator = flushSequences.iterator();
        int iterationCount = 0;
        while (iterator.hasNext()) {
            Sequence flushSequence = iterator.next();
            if (flushSequence.getSequence() < lastSequenceInQueue) {
                iterator.remove();
                executeNotifyOperation(flushSequence);
            }

            if (++iterationCount == maxIterationCount) {
                break;
            }
        }
    }

    private void executeNotifyOperation(Sequence flushSequence) {
        if (!flushSequence.isFullFlush()
                || !nodeEngine.getPartitionService().isPartitionOwner(partitionId)) {
            return;
        }

        Operation operation = new NotifyMapFlushOperation(mapName, flushSequence.getSequence());
        operation.setServiceName(SERVICE_NAME)
                .setNodeEngine(nodeEngine)
                .setPartitionId(partitionId)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setOperationResponseHandler(createEmptyResponseHandler());

        operationService.execute(operation);
    }

    protected void removeFromStagingArea(DelayedEntry delayedEntry) {
        if (delayedEntry == null) {
            return;
        }
        Data key = (Data) delayedEntry.getKey();
        stagingArea.remove(key, delayedEntry);
    }

    private DelayedEntry getFromStagingArea(Data key) {
        DelayedEntry delayedEntry = stagingArea.get(key);
        if (delayedEntry == null || delayedEntry == TRANSIENT) {
            return null;
        }
        return delayedEntry;
    }

    public Queue<Sequence> getFlushSequences() {
        return flushSequences;
    }

    public long getSequenceToFlush() {
        final int maxIterationCount = 100;

        Iterator<Sequence> iterator = flushSequences.iterator();
        long sequenceNumber = 0L;
        int iterationCount = 0;
        while (iterator.hasNext()) {
            Sequence sequence = iterator.next();
            sequenceNumber = sequence.getSequence();

            if (++iterationCount == maxIterationCount) {
                break;
            }
        }
        return sequenceNumber;
    }

    public void setFlushSequences(Queue<Sequence> flushSequences) {
        this.flushSequences.addAll(flushSequences);
    }

    private static InMemoryFormat getInMemoryFormat(MapStoreContext mapStoreContext) {
        MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();

        Config config = nodeEngine.getConfig();
        String mapName = mapStoreContext.getMapName();
        MapConfig mapConfig = config.findMapConfig(mapName);
        return mapConfig.getInMemoryFormat();
    }

    /**
     * The purpose of this class is to provide distinction
     * between flushes caused by eviction and {@link IMap#flush()}
     */
    public static class Sequence {

        /**
         * Sequence of the store operation.
         */
        private final long sequence;

        /**
         * When {@code true}, it means {@link IMap#flush()} was called at this sequence.
         */
        private final boolean fullFlush;

        public Sequence(long sequence, boolean fullFlush) {
            this.sequence = sequence;
            this.fullFlush = fullFlush;
        }

        public long getSequence() {
            return sequence;
        }

        public boolean isFullFlush() {
            return fullFlush;
        }
    }
}
