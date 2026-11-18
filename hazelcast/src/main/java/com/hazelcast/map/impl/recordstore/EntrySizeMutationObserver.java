/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.monitor.LocalRecordStoreStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.record.Record;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Records oversized entries written to a partition replica in its
 * {@link LocalRecordStoreStats}, from which the {@code oversizedEntriesSeen},
 * {@code lastOversizedEntrySize} and {@code lastOversizedEntryTime} map metrics
 * are reported for owned partitions.
 * Backup replicas are tracked too, so the statistics survive promotion, and
 * they are migrated with the partition. A warning is logged for oversized
 * entries written to a primary replica.
 * <p>
 * Only values already in serialized form are measured, so this adds no
 * serialization cost. Values held only as objects are skipped, such as
 * entry processor results and loaded values of {@link InMemoryFormat#OBJECT}
 * maps. When WAN replication is enabled, entry processor results are measured
 * from the value serialized for the WAN event instead, see
 * {@link RecordStore#recordEntrySize(Data, Data, boolean)}.
 */
class EntrySizeMutationObserver implements MutationObserver<Record> {

    private static final long LOG_PERIOD_MILLIS = TimeUnit.MINUTES.toMillis(30);

    private final String mapName;
    private final int partitionId;
    private final long thresholdBytes;
    private final LocalRecordStoreStats stats;
    private final AtomicLong lastLogTime;
    private final ILogger logger;

    EntrySizeMutationObserver(MapContainer mapContainer, int partitionId, long thresholdBytes,
                              LocalRecordStoreStats stats) {
        this.mapName = mapContainer.getName();
        this.partitionId = partitionId;
        this.thresholdBytes = thresholdBytes;
        this.stats = stats;
        this.lastLogTime = mapContainer.getLastOversizedEntryLogTime();
        this.logger = mapContainer.getMapServiceContext().getNodeEngine().getLogger(EntrySizeMutationObserver.class);
    }

    @Override
    public void onPutRecord(@Nonnull Data key, Record record, Object oldValue, Object newValue, boolean backup) {
        onStore(key, record, newValue, backup);
    }

    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull Record record,
                               Object oldValue, Object newValue, boolean backup) {
        // setTtl updates a record with its existing value
        if (newValue != oldValue) {
            onStore(key, record, newValue, backup);
        }
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull Record record, Object newValue, boolean backup) {
        onStore(key, record, newValue, backup);
    }

    private void onStore(Data key, Record record, Object newValue, boolean backup) {
        if (newValue instanceof Data data) {
            recordEntrySize(key, data, backup);
        } else if (record.getValue() instanceof Data data) {
            recordEntrySize(key, data, backup);
        }
    }

    /**
     * Records the size of a stored entry. Must be called on the partition thread.
     */
    void recordEntrySize(Data key, Data value, boolean backup) {
        long entrySize = (long) key.totalSize() + value.totalSize();
        if (entrySize > thresholdBytes) {
            long now = Clock.currentTimeMillis();
            stats.recordOversizedEntry(entrySize, now);
            if (!backup && shouldLogNow(now)) {
                logger.warning("Stored an oversized entry of " + entrySize + " bytes (key " + key.totalSize()
                        + " bytes, value " + value.totalSize() + " bytes) in map '" + mapName + "', partition "
                        + partitionId + ". This is above the threshold of " + thresholdBytes + " bytes set by the"
                        + " map's oversized-entry-threshold-bytes config. Large entries can cause memory pressure"
                        + " and slow down migrations and WAN replication. The oversizedEntriesSeen map metric counts"
                        + " every oversized entry; this warning is logged at most once every 30 minutes per map.");
            }
        }
    }

    private boolean shouldLogNow(long now) {
        long lastLogged = lastLogTime.get();
        return now - lastLogged >= LOG_PERIOD_MILLIS && lastLogTime.compareAndSet(lastLogged, now);
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key, @Nonnull Record record, boolean populateIndex) {
        // no op
    }

    @Override
    public void onRemoveRecord(@Nonnull Data key, Record record, boolean backup) {
        // no op
    }

    @Override
    public void onEvictRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        // no op
    }

    @Override
    public void onReset() {
        // no op
    }

    @Override
    public void onClear() {
        // no op
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        // no op
    }
}
