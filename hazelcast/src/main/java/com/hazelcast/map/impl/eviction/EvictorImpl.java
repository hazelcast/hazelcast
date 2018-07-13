/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.core.EntryView;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.LazyEntryViewFromRecord;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;

import static com.hazelcast.memory.MemorySize.toPrettyString;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Evictor helper methods.
 */
public class EvictorImpl implements Evictor {

    protected final int maxEvictionsPerCycle;
    protected final EvictionChecker evictionChecker;
    protected final IPartitionService partitionService;
    protected final MapEvictionPolicy mapEvictionPolicy;

    private final ILogger logger;

    public EvictorImpl(MapEvictionPolicy mapEvictionPolicy, int maxEvictionsPerCycle,
                       EvictionChecker evictionChecker, IPartitionService partitionService) {
        this.evictionChecker = checkNotNull(evictionChecker);
        this.partitionService = checkNotNull(partitionService);
        this.mapEvictionPolicy = checkNotNull(mapEvictionPolicy);
        this.maxEvictionsPerCycle = maxEvictionsPerCycle;
        this.logger = Logger.getLogger(EvictorImpl.class);
    }

    @Override
    public void evict(RecordStore recordStore, Data excludedKey) {
        assertRunningOnPartitionThread();

        int evictionsCountdown = maxEvictionsPerCycle;
        logger.fine("Trying eviction on " + recordStore.getName() + ". Evictable: " + recordStore.shouldEvict()
                + " numOfRecords: " + recordStore.size() + " and memSize: "
                + toPrettyString(recordStore.getStorage().getEntryCostEstimator().getEstimate()));

        if (!recordStore.shouldEvict()) {
            return;
        }

        EntryView entry = selectEvictableEntry(recordStore, excludedKey);

        while (entry != null && evictionsCountdown > 0) {
            evictEntry(recordStore, entry);

            entry = selectEvictableEntry(recordStore, excludedKey);
            evictionsCountdown--;
        }

        logger.fine("Evicted " + (maxEvictionsPerCycle - evictionsCountdown) + " on " + recordStore.getName() + ", New memSize: "
                + toPrettyString(recordStore.getStorage().getEntryCostEstimator().getEstimate()));
    }

    private EntryView selectEvictableEntry(RecordStore recordStore, Data excludedKey) {
        Iterable<EntryView> samples = getSamples(recordStore);
        EntryView excluded = null;
        EntryView selected = null;

        for (EntryView candidate : samples) {
            if (excludedKey != null && excluded == null && getDataKey(candidate).equals(excludedKey)) {
                excluded = candidate;
                continue;
            }

            if (selected == null) {
                selected = candidate;
            } else if (mapEvictionPolicy.compare(candidate, selected) < 0) {
                selected = candidate;
            }
        }

        return selected == null ? excluded : selected;
    }

    private Data getDataKey(EntryView candidate) {
        return getRecordFromEntryView(candidate).getKey();
    }

    private void evictEntry(RecordStore recordStore, EntryView selectedEntry) {
        Record record = getRecordFromEntryView(selectedEntry);
        Data key = record.getKey();

        if (recordStore.isLocked(record.getKey())) {
            return;
        }

        boolean backup = isBackup(recordStore);
        recordStore.evict(key, backup);

        if (!backup) {
            recordStore.doPostEvictionOperations(record, backup);
        }
    }

    @Override
    public boolean checkEvictable(RecordStore recordStore) {
        assertRunningOnPartitionThread();

        return evictionChecker.checkEvictable(recordStore);
    }

    // this method is overridden in another context.
    protected Record getRecordFromEntryView(EntryView selectedEntry) {
        return ((LazyEntryViewFromRecord) selectedEntry).getRecord();
    }

    protected boolean isBackup(RecordStore recordStore) {
        int partitionId = recordStore.getPartitionId();
        IPartition partition = partitionService.getPartition(partitionId, false);
        return !partition.isLocal();
    }

    protected Iterable<EntryView> getSamples(RecordStore recordStore) {
        Storage storage = recordStore.getStorage();
        return (Iterable<EntryView>) storage.getRandomSamples(SAMPLE_COUNT);
    }

    protected static long getNow() {
        return Clock.currentTimeMillis();
    }

}
